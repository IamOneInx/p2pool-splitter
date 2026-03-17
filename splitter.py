#!/usr/bin/env python3
"""
p2pool-splitter — Automated P2Pool mining reward splitter

How it works:
  P2Pool is a decentralized Monero mining pool. When the pool finds a block,
  it pays each participating wallet directly via the Monero coinbase transaction.
  This tool is for the case where multiple mining rigs all point to ONE operator
  wallet. The operator receives all P2Pool payouts and wants to forward each
  worker's proportional share automatically.

  Flow:
    1. Poll monero-wallet-rpc for new incoming transfers (P2Pool payouts)
    2. Queue each payout with a randomized delay (optional, for privacy)
    3. Once the delay passes, compute each worker's share by hashrate percentage
    4. Send one Monero transaction with all worker amounts as outputs
    5. Record everything in SQLite for auditing

  All amounts are stored internally in picoXMR (the smallest Monero unit).
  1 XMR = 1,000,000,000,000 picoXMR (12 decimal places).

Usage: python splitter.py --config config.yaml [--dry-run] [--once]
"""

import argparse
import json
import logging
import os
import random
import sqlite3
import sys
import time
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library required. Run: pip install requests", file=sys.stderr)
    sys.exit(1)

__version__ = "1.1.0"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_file: str | None = None, verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    return logging.getLogger("p2pool-splitter")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class Config:
    def __init__(self, path: str):
        data = self._load(path)
        self.wallet_rpc_url: str        = data.get("wallet_rpc_url", "http://127.0.0.1:18082/json_rpc")
        self.wallet_rpc_user: str       = data.get("wallet_rpc_user", "")
        self.wallet_rpc_password: str   = data.get("wallet_rpc_password", "")
        self.poll_interval: int         = int(data.get("poll_interval", 60))
        self.min_payout_threshold: int  = int(data.get("min_payout_threshold", 300_000_000))  # picoXMR
        self.confirmations_required: int = int(data.get("confirmations_required", 1))
        self.retry_count: int           = int(data.get("retry_count", 3))
        self.db_path: str               = data.get("db_path", "splitter.db")
        self.log_file: str | None       = data.get("log_file")
        self.verbose: bool              = bool(data.get("verbose", False))

        # Privacy: randomized payout delay (seconds). A random delay between
        # min and max is chosen per payout, breaking timing correlation with
        # the incoming P2Pool coinbase transaction.
        self.payout_delay_min: int = int(data.get("payout_delay_min", 0))
        self.payout_delay_max: int = int(data.get("payout_delay_max", 0))
        if self.payout_delay_max < self.payout_delay_min:
            raise ValueError("payout_delay_max must be >= payout_delay_min")

        # Privacy: batch multiple pending payouts into a single transfer.
        # When True, all payouts that are past their delay window are summed
        # per worker and sent in one transaction, preventing 1:1 correlation
        # between incoming and outgoing transactions.
        self.batch_payouts: bool = bool(data.get("batch_payouts", False))

        raw_workers = data.get("workers", [])
        if not raw_workers:
            raise ValueError("Config must define at least one worker")

        self.workers: list[dict] = []
        for w in raw_workers:
            if not all(k in w for k in ("name", "address", "hashrate")):
                raise ValueError(f"Worker entry missing name/address/hashrate: {w}")
            if int(w["hashrate"]) <= 0:
                raise ValueError(f"Worker '{w['name']}' hashrate must be > 0")
            self.workers.append({
                "name": str(w["name"]),
                "address": str(w["address"]),
                "hashrate": int(w["hashrate"]),
            })

        self.total_hashrate: int = sum(w["hashrate"] for w in self.workers)

    def _load(self, path: str) -> dict:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        text = p.read_text()
        if path.endswith(".json"):
            return json.loads(text)
        if yaml is not None:
            return yaml.safe_load(text)
        return self._parse_simple_yaml(text)

    @staticmethod
    def _parse_simple_yaml(text: str) -> dict:
        """Minimal YAML loader for environments without PyYAML."""
        raise ImportError(
            "PyYAML is required to parse YAML config files.\n"
            "Install with: pip install pyyaml\n"
            "Or rename your config to config.json and use JSON format."
        )


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA = """
-- One row per incoming P2Pool payout detected in the operator wallet.
-- release_at is a Unix timestamp: the payout will not be forwarded until
-- that time passes (supports the optional randomized privacy delay).
CREATE TABLE IF NOT EXISTS payouts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    txid            TEXT    UNIQUE NOT NULL,  -- Monero transaction ID of the incoming payout
    amount_pico     INTEGER NOT NULL,         -- Total received amount in picoXMR
    block_height    INTEGER,                  -- Block height where this payout was mined
    detected_at     INTEGER NOT NULL,         -- Unix timestamp when we first saw this payout
    release_at      INTEGER NOT NULL DEFAULT 0, -- Unix timestamp after which we forward the payout
    processed       INTEGER NOT NULL DEFAULT 0  -- 1 once the split transaction has been sent
);

-- One row per worker per payout. Each payout fans out into N split records
-- (one per worker). Together they record exactly how much each worker was owed
-- and whether the on-chain transfer succeeded.
CREATE TABLE IF NOT EXISTS splits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    payout_id       INTEGER NOT NULL REFERENCES payouts(id),
    worker_name     TEXT    NOT NULL,         -- Human-readable rig/worker name from config
    worker_address  TEXT    NOT NULL,         -- Monero address the share was sent to
    amount_pico     INTEGER NOT NULL,         -- Worker's share in picoXMR
    tx_hash         TEXT,                     -- Outgoing transaction hash (set after send)
    fee_pico        INTEGER,                  -- Transaction fee in picoXMR (set after send)
    sent_at         INTEGER,                  -- Unix timestamp of successful send
    status          TEXT    NOT NULL DEFAULT 'pending',  -- pending | sent | failed
    error           TEXT                      -- Error message if status = failed
);

CREATE INDEX IF NOT EXISTS idx_payouts_txid      ON payouts(txid);
CREATE INDEX IF NOT EXISTS idx_splits_payout_id  ON splits(payout_id);
CREATE INDEX IF NOT EXISTS idx_splits_status     ON splits(status);
"""


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    # Migrate existing databases that predate the release_at column
    try:
        conn.execute("ALTER TABLE payouts ADD COLUMN release_at INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists
    return conn


def load_processed_txids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT txid FROM payouts WHERE processed = 1").fetchall()
    return {r["txid"] for r in rows}


def insert_payout(conn: sqlite3.Connection, txid: str, amount_pico: int,
                  block_height: int | None, release_at: int) -> int:
    cur = conn.execute(
        "INSERT OR IGNORE INTO payouts (txid, amount_pico, block_height, detected_at, release_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (txid, amount_pico, block_height, int(time.time()), release_at)
    )
    conn.commit()
    if cur.lastrowid and cur.lastrowid > 0:
        return cur.lastrowid
    row = conn.execute("SELECT id FROM payouts WHERE txid = ?", (txid,)).fetchone()
    return row["id"]


def insert_splits(conn: sqlite3.Connection, payout_id: int,
                  splits: list[dict]) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO splits (payout_id, worker_name, worker_address, amount_pico) VALUES (?, ?, ?, ?)",
        [(payout_id, s["name"], s["address"], s["amount_pico"]) for s in splits]
    )
    conn.commit()


def mark_payout_processed(conn: sqlite3.Connection, payout_id: int,
                           tx_hash: str, fee_pico: int) -> None:
    conn.execute("UPDATE payouts SET processed = 1 WHERE id = ?", (payout_id,))
    conn.execute(
        "UPDATE splits SET status = 'sent', tx_hash = ?, fee_pico = ?, sent_at = ? WHERE payout_id = ?",
        (tx_hash, fee_pico, int(time.time()), payout_id)
    )
    conn.commit()


def mark_payout_failed(conn: sqlite3.Connection, payout_id: int, error: str) -> None:
    conn.execute(
        "UPDATE splits SET status = 'failed', error = ? WHERE payout_id = ?",
        (error, payout_id)
    )
    conn.commit()


def load_pending_payouts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return unprocessed payouts whose release window has passed."""
    now = int(time.time())
    return conn.execute(
        "SELECT * FROM payouts WHERE processed = 0 AND release_at <= ?", (now,)
    ).fetchall()


# ---------------------------------------------------------------------------
# Wallet RPC client
# ---------------------------------------------------------------------------

class WalletRPC:
    def __init__(self, url: str, user: str = "", password: str = ""):
        self.url = url
        self.auth = (user, password) if user else None
        self._id = 0

    def _call(self, method: str, params: dict | None = None) -> dict:
        self._id += 1
        payload = {"jsonrpc": "2.0", "id": str(self._id), "method": method}
        if params:
            payload["params"] = params
        resp = requests.post(
            self.url,
            json=payload,
            auth=self.auth,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"RPC error [{method}]: {data['error']}")
        return data.get("result", {})

    def get_balance(self) -> tuple[int, int]:
        """Returns (balance, unlocked_balance) in picoXMR."""
        r = self._call("get_balance", {"account_index": 0})
        return r["balance"], r["unlocked_balance"]

    def get_transfers(self, min_height: int = 0) -> list[dict]:
        """Returns incoming transfers at or above min_height."""
        params: dict = {"in": True}
        if min_height > 0:
            params["filter_by_height"] = True
            params["min_height"] = min_height
        r = self._call("get_transfers", params)
        return r.get("in", [])

    def transfer(self, destinations: list[dict], priority: int = 2) -> dict:
        """
        Send to multiple destinations in one transaction.
        destinations: [{"amount": picoXMR, "address": "..."}]
        Returns {"tx_hash": ..., "fee": ..., "amount": ...}
        """
        return self._call("transfer", {
            "destinations": destinations,
            "priority": priority,
            "ring_size": 16,      # 16 decoys per input — current Monero network default for privacy
            "account_index": 0,
        })

    def get_height(self) -> int:
        """Returns the current blockchain height known to the wallet."""
        r = self._call("get_height")
        return r["height"]


# ---------------------------------------------------------------------------
# Core splitting logic
# ---------------------------------------------------------------------------

def compute_splits(amount_pico: int, workers: list[dict],
                   total_hashrate: int) -> list[dict]:
    """
    Compute each worker's share of a payout in picoXMR.

    Share is proportional to hashrate: worker_share = worker_hashrate / total_hashrate.
    Integer division is used to avoid fractional picoXMR (Monero's smallest unit).
    Any picoXMR lost to rounding (remainder) is credited to the first worker so
    that the output amounts always sum exactly to the input amount — no dust lost.
    """
    splits = []
    allocated = 0
    for w in workers:
        share = w["hashrate"] / total_hashrate
        worker_amount = int(amount_pico * share)  # floor division — remainder handled below
        splits.append({
            "name": w["name"],
            "address": w["address"],
            "hashrate": w["hashrate"],
            "share_pct": share * 100,
            "amount_pico": worker_amount,
        })
        allocated += worker_amount

    # Give any unallocated picoXMR (from rounding) to the first worker.
    # This keeps the math exact: sum(splits) == amount_pico always.
    remainder = amount_pico - allocated
    if remainder > 0:
        splits[0]["amount_pico"] += remainder

    return splits


def merge_splits(split_lists: list[list[dict]]) -> list[dict]:
    """
    Sum per-worker amounts across multiple payouts into one list.
    Used when batch_payouts=True to combine several pending payouts
    into a single outgoing transaction.
    """
    totals: dict[str, dict] = {}
    for splits in split_lists:
        for s in splits:
            key = s["address"]
            if key not in totals:
                totals[key] = {**s, "amount_pico": 0}
            totals[key]["amount_pico"] += s["amount_pico"]
    return list(totals.values())


PICO_PER_XMR = 1_000_000_000_000


def pico_to_xmr(pico: int) -> str:
    return f"{pico / PICO_PER_XMR:.12f}"


def random_delay(min_seconds: int, max_seconds: int) -> int:
    """Return a random delay in seconds between min and max."""
    if max_seconds <= 0:
        return 0
    return random.randint(min_seconds, max_seconds)


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------

class Splitter:
    def __init__(self, config: Config, dry_run: bool = False):
        self.cfg = config
        self.dry_run = dry_run
        self.log = logging.getLogger("p2pool-splitter")
        self.db = open_db(config.db_path)
        self.rpc = WalletRPC(config.wallet_rpc_url,
                             config.wallet_rpc_user,
                             config.wallet_rpc_password)
        self.processed_txids = load_processed_txids(self.db)
        self.last_height = self._get_last_height()
        self.log.info(f"p2pool-splitter v{__version__} started"
                      + (" [DRY RUN]" if dry_run else ""))
        self.log.info(f"Workers: {len(self.cfg.workers)} | "
                      f"Total hashrate: {self.cfg.total_hashrate:,} H/s | "
                      f"Min payout: {pico_to_xmr(self.cfg.min_payout_threshold)} XMR")
        if self.cfg.payout_delay_max > 0:
            self.log.info(f"Payout delay: {self.cfg.payout_delay_min}–{self.cfg.payout_delay_max}s "
                          f"({self.cfg.payout_delay_min//3600}–{self.cfg.payout_delay_max//3600}h) random window")
        if self.cfg.batch_payouts:
            self.log.info("Batching enabled: multiple pending payouts will be merged into one transaction")
        for w in self.cfg.workers:
            pct = w["hashrate"] / self.cfg.total_hashrate * 100
            self.log.info(f"  {w['name']}: {w['hashrate']:,} H/s ({pct:.1f}%) → {w['address'][:16]}…")

    def _get_last_height(self) -> int:
        """
        Return the block height to start scanning from on startup.
        We go back 10 blocks behind the last processed payout as a safety margin —
        monero-wallet-rpc can occasionally report heights slightly inconsistently,
        and rescanning a few extra blocks is harmless because duplicate txids are
        filtered by the UNIQUE constraint in the payouts table.
        """
        row = self.db.execute(
            "SELECT MAX(block_height) AS h FROM payouts WHERE processed = 1"
        ).fetchone()
        h = row["h"] if row and row["h"] else 0
        return max(0, h - 10)

    def _find_new_payouts(self) -> list[dict]:
        transfers = self.rpc.get_transfers(min_height=self.last_height)
        new = []
        for t in transfers:
            txid = t.get("txid", "")
            amount = t.get("amount", 0)
            confirmations = t.get("confirmations", 0)
            height = t.get("height", 0)

            if txid in self.processed_txids:
                continue
            if amount < self.cfg.min_payout_threshold:
                continue
            if confirmations < self.cfg.confirmations_required:
                self.log.debug(f"Skipping {txid[:16]}… — only {confirmations} confirmation(s)")
                continue

            # Check if already queued (detected but not yet released)
            existing = self.db.execute(
                "SELECT id FROM payouts WHERE txid = ?", (txid,)
            ).fetchone()
            if existing:
                continue

            new.append({
                "txid": txid,
                "amount": amount,
                "height": height,
                "confirmations": confirmations,
            })

        return new

    def _queue_payout(self, payout: dict) -> None:
        """Record a new payout with its randomized release timestamp."""
        delay = random_delay(self.cfg.payout_delay_min, self.cfg.payout_delay_max)
        release_at = int(time.time()) + delay
        txid = payout["txid"]
        amount_pico = payout["amount"]
        height = payout.get("height")

        insert_payout(self.db, txid, amount_pico, height, release_at)

        if delay > 0:
            self.log.info(f"Queued payout {txid[:16]}… "
                          f"({pico_to_xmr(amount_pico)} XMR) — "
                          f"releasing in {delay}s ({delay/3600:.1f}h)")
        else:
            self.log.info(f"Queued payout {txid[:16]}… "
                          f"({pico_to_xmr(amount_pico)} XMR) — releasing immediately")

    def _send_batch(self, pending: list[sqlite3.Row]) -> bool:
        """
        Send a single transaction covering all pending payouts.
        Sums each worker's share across all payouts and sends once.
        """
        total_pico = sum(p["amount_pico"] for p in pending)
        self.log.info(f"Batching {len(pending)} payout(s) — "
                      f"total {pico_to_xmr(total_pico)} XMR")

        split_lists = [
            compute_splits(p["amount_pico"], self.cfg.workers, self.cfg.total_hashrate)
            for p in pending
        ]
        merged = merge_splits(split_lists)

        for s in merged:
            self.log.info(f"  → {s['name']}: {pico_to_xmr(s['amount_pico'])} XMR "
                          f"({s['share_pct']:.2f}%)")

        # Record splits for each payout before sending
        payout_ids = []
        for p, splits in zip(pending, split_lists):
            payout_ids.append(p["id"])
            insert_splits(self.db, p["id"], splits)

        if self.dry_run:
            self.log.info("  [DRY RUN] Skipping actual transfer")
            for txid in [p["txid"] for p in pending]:
                self.processed_txids.add(txid)
            return True

        _, unlocked = self.rpc.get_balance()
        if unlocked < total_pico:
            self.log.warning(f"Insufficient unlocked balance "
                             f"({pico_to_xmr(unlocked)} XMR) — will retry next cycle")
            return False

        destinations = [
            {"amount": s["amount_pico"], "address": s["address"]}
            for s in merged if s["amount_pico"] > 0
        ]

        for attempt in range(1, self.cfg.retry_count + 1):
            try:
                result = self.rpc.transfer(destinations)
                tx_hash = result["tx_hash"]
                fee_pico = result.get("fee", 0)
                self.log.info(f"  Sent batch! tx_hash={tx_hash[:16]}… "
                              f"fee={pico_to_xmr(fee_pico)} XMR")
                for pid in payout_ids:
                    mark_payout_processed(self.db, pid, tx_hash, fee_pico)
                for p in pending:
                    self.processed_txids.add(p["txid"])
                    if p["block_height"]:
                        self.last_height = max(self.last_height, p["block_height"])
                return True
            except Exception as e:
                self.log.warning(f"  Batch transfer attempt {attempt}/{self.cfg.retry_count} failed: {e}")
                if attempt < self.cfg.retry_count:
                    time.sleep(5 * attempt)

        error_msg = f"Batch transfer failed after {self.cfg.retry_count} attempts"
        self.log.error(f"  {error_msg}")
        for pid in payout_ids:
            mark_payout_failed(self.db, pid, error_msg)
        return False

    def _send_single(self, payout: sqlite3.Row) -> bool:
        """Send one transaction for a single payout."""
        txid = payout["txid"]
        amount_pico = payout["amount_pico"]
        height = payout["block_height"]

        self.log.info(f"Processing payout: {pico_to_xmr(amount_pico)} XMR "
                      f"(txid {txid[:16]}… height {height})")

        splits = compute_splits(amount_pico, self.cfg.workers, self.cfg.total_hashrate)
        for s in splits:
            self.log.info(f"  → {s['name']}: {pico_to_xmr(s['amount_pico'])} XMR "
                          f"({s['share_pct']:.2f}%)")

        payout_id = payout["id"]
        insert_splits(self.db, payout_id, splits)

        if self.dry_run:
            self.log.info("  [DRY RUN] Skipping actual transfer")
            self.processed_txids.add(txid)
            return True

        _, unlocked = self.rpc.get_balance()
        if unlocked < amount_pico:
            self.log.warning(f"Insufficient unlocked balance "
                             f"({pico_to_xmr(unlocked)} XMR) — will retry next cycle")
            return False

        destinations = [
            {"amount": s["amount_pico"], "address": s["address"]}
            for s in splits if s["amount_pico"] > 0
        ]

        for attempt in range(1, self.cfg.retry_count + 1):
            try:
                result = self.rpc.transfer(destinations)
                tx_hash = result["tx_hash"]
                fee_pico = result.get("fee", 0)
                self.log.info(f"  Sent! tx_hash={tx_hash[:16]}… fee={pico_to_xmr(fee_pico)} XMR")
                mark_payout_processed(self.db, payout_id, tx_hash, fee_pico)
                self.processed_txids.add(txid)
                if height:
                    self.last_height = max(self.last_height, height)
                return True
            except Exception as e:
                self.log.warning(f"  Transfer attempt {attempt}/{self.cfg.retry_count} failed: {e}")
                if attempt < self.cfg.retry_count:
                    time.sleep(5 * attempt)

        error_msg = f"Transfer failed after {self.cfg.retry_count} attempts"
        self.log.error(f"  {error_msg} — payout {txid[:16]}… will be retried next cycle")
        mark_payout_failed(self.db, payout_id, error_msg)
        return False

    def run_once(self) -> int:
        """
        Run one poll cycle. Returns number of payouts forwarded to workers.

        Processing is intentionally split into two stages so that the optional
        privacy delay works correctly:

        Stage 1 — Detect: ask monero-wallet-rpc for new incoming transfers,
          record any new P2Pool payouts in the DB with a randomized release
          timestamp, then return without sending anything yet.

        Stage 2 — Send: query the DB for payouts whose release time has passed
          and forward them to workers (either one transaction per payout, or
          all batched into a single transaction if batch_payouts=True).
        """
        # Stage 1: detect and queue new incoming payouts
        try:
            new_payouts = self._find_new_payouts()
            for p in new_payouts:
                self._queue_payout(p)
        except Exception as e:
            self.log.error(f"Failed to fetch transfers: {e}")
            return 0

        # Stage 2: process queued payouts whose release window has passed
        pending = load_pending_payouts(self.db)
        if not pending:
            self.log.debug("No payouts ready to send")
            return 0

        try:
            if self.cfg.batch_payouts and len(pending) > 1:
                return 1 if self._send_batch(pending) else 0
            else:
                processed = 0
                for p in pending:
                    try:
                        if self._send_single(p):
                            processed += 1
                    except Exception as e:
                        self.log.error(f"Unexpected error processing payout "
                                       f"{p['txid'][:16]}…: {e}")
                return processed
        except Exception as e:
            self.log.error(f"Unexpected error in send stage: {e}")
            return 0

    def run_forever(self) -> None:
        self.log.info(f"Polling every {self.cfg.poll_interval}s — press Ctrl+C to stop")
        while True:
            self.run_once()
            try:
                time.sleep(self.cfg.poll_interval)
            except KeyboardInterrupt:
                self.log.info("Shutting down")
                break


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="p2pool-splitter: distribute P2Pool payouts among workers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--config", "-c", default="config.yaml",
                        help="Path to config file (YAML or JSON) [default: config.yaml]")
    parser.add_argument("--dry-run", "-n", action="store_true",
                        help="Detect payouts and compute splits but do not send any transactions")
    parser.add_argument("--once", action="store_true",
                        help="Run one poll cycle then exit (useful for cron)")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    args = parser.parse_args()

    try:
        cfg = Config(args.config)
    except (FileNotFoundError, ValueError, ImportError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    log = setup_logging(cfg.log_file, cfg.verbose)

    splitter = Splitter(cfg, dry_run=args.dry_run)

    if args.once:
        n = splitter.run_once()
        log.info(f"Done — {n} payout(s) processed")
    else:
        splitter.run_forever()


if __name__ == "__main__":
    main()
