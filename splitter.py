#!/usr/bin/env python3
"""
p2pool-splitter — Automated P2Pool mining reward splitter
Monitors a Monero wallet for P2Pool payouts and distributes them
proportionally among configured workers based on hashrate share.

Usage: python splitter.py --config config.yaml [--dry-run] [--once]
"""

import argparse
import json
import logging
import os
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

__version__ = "1.0.0"

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
        # Fallback: minimal YAML parser for simple key: value files
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
CREATE TABLE IF NOT EXISTS payouts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    txid            TEXT    UNIQUE NOT NULL,
    amount_pico     INTEGER NOT NULL,
    block_height    INTEGER,
    detected_at     INTEGER NOT NULL,
    processed       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS splits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    payout_id       INTEGER NOT NULL REFERENCES payouts(id),
    worker_name     TEXT    NOT NULL,
    worker_address  TEXT    NOT NULL,
    amount_pico     INTEGER NOT NULL,
    tx_hash         TEXT,
    fee_pico        INTEGER,
    sent_at         INTEGER,
    status          TEXT    NOT NULL DEFAULT 'pending',
    error           TEXT
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
    conn.commit()
    return conn


def load_processed_txids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT txid FROM payouts WHERE processed = 1").fetchall()
    return {r["txid"] for r in rows}


def insert_payout(conn: sqlite3.Connection, txid: str, amount_pico: int,
                  block_height: int | None) -> int:
    cur = conn.execute(
        "INSERT OR IGNORE INTO payouts (txid, amount_pico, block_height, detected_at) VALUES (?, ?, ?, ?)",
        (txid, amount_pico, block_height, int(time.time()))
    )
    conn.commit()
    if cur.lastrowid and cur.lastrowid > 0:
        return cur.lastrowid
    # Already exists — return existing id
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
            "ring_size": 16,
            "account_index": 0,
        })

    def get_height(self) -> int:
        r = self._call("get_height")
        return r["height"]


# ---------------------------------------------------------------------------
# Core splitting logic
# ---------------------------------------------------------------------------

def compute_splits(amount_pico: int, workers: list[dict],
                   total_hashrate: int) -> list[dict]:
    """
    Compute per-worker split amounts (picoXMR).
    Remainder from integer division goes to the first worker.
    """
    splits = []
    allocated = 0
    for i, w in enumerate(workers):
        share = w["hashrate"] / total_hashrate
        worker_amount = int(amount_pico * share)
        splits.append({
            "name": w["name"],
            "address": w["address"],
            "hashrate": w["hashrate"],
            "share_pct": share * 100,
            "amount_pico": worker_amount,
        })
        allocated += worker_amount

    # Give remainder to first worker to avoid losing picoXMR to rounding
    remainder = amount_pico - allocated
    if remainder > 0:
        splits[0]["amount_pico"] += remainder

    return splits


PICO_PER_XMR = 1_000_000_000_000


def pico_to_xmr(pico: int) -> str:
    return f"{pico / PICO_PER_XMR:.12f}"


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
        for w in self.cfg.workers:
            pct = w["hashrate"] / self.cfg.total_hashrate * 100
            self.log.info(f"  {w['name']}: {w['hashrate']:,} H/s ({pct:.1f}%) → {w['address'][:16]}…")

    def _get_last_height(self) -> int:
        """Resume from the last processed payout's block height."""
        row = self.db.execute(
            "SELECT MAX(block_height) AS h FROM payouts WHERE processed = 1"
        ).fetchone()
        h = row["h"] if row and row["h"] else 0
        return max(0, h - 10)  # small overlap to avoid missing payouts near edges

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

            new.append({
                "txid": txid,
                "amount": amount,
                "height": height,
                "confirmations": confirmations,
            })

        return new

    def _process_payout(self, payout: dict) -> bool:
        txid = payout["txid"]
        amount_pico = payout["amount"]
        height = payout.get("height")

        self.log.info(f"New payout: {pico_to_xmr(amount_pico)} XMR "
                      f"(txid {txid[:16]}… height {height})")

        splits = compute_splits(amount_pico, self.cfg.workers, self.cfg.total_hashrate)

        for s in splits:
            self.log.info(f"  → {s['name']}: {pico_to_xmr(s['amount_pico'])} XMR "
                          f"({s['share_pct']:.2f}%)")

        payout_id = insert_payout(self.db, txid, amount_pico, height)
        insert_splits(self.db, payout_id, splits)

        if self.dry_run:
            self.log.info("  [DRY RUN] Skipping actual transfer")
            self.processed_txids.add(txid)
            return True

        # Check unlocked balance
        _, unlocked = self.rpc.get_balance()
        if unlocked < amount_pico:
            self.log.warning(f"Insufficient unlocked balance "
                             f"({pico_to_xmr(unlocked)} XMR) — will retry next cycle")
            return False

        # Build destinations (exclude any worker with 0 amount)
        destinations = [
            {"amount": s["amount_pico"], "address": s["address"]}
            for s in splits if s["amount_pico"] > 0
        ]

        # Send with retries
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
        """Run one poll cycle. Returns number of payouts processed."""
        try:
            payouts = self._find_new_payouts()
        except Exception as e:
            self.log.error(f"Failed to fetch transfers: {e}")
            return 0

        if not payouts:
            self.log.debug("No new payouts found")
            return 0

        processed = 0
        for p in payouts:
            try:
                if self._process_payout(p):
                    processed += 1
            except Exception as e:
                self.log.error(f"Unexpected error processing payout {p.get('txid','?')[:16]}…: {e}")

        return processed

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
