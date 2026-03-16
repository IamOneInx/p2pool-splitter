-- p2pool-splitter database schema
-- Applied automatically by splitter.py on startup.
-- You can also apply it manually: sqlite3 splitter.db < schema.sql

CREATE TABLE IF NOT EXISTS payouts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    txid            TEXT    UNIQUE NOT NULL,   -- Monero transaction ID of the incoming P2Pool payout
    amount_pico     INTEGER NOT NULL,          -- Amount in picoXMR (1 XMR = 1e12 picoXMR)
    block_height    INTEGER,                   -- Block height when payout was received
    detected_at     INTEGER NOT NULL,          -- Unix timestamp when splitter detected this payout
    processed       INTEGER NOT NULL DEFAULT 0 -- 1 = split transaction sent, 0 = pending
);

CREATE TABLE IF NOT EXISTS splits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    payout_id       INTEGER NOT NULL REFERENCES payouts(id),
    worker_name     TEXT    NOT NULL,          -- Worker name from config
    worker_address  TEXT    NOT NULL,          -- Worker Monero address
    amount_pico     INTEGER NOT NULL,          -- Amount owed to this worker (picoXMR)
    tx_hash         TEXT,                      -- Hash of the outbound split transaction
    fee_pico        INTEGER,                   -- Transaction fee paid (picoXMR), NULL until sent
    sent_at         INTEGER,                   -- Unix timestamp when split was sent
    status          TEXT    NOT NULL DEFAULT 'pending',  -- pending / sent / failed
    error           TEXT                       -- Error message if status = 'failed'
);

CREATE INDEX IF NOT EXISTS idx_payouts_txid      ON payouts(txid);
CREATE INDEX IF NOT EXISTS idx_payouts_processed ON payouts(processed);
CREATE INDEX IF NOT EXISTS idx_splits_payout_id  ON splits(payout_id);
CREATE INDEX IF NOT EXISTS idx_splits_status     ON splits(status);

-- Useful queries:
--
-- See all payouts:
--   SELECT txid, amount_pico / 1e12 AS xmr, processed, datetime(detected_at, 'unixepoch') FROM payouts;
--
-- See splits for a payout:
--   SELECT worker_name, amount_pico / 1e12 AS xmr, status, tx_hash FROM splits WHERE payout_id = 1;
--
-- Total paid per worker:
--   SELECT worker_name, SUM(amount_pico) / 1e12 AS total_xmr FROM splits WHERE status = 'sent' GROUP BY worker_name;
--
-- Pending splits (not yet sent):
--   SELECT p.txid, s.worker_name, s.amount_pico / 1e12 AS xmr
--   FROM splits s JOIN payouts p ON s.payout_id = p.id
--   WHERE s.status = 'pending';
