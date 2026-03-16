# p2pool-splitter

Automatically distributes P2Pool mining rewards among multiple worker wallets in proportion to their hashrate contribution.

## How it works

When multiple mining rigs connect to a single P2Pool node (all pointing to the same `--wallet` address), the pool operator receives all payouts. p2pool-splitter monitors that wallet via `monero-wallet-rpc`, detects incoming P2Pool coinbase transactions, and automatically forwards each worker's proportional share to their individual Monero address — all in a single on-chain transaction to minimize fees.

```
P2Pool node → Operator wallet → p2pool-splitter → Worker A (50%)
                                                 → Worker B (30%)
                                                 → Worker C (20%)
```

All split history is recorded in a local SQLite database for auditing.

## Requirements

- Python 3.8+
- `requests` and `pyyaml` libraries (`pip install -r requirements.txt`)
- A running `monero-wallet-rpc` instance connected to the pool operator's wallet

## Quick start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy and edit config
cp config.example.yaml config.yaml
nano config.yaml          # set wallet_rpc_url and workers

# 3. Dry run — detects payouts and logs splits without sending anything
python splitter.py --config config.yaml --dry-run

# 4. Run for real
python splitter.py --config config.yaml
```

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `wallet_rpc_url` | `http://127.0.0.1:18082/json_rpc` | URL of monero-wallet-rpc |
| `wallet_rpc_user` | `""` | RPC auth username (if set) |
| `wallet_rpc_password` | `""` | RPC auth password (if set) |
| `poll_interval` | `60` | Seconds between payout checks |
| `min_payout_threshold` | `300000000` | Minimum picoXMR to treat as a payout (0.0003 XMR) |
| `confirmations_required` | `1` | Confirmations before processing |
| `retry_count` | `3` | Transfer retry attempts on failure |
| `db_path` | `splitter.db` | SQLite database file path |
| `log_file` | `null` | Log file path (optional) |
| `verbose` | `false` | Enable debug logging |

Workers are defined as a list:

```yaml
workers:
  - name: rig1
    address: "48..."   # Monero address or subaddress
    hashrate: 5000     # H/s (consistent units across all workers)
  - name: rig2
    address: "47..."
    hashrate: 3000
```

Split percentages are calculated as `worker_hashrate / total_hashrate`. JSON config format is also supported — rename to `config.json`.

## Starting monero-wallet-rpc

```bash
monero-wallet-rpc \
  --wallet-file /path/to/your/wallet \
  --password "your_wallet_password" \
  --rpc-bind-port 18082 \
  --rpc-bind-ip 127.0.0.1 \
  --disable-rpc-login \
  --daemon-address 127.0.0.1:18081
```

If using RPC login add `--rpc-login user:password` and set `wallet_rpc_user` / `wallet_rpc_password` in the config.

## Running as a systemd service

```bash
# Copy files
sudo cp splitter.py /opt/p2pool-splitter/
sudo cp config.yaml /opt/p2pool-splitter/
sudo cp p2pool-splitter.service /etc/systemd/system/

# Create service user
sudo useradd -r -s /bin/false p2pool
sudo chown -R p2pool:p2pool /opt/p2pool-splitter

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable p2pool-splitter
sudo systemctl start p2pool-splitter
sudo journalctl -u p2pool-splitter -f
```

## Running with Docker

```bash
mkdir data
cp config.example.yaml data/config.yaml
nano data/config.yaml   # edit your settings

docker compose up -d
docker compose logs -f
```

## Auditing splits

The SQLite database records every payout and split:

```bash
sqlite3 splitter.db

-- All payouts processed
SELECT txid, amount_pico / 1e12 AS xmr, processed,
       datetime(detected_at, 'unixepoch') AS detected
FROM payouts;

-- Total earned per worker
SELECT worker_name, SUM(amount_pico) / 1e12 AS total_xmr
FROM splits WHERE status = 'sent'
GROUP BY worker_name;

-- Any failed splits
SELECT p.txid, s.worker_name, s.error
FROM splits s JOIN payouts p ON s.payout_id = p.id
WHERE s.status = 'failed';
```

## CLI options

```
python splitter.py --help

  --config, -c    Config file path [default: config.yaml]
  --dry-run, -n   Detect and log splits without sending transactions
  --once          Run one poll cycle then exit (useful for cron)
  --version       Show version
```

## How P2Pool payouts work

P2Pool is a trustless decentralized pool. When the pool finds a Monero block, the block template directly encodes outputs for all wallets in the PPLNS window. The Monero network itself pays each wallet — there is no pool-side payout transaction.

p2pool-splitter is designed for the case where multiple workers mine to a **single** pool operator wallet. The operator wants to redistribute earnings proportionally. Each payout is detected as an incoming transfer in `monero-wallet-rpc` and forwarded to workers in a single on-chain transaction.

Workers who connect P2Pool with their **own** wallet address receive payment directly from the Monero network and do not need this tool.

## License

MIT
