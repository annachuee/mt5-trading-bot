# MT5 trading platform modules

## Operations notifications

`mt5_ops_webhooks.py` provides an asynchronous `OpsNotifier` for Telegram and
Slack. It queues P&L updates, errors, and emergency-kill reports and delivers
them with bounded retry/backoff. Use Telegram environment variables, Slack bot
environment variables, or both. Slack bot apps need `chat:write` and must be in
the destination channel. Telegram bots must be able to post to the target chat.

The module can also run as an inbound relay. The MT5 host can POST JSON events
to `POST /events` with `Authorization: Bearer <OPS_INGEST_TOKEN>`. Accepted
event types are `pnl`, `error`, and `emergency_kill`. `GET /health` is intended
for a private health check. Put the relay behind a TLS reverse proxy or private
network; do not expose the unauthenticated health endpoint publicly if that is
not acceptable for your deployment.

P&L example payload:

```json
{
  "event_type": "pnl",
  "payload": {
    "symbol": "EURUSD",
    "equity": 100250.0,
    "balance": 100000.0,
    "floating_pnl": 250.0,
    "daily_pnl": 250.0,
    "strategy": "ma_cross_m15"
  }
}
```

The event API is enqueue-only and replies `202`; delivery happens asynchronously.
The relay's own queue is in memory, so use a durable message broker if queued
notifications must survive container restarts.

In the live process, `OpsNotifier.publish_mt5_pnl(mt5, ...)` samples account
equity, balance, and aggregate floating P&L. `pnl_report_loop(...)` can publish
periodically. `bind_risk_breach_notifications(risk, notifier, loop)` forwards
Step 3 breach callbacks, and `emergency_kill_and_notify(risk, notifier, ...)`
runs and reports a manual kill action. Attach `AsyncWebhookLogHandler` to the
Python root logger to forward error-level logs.

## Linux container and MT5 runtime

The Dockerfile runs the notification relay and includes the backtesting stack.
It does not install the native `MetaTrader5` Python package: the published
MetaTrader5 wheels are Windows x86-64 builds. Run the live MT5 terminal/Python
adapter on a Windows VPS/VM, then send operations events to this Linux relay
over a private network or VPN. Alternatively, implement and operate a supported
broker/terminal bridge. This image alone is not a live MT5 execution host.

For lower network latency, select an AWS/GCP region close to the broker's actual
trade server endpoint and measure round-trip time from that region. Geographic
proximity to a financial center does not guarantee proximity to the broker.

Build and run the relay:

```sh
cp .env.example .env
# Fill in a notifier destination and a strong OPS_INGEST_TOKEN.
docker build -t mt5-ops-relay .
docker run --rm --env-file .env -p 8080:8080 mt5-ops-relay
```

For production, inject secrets from the cloud secret manager instead of baking
them into the image or committing them. Terminate TLS at a reverse proxy/load
balancer, and restrict `/events` ingress to the MT5 host/VPN.

## Strategy parameter versioning

`config/strategy_params.json` is a tracked sample configuration. Commit each
reviewed parameter change so Git history records the exact settings used by a
run. Inspect changes with `git diff -- config/strategy_params.json`; restore a
tracked file to a prior revision with `git checkout <commit> --
config/strategy_params.json`. `.gitignore` excludes credentials, logs, local
market data, and generated backtest output while leaving code and configuration
tracked.

Historical tick files are intentionally excluded from Git. Keep data in a
versioned object store or dataset registry and record its immutable identifier
and checksum alongside each backtest report.

## Windows production deployment

`mt5_production.py` is the Windows live-process entry point. It loads secrets
from a local `.env` using `python-dotenv`, connects the terminal, starts the
market-data collector, the 200 ms risk monitor, and the configured strategy
engine. Market execution is routed through the IOC-only OMS. The runner does
not log credential values. With `TRADING_ENABLED=false`, it starts the feed and
risk monitor and uses an OMS-compatible sink that logs and suppresses every
order. Live submission defaults to requiring a DEMO account and a config with
passing holdout metadata. The current sample config has not passed holdout
validation, so live mode requires `REQUIRE_OPTIMIZED_CONFIG=true` and stops
until a qualified configuration is committed. `ALLOW_UNOPTIMIZED_LIVE=true`
is a separate explicit override. Dry-run mode allows the safe sample
parameters and sends no orders.

On the Windows host, install 64-bit Python and MetaTrader 5, copy
`.env.example` to `.env`, set the terminal/account values and daily loss limit,
review the strategy config, then run `run_production.bat`. Keep `.env` access
restricted to the service account and operators; do not copy it into the ZIP.
The batch file creates a local virtual environment and installs the Windows
MetaTrader5 package alongside the shared Python requirements.

From a clean, committed Git checkout, run `python build_release.py`. The script
generates `run_production.bat`, requires a clean tree, packages tracked source
into `dist/mt5-platform-release-<commit>.zip`, and writes a release manifest.
It excludes `.env` secrets, logs, tick CSVs, local data, and runtime folders.
The manifest records the commit and active strategy settings without including
credentials.

## Temporary Windows cloud host

`spin_up_vps.py` can create or terminate an AWS EC2 Windows Server 2022 host
in London (`eu-west-2`) using boto3 and the standard AWS credential chain. It
requires an existing EC2 key pair, public subnet, and security group whose only
ingress is RDP from one operator IPv4 `/32`; it rejects broad RDP ingress.
Install its optional dependency with `python -m pip install -r
requirements-cloud.txt`. The script reports the instance ID and public IP but
never prints, stores, or logs a password, access key, or private key. The TTL
tag is informational; terminate the host with the printed `--terminate` command
after use. Provisioning also requires active AWS credentials and permissions.

## Extracting EURUSD ticks on Windows

Run `extract_ticks.py` on the Windows host where the logged-in MT5 terminal is
running. Install the Windows package with `py -m pip install -r
requirements-mt5-windows.txt`, then run `py extract_ticks.py`. It requests all
ticks for the last 180 days in daily UTC chunks and writes
`EURUSD_ticks.csv` beside the script (the requested `outputs` directory when
run there). The script checks that the file contains a header and data row
before reporting success. The CSV is ignored by Git because historical ticks
can be large; the optimizer can consume it directly.

## Grid optimization

`mt5_grid_optimizer.py` reads a tick CSV/Parquet file through the Backtrader
engine, evaluates fast SMA values 5–20 and slow SMA values 30–100 by default,
and writes per-pair train/holdout metrics to `backtest_results/grid_search.csv`.
It accepts native M15 OHLCV files without flattening their candle ranges, and
simulates the strategy's ADX band, UTC session window, 150-point EURUSD stop
floor, 1:2 take-profit, FX lot contract value, slippage, and commission.
Native candle entries use the next M15 open; tick-source runs apply the
configured post-signal latency at the first eligible source tick.
Set the pip/tick economics, commission schedule, slippage, and latency from the
actual instrument/account before comparing runs. Example:

```sh
python mt5_grid_optimizer.py /path/to/EURUSD_ticks.csv \
  --symbol EURUSD --pip-size 0.0001 --tick-size 0.00001 \
  --tick-value-loss 1.0 --fee-per-lot-side 3.5
```

The tool ranks candidates on the training partition using
`Sharpe + log(1 + Profit Factor) - 2 * Maximum Drawdown` and validates the
single training winner on the later holdout. It writes fast/slow periods to
`config/strategy_params.json` only if that winner also has positive holdout
return, Profit Factor above 1, positive Sharpe, and at least five closed trades.
Use `--no-write-config` to inspect a run without changing the active file.
`MT5StrategyEngine.from_config(...)` loads the resulting parameters. Review the
config diff and commit it to preserve or roll back a chosen parameter set.

When MT5 history is unavailable, run `python download_eurusd_m15.py` to fetch
the pinned public M15 sample and atomically replace the ignored
`EURUSD_ticks.csv`. The pinned repository sample covers 2026-03-26 through
2026-09-25 and was cross-checked over overlapping bars against FXCM's public
UTC minute-candle archive. Both sources are indicative and can differ from the
broker's feed; FXCM states its public prices reflect its lowest Active Trader
spreads. The CSV is ignored and is deliberately excluded from release bundles.

## System diagnostics

Run `python sys_diagnostics.py` to check Telegram `sendMessage`, Slack
`chat.postMessage`, authenticated relay ingestion, and JSON parsing of
`config/strategy_params.json`. Configure `OPS_RELAY_URL` in `.env`. This sends
visible mock notifications to each configured provider and a mock error event
through the relay; run it only when those test messages are appropriate.
