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

## System diagnostics

Run `python sys_diagnostics.py` to check Telegram `sendMessage`, Slack
`chat.postMessage`, authenticated relay ingestion, and JSON parsing of
`config/strategy_params.json`. Configure `OPS_RELAY_URL` in `.env`. This sends
visible mock notifications to each configured provider and a mock error event
through the relay; run it only when those test messages are appropriate.
