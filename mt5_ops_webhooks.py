"""Async Telegram/Slack notifications and optional inbound webhook relay.

Use ``OpsNotifier`` directly from the trading process, or run this module as a
small Linux-side relay and POST events to ``/events``. Secrets are read from
environment variables and never included in logs or request payloads.
"""

from __future__ import annotations

import asyncio
import hmac
import itertools
import json
import logging
import os
import signal
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
from aiohttp import web


@dataclass(frozen=True)
class WebhookSettings:
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    slack_bot_token: Optional[str] = None
    slack_channel_id: Optional[str] = None
    request_timeout: float = 5.0
    max_retries: int = 5
    queue_maxsize: int = 10_000

    @classmethod
    def from_env(cls) -> "WebhookSettings":
        return cls(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            slack_bot_token=os.getenv("SLACK_BOT_TOKEN"),
            slack_channel_id=os.getenv("SLACK_CHANNEL_ID"),
            request_timeout=float(os.getenv("WEBHOOK_TIMEOUT_SECONDS", "5")),
            max_retries=int(os.getenv("WEBHOOK_MAX_RETRIES", "5")),
            queue_maxsize=int(os.getenv("WEBHOOK_QUEUE_MAXSIZE", "10000")),
        )

    @property
    def has_destination(self) -> bool:
        return bool((self.telegram_bot_token and self.telegram_chat_id)
                    or (self.slack_bot_token and self.slack_channel_id))


class OpsNotifier:
    """Async buffered notifier supporting Telegram Bot API and Slack Web API."""

    def __init__(
        self,
        settings: Optional[WebhookSettings] = None,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.settings = settings or WebhookSettings.from_env()
        if not self.settings.has_destination:
            raise ValueError("Configure Telegram and/or Slack credentials")
        self.logger = logger or logging.getLogger(__name__)
        self._queue: asyncio.PriorityQueue[tuple[int, int, Optional[dict[str, Any]]]] = (
            asyncio.PriorityQueue(maxsize=self.settings.queue_maxsize)
        )
        self._counter = itertools.count()
        self._worker: Optional[asyncio.Task[None]] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._closed = False

    async def start(self) -> None:
        if self._worker is not None:
            return
        timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._worker = asyncio.create_task(self._run(), name="ops-webhook-dispatcher")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._worker is not None:
            await self._queue.join()
            await self._queue.put((100, next(self._counter), None))
            await self._worker
        if self._session is not None:
            await self._session.close()
        self._worker = None

    async def send_pnl(self, *, symbol: str, equity: float, balance: float,
                       floating_pnl: float, daily_pnl: Optional[float] = None,
                       strategy: Optional[str] = None,
                       extra: Optional[dict[str, Any]] = None) -> None:
        payload: dict[str, Any] = {
            "symbol": symbol, "equity": equity, "balance": balance,
            "floating_pnl": floating_pnl, "daily_pnl": daily_pnl,
            "strategy": strategy, **(extra or {}),
        }
        await self._enqueue("pnl", payload, priority=20)

    async def send_error(self, message: str, *, component: str = "platform",
                         details: Optional[dict[str, Any]] = None) -> None:
        await self._enqueue("error", {
            "component": component, "message": message, **(details or {}),
        }, priority=10)

    async def send_kill(self, *, reason: str, report: Optional[dict[str, Any]] = None) -> None:
        await self._enqueue("emergency_kill", {
            "reason": reason, "report": report or {},
        }, priority=0)

    async def publish_mt5_pnl(self, mt5: Any, *, symbol: str = "ACCOUNT",
                              strategy: Optional[str] = None,
                              daily_pnl: Optional[float] = None) -> None:
        """Read account/position state off-loop, then enqueue a live P&L snapshot."""
        account, positions = await asyncio.to_thread(_read_mt5_pnl, mt5)
        await self.send_pnl(
            symbol=symbol,
            equity=float(account.equity),
            balance=float(account.balance),
            floating_pnl=sum(float(getattr(position, "profit", 0.0))
                             for position in positions),
            daily_pnl=daily_pnl,
            strategy=strategy,
            extra={"open_positions": len(positions)},
        )

    async def _enqueue(self, event_type: str, payload: dict[str, Any], *, priority: int) -> None:
        if self._closed:
            raise RuntimeError("notifier is closed")
        if self._worker is None:
            await self.start()
        envelope = {
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": _json_safe(payload),
        }
        await self._queue.put((priority, next(self._counter), envelope))

    async def _run(self) -> None:
        while True:
            priority, sequence, event = await self._queue.get()
            try:
                if event is None:
                    return
                await self._deliver(event)
            except Exception:
                self.logger.exception("Notification dispatch failed")
            finally:
                self._queue.task_done()

    async def _deliver(self, event: dict[str, Any]) -> None:
        text = _format_event(event)
        jobs = []
        if self.settings.telegram_bot_token and self.settings.telegram_chat_id:
            jobs.append(self._post_telegram(text))
        if self.settings.slack_bot_token and self.settings.slack_channel_id:
            jobs.append(self._post_slack(text))
        results = await asyncio.gather(*jobs, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                self.logger.error("Notification destination failed: %s", result)

    async def _post_telegram(self, text: str) -> None:
        token = self.settings.telegram_bot_token
        assert token is not None and self._session is not None
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        await self._retry_post(url, {"chat_id": self.settings.telegram_chat_id,
                                     "text": text})

    async def _post_slack(self, text: str) -> None:
        assert self.settings.slack_bot_token is not None and self._session is not None
        await self._retry_post(
            "https://slack.com/api/chat.postMessage",
            {"channel": self.settings.slack_channel_id, "text": text,
             "mrkdwn": False, "unfurl_links": False, "unfurl_media": False},
            headers={"Authorization": f"Bearer {self.settings.slack_bot_token}"},
            slack=True,
        )

    async def _retry_post(self, url: str, body: dict[str, Any], *,
                          headers: Optional[dict[str, str]] = None,
                          slack: bool = False) -> None:
        assert self._session is not None
        for attempt in range(self.settings.max_retries + 1):
            retry_after: Optional[float] = None
            try:
                async with self._session.post(url, json=body, headers=headers) as response:
                    data = await response.json(content_type=None)
                    if response.status == 429:
                        raw = response.headers.get("Retry-After")
                        retry_after = float(raw) if raw else None
                    elif response.status >= 500:
                        pass
                    elif response.status >= 400:
                        raise RuntimeError(f"Webhook HTTP {response.status}: {_safe_error(data)}")
                    elif slack and not data.get("ok", False):
                        raise RuntimeError(f"Slack API error: {data.get('error', 'unknown')}")
                    elif not slack and data.get("ok") is False:
                        raise RuntimeError(f"Telegram API error: {_safe_error(data)}")
                    else:
                        return
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self.settings.max_retries:
                    # Do not include the request URL: Telegram's bot token is
                    # embedded in that URL by the Bot API.
                    raise RuntimeError(
                        f"Webhook request failed after retries ({type(exc).__name__})"
                    ) from None
            if attempt >= self.settings.max_retries:
                raise RuntimeError("Webhook server remained unavailable after retries")
            delay = retry_after if retry_after is not None else min(0.5 * (2 ** attempt), 15.0)
            await asyncio.sleep(delay)


class AsyncWebhookLogHandler(logging.Handler):
    """Forward ERROR+ LogRecords from sync code to an active notifier loop."""

    def __init__(self, notifier: OpsNotifier, loop: asyncio.AbstractEventLoop,
                 level: int = logging.ERROR) -> None:
        super().__init__(level=level)
        self.notifier = notifier
        self.loop = loop

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        self.loop.call_soon_threadsafe(
            lambda: asyncio.create_task(self.notifier.send_error(
                message, component=record.name,
                details={"level": record.levelname},
            ))
        )


def bind_risk_breach_notifications(risk_engine: Any, notifier: OpsNotifier,
                                   loop: asyncio.AbstractEventLoop) -> None:
    """Wire Step 3 risk breach callbacks to priority emergency notifications.

    The risk engine's own automatic kill switch remains responsible for
    cancel/flatten actions. This callback only reports the trigger.
    """
    previous = getattr(risk_engine, "on_breach", None)

    def on_breach(reason: str) -> None:
        def schedule() -> None:
            asyncio.create_task(notifier.send_kill(
                reason=reason, report={"source": "automatic risk breach"}
            ))
        loop.call_soon_threadsafe(schedule)
        if previous is not None:
            previous(reason)

    risk_engine.on_breach = on_breach


async def pnl_report_loop(mt5: Any, notifier: OpsNotifier, *, interval_seconds: float = 5.0,
                          symbol: str = "ACCOUNT", strategy: Optional[str] = None,
                          daily_pnl_provider: Optional[Any] = None) -> None:
    """Continuously enqueue account P&L snapshots until the task is cancelled."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    while True:
        daily_pnl = daily_pnl_provider() if daily_pnl_provider is not None else None
        if asyncio.iscoroutine(daily_pnl):
            daily_pnl = await daily_pnl
        try:
            await notifier.publish_mt5_pnl(mt5, symbol=symbol,
                                            strategy=strategy, daily_pnl=daily_pnl)
        except Exception:
            logging.getLogger(__name__).exception("Could not publish MT5 P&L snapshot")
        await asyncio.sleep(interval_seconds)


async def emergency_kill_and_notify(risk_engine: Any, notifier: OpsNotifier, *,
                                    reason: str = "manual emergency stop") -> dict[str, Any]:
    """Run the Step 3 account-wide kill switch off-loop and report its outcome."""
    report = await asyncio.to_thread(risk_engine.emergency_kill_switch, reason=reason)
    await notifier.send_kill(reason=reason, report=report)
    return report


def create_relay_app(notifier: OpsNotifier, ingest_token: str) -> web.Application:
    """Create a small authenticated inbound relay for Windows MT5 hosts."""
    if len(ingest_token) < 24:
        raise ValueError("OPS_INGEST_TOKEN must be at least 24 characters")
    app = web.Application(client_max_size=64 * 1024)
    app["notifier"] = notifier
    app["ingest_token"] = ingest_token
    app.router.add_get("/health", _health)
    app.router.add_post("/events", _ingest_event)
    return app


async def _health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "queue_size": request.app["notifier"]._queue.qsize()})


async def _ingest_event(request: web.Request) -> web.Response:
    expected = f"Bearer {request.app['ingest_token']}"
    supplied = request.headers.get("Authorization", "")
    if not hmac.compare_digest(supplied, expected):
        raise web.HTTPUnauthorized()
    try:
        data = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise web.HTTPBadRequest(text="Expected a JSON object")
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text="Expected a JSON object")
    event_type = data.get("event_type")
    payload = data.get("payload", {})
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="payload must be an object")
    notifier: OpsNotifier = request.app["notifier"]
    if event_type == "pnl":
        required = {"symbol", "equity", "balance", "floating_pnl"}
        missing = required - payload.keys()
        if missing:
            raise web.HTTPBadRequest(text=f"Missing P&L fields: {sorted(missing)}")
        await notifier.send_pnl(
            symbol=str(payload["symbol"]), equity=float(payload["equity"]),
            balance=float(payload["balance"]), floating_pnl=float(payload["floating_pnl"]),
            daily_pnl=_optional_float(payload.get("daily_pnl")),
            strategy=payload.get("strategy"),
            extra={k: v for k, v in payload.items()
                   if k not in {"symbol", "equity", "balance", "floating_pnl", "daily_pnl", "strategy"}},
        )
    elif event_type == "error":
        await notifier.send_error(str(payload.get("message", "unspecified error")),
                                  component=str(payload.get("component", "platform")),
                                  details={k: v for k, v in payload.items()
                                           if k not in {"message", "component"}})
    elif event_type in {"kill", "emergency_kill"}:
        await notifier.send_kill(reason=str(payload.get("reason", "unspecified")),
                                 report=payload.get("report", {}))
    else:
        raise web.HTTPBadRequest(text="event_type must be pnl, error, or emergency_kill")
    return web.json_response({"queued": True}, status=202)


async def _run_relay() -> None:
    settings = WebhookSettings.from_env()
    notifier = OpsNotifier(settings)
    await notifier.start()
    app = create_relay_app(notifier, os.environ["OPS_INGEST_TOKEN"])
    runner = web.AppRunner(app, access_log=logging.getLogger("aiohttp.access"))
    await runner.setup()
    site = web.TCPSite(runner, os.getenv("OPS_BIND_HOST", "0.0.0.0"),
                       int(os.getenv("OPS_PORT", "8080")))
    await site.start()
    logging.getLogger(__name__).info("Operations relay listening on port %s",
                                     os.getenv("OPS_PORT", "8080"))
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopped.set)
        except NotImplementedError:
            pass
    try:
        await stopped.wait()
    finally:
        await runner.cleanup()
        await notifier.close()


def _format_event(event: dict[str, Any]) -> str:
    titles = {"pnl": "LIVE P&L", "error": "PLATFORM ERROR", "emergency_kill": "EMERGENCY KILL"}
    payload = json.dumps(event["payload"], ensure_ascii=False, sort_keys=True)
    return f"{titles.get(event['event_type'], event['event_type'])}\n{event['timestamp']}\n{payload}"[:4000]


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _safe_error(data: Any) -> str:
    if isinstance(data, dict):
        return str(data.get("description") or data.get("error") or data)[:300]
    return str(data)[:300]


def _optional_float(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def _read_mt5_pnl(mt5: Any) -> tuple[Any, tuple[Any, ...]]:
    account = mt5.account_info()
    positions = mt5.positions_get()
    if account is None:
        raise RuntimeError(f"MT5 account_info failed: {mt5.last_error()}")
    if positions is None:
        raise RuntimeError(f"MT5 positions_get failed: {mt5.last_error()}")
    return account, tuple(positions)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    asyncio.run(_run_relay())
