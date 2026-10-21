"""Cached pre-trade risk gates and account-wide emergency controls for MT5.

This module expects an initialized MetaTrader5 Python module. The monitor
refreshes terminal connectivity, account equity, and configured spreads every
200 ms by default. ``check_order`` reads only cached state so its normal path
avoids MT5 IPC. Python scheduling and terminal IPC do not provide a hard
sub-millisecond latency guarantee.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from mt5_news_calendar import (
    DEFAULT_CALENDAR_URL,
    CalendarUnavailable,
    EconomicEvent,
    fetch_weekly_calendar,
)


class RiskBlocked(RuntimeError):
    """Raised when a pre-trade risk gate blocks an order."""


@dataclass(frozen=True)
class RiskSnapshot:
    captured_monotonic: float
    terminal_connected: bool
    equity: Optional[float]
    daily_pnl: Optional[float]
    daily_loss: Optional[float]
    breached: bool
    reasons: tuple[str, ...]
    news_calendar_available: bool
    news_blackout: bool
    news_reason: Optional[str]


class MT5PreTradeRisk:
    """Independent risk gate that can wrap any OMS submission callable.

    Args:
        mt5_module: Imported ``MetaTrader5`` module on the OMS connection.
        symbols: Symbols whose spreads should be monitored.
        max_daily_loss: Maximum currency drawdown from start-of-day equity.
        max_daily_loss_pct: Optional limit as a fraction of baseline equity.
        start_of_day_equity: Persisted baseline if one has already been captured;
            otherwise account equity is sampled when monitoring starts.
        spread_multiplier: Block when spread exceeds this multiple of the
            rolling median, after ``spread_min_samples`` observations.
        max_spread_points: Optional hard spread ceiling in broker points.
        quote_max_age_seconds: Fail closed on a quote older than this age.

    ``check_order`` is intended to run directly before dispatch into the OMS.
    The monitor's snapshot is cached; configure every orderable symbol in
    ``symbols`` so the spread gate has fresh observations for it.
    """

    def __init__(
        self,
        mt5_module: Any,
        symbols: list[str] | tuple[str, ...],
        *,
        max_daily_loss: Optional[float] = None,
        max_daily_loss_pct: Optional[float] = None,
        start_of_day_equity: Optional[float] = None,
        heartbeat_interval: float = 0.2,
        heartbeat_max_age: float = 0.6,
        spread_multiplier: float = 3.0,
        max_spread_points: Optional[float] = None,
        spread_window: int = 60,
        spread_min_samples: int = 10,
        quote_max_age_seconds: float = 1.0,
        auto_kill_switch: bool = True,
        news_enabled: bool = True,
        news_calendar_url: str = DEFAULT_CALENDAR_URL,
        news_refresh_interval: float = 900.0,
        news_max_age_seconds: float = 7200.0,
        news_blackout_minutes: float = 30.0,
        news_currencies: tuple[str, ...] = ("USD", "EUR", "GBP"),
        logger: Optional[logging.Logger] = None,
        on_breach: Optional[Callable[[str], None]] = None,
    ) -> None:
        if not symbols:
            raise ValueError("at least one symbol is required")
        if max_daily_loss is None and max_daily_loss_pct is None:
            raise ValueError("set max_daily_loss and/or max_daily_loss_pct")
        if max_daily_loss is not None and max_daily_loss <= 0:
            raise ValueError("max_daily_loss must be positive")
        if max_daily_loss_pct is not None and not 0 < max_daily_loss_pct < 1:
            raise ValueError("max_daily_loss_pct must be a fraction in (0, 1)")
        if heartbeat_interval <= 0 or heartbeat_max_age <= heartbeat_interval:
            raise ValueError("heartbeat age must exceed a positive interval")
        if spread_multiplier <= 1 or spread_window < 1 or spread_min_samples < 1:
            raise ValueError("invalid spread detector configuration")
        if news_refresh_interval <= 0 or news_max_age_seconds <= 0 or news_blackout_minutes <= 0:
            raise ValueError("news refresh, freshness, and blackout settings must be positive")
        if not news_currencies:
            raise ValueError("news_currencies must not be empty")

        self.mt5 = mt5_module
        self.symbols = tuple(dict.fromkeys(symbols))
        self.max_daily_loss = max_daily_loss
        self.max_daily_loss_pct = max_daily_loss_pct
        self._baseline_equity = start_of_day_equity
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_max_age = heartbeat_max_age
        self.spread_multiplier = spread_multiplier
        self.max_spread_points = max_spread_points
        self.spread_min_samples = spread_min_samples
        self.quote_max_age_seconds = quote_max_age_seconds
        self.auto_kill_switch = auto_kill_switch
        self.news_enabled = bool(news_enabled)
        self.news_calendar_url = news_calendar_url
        self.news_refresh_interval = float(news_refresh_interval)
        self.news_max_age_seconds = float(news_max_age_seconds)
        self.news_blackout = timedelta(minutes=float(news_blackout_minutes))
        self.news_currencies = tuple(code.upper() for code in news_currencies)
        self.logger = logger or logging.getLogger(__name__)
        self.on_breach = on_breach

        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._news_thread: Optional[threading.Thread] = None
        self._last_heartbeat = 0.0
        self._terminal_connected = False
        self._equity: Optional[float] = None
        self._daily_pnl: Optional[float] = None
        self._daily_loss: Optional[float] = None
        self._breached = False
        self._breach_reasons: tuple[str, ...] = ()
        self._spreads: dict[str, deque[float]] = {
            symbol: deque(maxlen=spread_window) for symbol in self.symbols
        }
        self._quote_seen: dict[str, float] = {}
        self._spread_breaches: set[str] = set()
        self._kill_lock = threading.Lock()
        self._killed = False
        self._news_available = not self.news_enabled
        self._news_last_success = 0.0
        self._news_events: tuple[EconomicEvent, ...] = ()
        self._news_error: Optional[str] = None

    @property
    def baseline_equity(self) -> Optional[float]:
        return self._baseline_equity

    def start(self) -> None:
        """Start the 200 ms monitor; capture baseline if not provided."""
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._monitor, name="mt5-pretrade-risk", daemon=True
        )
        self._thread.start()
        if self.news_enabled and not (self._news_thread and self._news_thread.is_alive()):
            self._news_thread = threading.Thread(
                target=lambda: asyncio.run(self._news_refresh_loop()),
                name="mt5-news-calendar", daemon=True,
            )
            self._news_thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)
            if self._thread.is_alive():
                raise TimeoutError("Risk monitor did not stop before timeout")
        if self._news_thread:
            self._news_thread.join(timeout)
            if self._news_thread.is_alive():
                raise TimeoutError("News calendar refresh did not stop before timeout")

    def check_order(self, symbol: str) -> None:
        """Fast, cached fail-closed check. Call immediately before OMS submit."""
        now = time.monotonic()
        with self._lock:
            reasons: list[str] = []
            if self._killed:
                reasons.append("emergency kill switch is latched")
            if now - self._last_heartbeat > self.heartbeat_max_age:
                reasons.append("terminal heartbeat is stale")
            if not self._terminal_connected:
                reasons.append("MT5 terminal reports disconnected")
            if self._breached:
                reasons.extend(self._breach_reasons or ("daily loss limit breached",))
            if self.news_enabled:
                news_reason = self._news_gate_reason_locked(datetime.now(timezone.utc), now)
                if news_reason:
                    reasons.append(news_reason)
            if symbol not in self._spreads:
                reasons.append(f"no spread monitor configured for {symbol}")
            else:
                seen = self._quote_seen.get(symbol)
                if seen is None or now - seen > self.quote_max_age_seconds:
                    reasons.append(f"spread quote stale for {symbol}")
                if symbol in self._spread_breaches:
                    reasons.append(f"abnormal spread for {symbol}")
            if reasons:
                raise RiskBlocked("; ".join(dict.fromkeys(reasons)))

    def submit(self, symbol: str, oms_callable: Callable[..., Any], *args: Any,
               **kwargs: Any) -> Any:
        """Gate then call an OMS method, preventing accidental bypass at call site."""
        self.check_order(symbol)
        return oms_callable(*args, **kwargs)

    def snapshot(self) -> RiskSnapshot:
        with self._lock:
            return RiskSnapshot(
                captured_monotonic=self._last_heartbeat,
                terminal_connected=self._terminal_connected,
                equity=self._equity,
                daily_pnl=self._daily_pnl,
                daily_loss=self._daily_loss,
                breached=self._breached,
                reasons=self._breach_reasons,
                news_calendar_available=(
                    not self.news_enabled or
                    (self._news_available and time.monotonic() - self._news_last_success
                     <= self.news_max_age_seconds)
                ),
                news_blackout=bool(self.news_enabled and self._active_news_reason_locked(
                    datetime.now(timezone.utc)
                )),
                news_reason=(self._news_gate_reason_locked(datetime.now(timezone.utc), time.monotonic())
                             if self.news_enabled else None),
            )

    def _news_gate_reason_locked(self, now_utc: datetime, now_monotonic: float) -> Optional[str]:
        if not self._news_available:
            detail = f": {self._news_error}" if self._news_error else ""
            return f"economic calendar unavailable; entries fail closed{detail}"
        age = now_monotonic - self._news_last_success
        if age < 0 or age > self.news_max_age_seconds:
            return "economic calendar is stale; entries fail closed"
        return self._active_news_reason_locked(now_utc)

    def _active_news_reason_locked(self, now_utc: datetime) -> Optional[str]:
        for event in self._news_events:
            start = event.release_utc - self.news_blackout
            end = event.release_utc + self.news_blackout
            if start <= now_utc <= end:
                return (
                    f"high-impact {event.currency} news blackout: {event.title} at "
                    f"{event.release_utc.isoformat()} (UTC)"
                )
        return None

    async def _news_refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                events = await fetch_weekly_calendar(
                    self.news_calendar_url, currencies=self.news_currencies,
                )
            except Exception as exc:
                with self._lock:
                    self._news_available = False
                    self._news_error = str(exc)
                self.logger.error("Economic calendar refresh failed; news gate closed: %s", exc)
            else:
                with self._lock:
                    self._news_events = events
                    self._news_available = True
                    self._news_last_success = time.monotonic()
                    self._news_error = None
                self.logger.info(
                    "Economic calendar refreshed: %d high-impact USD/EUR/GBP events",
                    len(events),
                )
            try:
                await asyncio.to_thread(self._stop.wait, self.news_refresh_interval)
            except RuntimeError:
                # Interpreter shutdown may dismantle the default executor.
                return

    def emergency_kill_switch(self, *, reason: str = "manual emergency stop",
                              deviation: int = 50) -> dict[str, Any]:
        """Latch the gate, cancel all active orders, and close all positions.

        Operates globally across symbols and strategies. It is best effort:
        MT5/broker rejections, market closure, disconnection, or partial fills
        can prevent immediate completion. Inspect the returned per-ticket
        results and reconcile account state after the call.
        """
        with self._kill_lock:
            with self._lock:
                self._killed = True
                self._breached = True
                self._breach_reasons = (reason,)
            report: dict[str, Any] = {"reason": reason, "canceled": [], "closed": [],
                                      "errors": []}
            terminal = self.mt5.terminal_info()
            if terminal is None or not bool(getattr(terminal, "connected", False)):
                report["errors"].append("MT5 terminal disconnected; emergency actions not sent")
                return report

            # Snapshot first, then send one remove request per pending ticket.
            orders = self.mt5.orders_get()
            if orders is None:
                report["errors"].append(f"orders_get failed: {self.mt5.last_error()}")
                orders = ()
            for order in orders:
                request = {
                    "action": self.mt5.TRADE_ACTION_REMOVE,
                    "order": int(order.ticket),
                    "symbol": str(order.symbol),
                    "comment": "risk-kill cancel",
                }
                result = self.mt5.order_send(request)
                self._append_result(report, "canceled", int(order.ticket), result)

            positions = self.mt5.positions_get()
            if positions is None:
                report["errors"].append(f"positions_get failed: {self.mt5.last_error()}")
                positions = ()
            for position in positions:
                try:
                    tick = self.mt5.symbol_info_tick(position.symbol)
                    if tick is None:
                        report["errors"].append(
                            f"No current tick for position {position.ticket} ({position.symbol})"
                        )
                        continue
                    is_buy = int(position.type) == int(self.mt5.POSITION_TYPE_BUY)
                    side = self.mt5.ORDER_TYPE_SELL if is_buy else self.mt5.ORDER_TYPE_BUY
                    price = tick.bid if is_buy else tick.ask
                    request = {
                        "action": self.mt5.TRADE_ACTION_DEAL,
                        "position": int(position.ticket),
                        "symbol": str(position.symbol),
                        "volume": float(position.volume),
                        "type": side,
                        "price": float(price),
                        "deviation": int(deviation),
                        "magic": int(getattr(position, "magic", 0)),
                        "comment": "risk-kill flatten",
                        "type_time": self.mt5.ORDER_TIME_GTC,
                        "type_filling": self._emergency_fill_policy(position.symbol),
                    }
                    result = self.mt5.order_send(request)
                    self._append_result(report, "closed", int(position.ticket), result)
                except Exception as exc:
                    report["errors"].append(f"Position {position.ticket}: {exc}")
            return report

    def reset_kill_switch(self) -> None:
        """Explicitly clear the manual latch after an operator review."""
        with self._lock:
            if self._breached:
                raise RiskBlocked("clear the underlying breach before resetting kill switch")
            self._killed = False

    def _monitor(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self._refresh()
            except Exception:
                self.logger.exception("Risk monitor refresh failed; gate will fail closed")
                with self._lock:
                    self._terminal_connected = False
                    self._last_heartbeat = time.monotonic()
            elapsed = time.monotonic() - started
            self._stop.wait(max(0.0, self.heartbeat_interval - elapsed))

    def _refresh(self) -> None:
        terminal = self.mt5.terminal_info()
        now = time.monotonic()
        connected = terminal is not None and bool(getattr(terminal, "connected", False))
        account = self.mt5.account_info() if connected else None
        if connected and account is None:
            connected = False

        with self._lock:
            self._last_heartbeat = now
            self._terminal_connected = connected
            if not connected:
                return
            equity = float(account.equity)
            if self._baseline_equity is None:
                self._baseline_equity = equity
                self.logger.info("Captured start-of-day equity baseline: %.2f", equity)
            self._equity = equity
            self._daily_pnl = equity - self._baseline_equity
            self._daily_loss = max(0.0, -self._daily_pnl)
            limits = []
            if self.max_daily_loss is not None:
                limits.append(self.max_daily_loss)
            if self.max_daily_loss_pct is not None:
                limits.append(self._baseline_equity * self.max_daily_loss_pct)
            limit = min(limits) if limits else float("inf")
            if self._daily_loss >= limit:
                reason = f"daily loss {self._daily_loss:.2f} reached limit {limit:.2f}"
                self._raise_breach_locked(reason)

        # Quote calls remain off the pre-trade path; a failed quote leaves the
        # previous observation to age out and block orders when stale.
        for symbol in self.symbols:
            try:
                info = self.mt5.symbol_info(symbol)
                tick = self.mt5.symbol_info_tick(symbol)
                if info is None or tick is None or float(info.point) <= 0:
                    continue
                spread_points = (float(tick.ask) - float(tick.bid)) / float(info.point)
                if spread_points < 0:
                    continue
                with self._lock:
                    series = self._spreads[symbol]
                    baseline = statistics.median(series) if len(series) >= self.spread_min_samples else None
                    series.append(spread_points)
                    self._quote_seen[symbol] = time.monotonic()
                    breached = (
                        (self.max_spread_points is not None and spread_points > self.max_spread_points)
                        or (baseline is not None and spread_points > max(1.0, baseline) * self.spread_multiplier)
                    )
                    if breached:
                        self._spread_breaches.add(symbol)
                        self._raise_breach_locked(
                            f"abnormal spread for {symbol}: {spread_points:.2f} points"
                        )
                    else:
                        self._spread_breaches.discard(symbol)
            except Exception:
                self.logger.exception("Failed to update spread monitor for %s", symbol)

    def _raise_breach_locked(self, reason: str) -> None:
        first = not self._breached
        self._breached = True
        if reason not in self._breach_reasons:
            self._breach_reasons = (*self._breach_reasons, reason)
        if first:
            # Run kill actions and user code outside the monitor thread/lock.
            if self.auto_kill_switch:
                threading.Thread(
                    target=self.emergency_kill_switch, kwargs={"reason": reason},
                    name="risk-emergency-kill", daemon=True,
                ).start()
            if self.on_breach is not None:
                threading.Thread(target=self._notify_breach,
                                 args=(self.on_breach, reason),
                                 name="risk-breach-callback", daemon=True).start()

    def _notify_breach(self, callback: Callable[[str], None], reason: str) -> None:
        try:
            callback(reason)
        except Exception:
            self.logger.exception("Risk breach callback failed")

    def _emergency_fill_policy(self, symbol: str) -> int:
        """Prefer IOC, then a broker-supported immediate fallback for flattening."""
        info = self.mt5.symbol_info(symbol)
        if info is not None:
            mode = int(info.filling_mode)
            if mode & int(self.mt5.SYMBOL_FILLING_IOC):
                return self.mt5.ORDER_FILLING_IOC
            if mode & int(self.mt5.SYMBOL_FILLING_FOK):
                return self.mt5.ORDER_FILLING_FOK
        # RETURN may be unavailable for Market Execution; attempted as the last
        # fallback because flattening is the emergency objective.
        return self.mt5.ORDER_FILLING_RETURN

    def _append_result(self, report: dict[str, Any], key: str, ticket: int,
                       result: Any) -> None:
        if result is None:
            report["errors"].append(
                f"{key} ticket {ticket}: order_send returned None ({self.mt5.last_error()})"
            )
            return
        item = {"ticket": ticket, "retcode": int(result.retcode),
                "comment": str(getattr(result, "comment", "")),
                "volume": float(getattr(result, "volume", 0.0))}
        report[key].append(item)
        done = getattr(self.mt5, "TRADE_RETCODE_DONE", 10009)
        placed = getattr(self.mt5, "TRADE_RETCODE_PLACED", 10008)
        partial = getattr(self.mt5, "TRADE_RETCODE_DONE_PARTIAL", 10010)
        if int(result.retcode) not in (done, placed, partial):
            report["errors"].append(
                f"{key} ticket {ticket} rejected: {result.retcode} {item['comment']}"
            )
