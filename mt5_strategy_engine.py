"""Decoupled event-driven strategy engine for the MT5 platform modules.

Default strategy: ADX- and session-filtered moving-average cross on completed candles. Tick events from
``MT5MarketData`` are aggregated into UTC candles; the engine can also accept
completed candles directly through :meth:`on_candle_close`.
"""

from __future__ import annotations

import logging
import json
import math
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from mt5_position_sizing import calculate_atr_position_size
from mt5_pretrade_risk import RiskBlocked


@dataclass(frozen=True)
class Signal:
    strategy_id: str
    symbol: str
    side: str
    timestamp: datetime
    close: float
    fast_ma: float
    slow_ma: float
    reason: str


@dataclass
class _Candle:
    start: datetime
    open: float
    high: float
    low: float
    close: float

    def update(self, price: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price

    def as_dict(self, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol, "time": self.start, "open": self.open,
            "high": self.high, "low": self.low, "close": self.close,
        }


class MT5StrategyEngine:
    """Connect streamed feed events to signal generation and order dispatch.

    Components are injected so strategy logic remains independent of MT5
    lifecycle and can be run against test doubles or alternate data sources.
    The actual order path is strictly:

        completed candle -> signal -> ATR position sizing -> risk gate -> OMS

    Args:
        mt5_module: Initialized MetaTrader5 module used by the position sizer.
        market_data: Existing ``MT5MarketData`` instance from Step 1.
        oms: Existing ``MT5OMS`` instance from Step 2.
        risk_engine: Existing ``MT5PreTradeRisk`` from Step 3.
        strategy_id/magic: Must uniquely identify this strategy in the OMS.
        timeframe_minutes: Tick aggregation candle period, default M15.
        fast_period/slow_period: Moving-average crossover lengths in candles.
        adx_period/adx_threshold/adx_max_threshold: Wilder ADX band; entries
            require ``adx_threshold < ADX < adx_max_threshold``.
        session_start_utc/session_end_utc: Entry window [start, end) in UTC.
        risk_reward_multiple: TP distance as a multiple of the sized SL distance.
    """

    def __init__(
        self,
        mt5_module: Any,
        market_data: Any,
        oms: Any,
        risk_engine: Any,
        *,
        strategy_id: str,
        magic: int,
        symbol: str,
        timeframe_minutes: int = 15,
        fast_period: int = 10,
        slow_period: int = 30,
        risk_fraction: float = 0.01,
        atr_period: int = 14,
        atr_multiple: float = 1.0,
        minimum_stop_points: float = 150.0,
        adx_period: int = 14,
        adx_threshold: float = 25.0,
        adx_max_threshold: float = 55.0,
        session_start_utc: str = "12:00",
        session_end_utc: str = "17:00",
        risk_reward_multiple: float = 2.0,
        deviation: int = 10,
        logger: Optional[logging.Logger] = None,
        on_signal: Optional[Callable[[Signal], None]] = None,
    ) -> None:
        if not strategy_id:
            raise ValueError("strategy_id is required")
        if magic <= 0:
            raise ValueError("magic must be positive and unique per strategy")
        if not symbol:
            raise ValueError("symbol is required")
        if timeframe_minutes < 1 or 60 % timeframe_minutes:
            raise ValueError("timeframe_minutes must be a positive divisor of 60")
        if fast_period < 1 or slow_period <= fast_period:
            raise ValueError("require 1 <= fast_period < slow_period")
        if adx_period < 1:
            raise ValueError("adx_period must be positive")
        if not math.isfinite(float(adx_threshold)) or adx_threshold < 0:
            raise ValueError("adx_threshold must be finite and non-negative")
        if (not math.isfinite(float(adx_max_threshold))
                or adx_max_threshold <= adx_threshold):
            raise ValueError("adx_max_threshold must be finite and above adx_threshold")
        if not math.isfinite(float(risk_reward_multiple)) or risk_reward_multiple <= 0:
            raise ValueError("risk_reward_multiple must be finite and positive")
        if not math.isfinite(float(minimum_stop_points)) or minimum_stop_points < 0:
            raise ValueError("minimum_stop_points must be finite and non-negative")
        try:
            session_start = datetime_time.fromisoformat(session_start_utc)
            session_end = datetime_time.fromisoformat(session_end_utc)
        except (TypeError, ValueError) as exc:
            raise ValueError("session times must use HH:MM or HH:MM:SS UTC format") from exc
        if session_start.tzinfo is not None or session_end.tzinfo is not None:
            raise ValueError("session times must be timezone-naive UTC clock times")
        if session_start >= session_end:
            raise ValueError("session_start_utc must be earlier than session_end_utc")

        self.mt5 = mt5_module
        self.market_data = market_data
        self.oms = oms
        self.risk_engine = risk_engine
        self.strategy_id = strategy_id
        self.magic = int(magic)
        self.symbol = symbol
        self.timeframe_minutes = timeframe_minutes
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.risk_fraction = risk_fraction
        self.atr_period = atr_period
        self.atr_multiple = atr_multiple
        self.minimum_stop_points = float(minimum_stop_points)
        self.adx_period = int(adx_period)
        self.adx_threshold = float(adx_threshold)
        self.adx_max_threshold = float(adx_max_threshold)
        self.session_start_utc = session_start
        self.session_end_utc = session_end
        self.risk_reward_multiple = float(risk_reward_multiple)
        self.deviation = deviation
        self.logger = logger or logging.getLogger(__name__)
        self.on_signal = on_signal

        self._lock = threading.RLock()
        self._closes: deque[float] = deque(maxlen=slow_period + 1)
        self._adx_bars: deque[tuple[float, float, float]] = deque(
            maxlen=max(2 * self.adx_period, self.slow_period + 1)
        )
        self._candle: Optional[_Candle] = None
        self._last_cross: Optional[str] = None
        self._sequence = 0
        self._previous_feed_callback: Optional[Callable[[dict[str, Any]], None]] = None
        self.last_signal: Optional[Signal] = None
        self.last_order_state: Any = None
        self.last_position_size: Any = None
        self.last_take_profit: Optional[float] = None
        self.last_adx: Optional[float] = None
        self.last_entry_block_reason: Optional[str] = None

    @classmethod
    def from_config(cls, mt5_module: Any, market_data: Any, oms: Any,
                    risk_engine: Any, config_path: str | Path, **overrides: Any
                    ) -> "MT5StrategyEngine":
        """Construct the engine from tracked ``config/strategy_params.json``.

        Explicit keyword overrides take precedence over file values.
        Optimization metadata stored by the grid-search tool is ignored.
        """
        path = Path(config_path)
        try:
            config = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ValueError(f"Strategy config not found: {path}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid strategy config JSON at line {exc.lineno}") from exc
        if not isinstance(config, dict):
            raise ValueError("Strategy config must be a JSON object")
        values = {
            "strategy_id": config.get("strategy_id"),
            "magic": config.get("magic"),
            "symbol": config.get("symbol"),
            "timeframe_minutes": config.get("timeframe_minutes", 15),
            "fast_period": config.get("fast_period", 10),
            "slow_period": config.get("slow_period", 30),
            "risk_fraction": config.get("risk_fraction", 0.01),
            "atr_period": config.get("atr_period", 14),
            "atr_multiple": config.get("atr_multiple", 1.0),
            "minimum_stop_points": config.get("minimum_stop_points", 150.0),
            "adx_period": config.get("adx_period", 14),
            "adx_threshold": config.get("adx_threshold", 25.0),
            "adx_max_threshold": config.get("adx_max_threshold", 55.0),
            "session_start_utc": config.get("session_start_utc", "12:00"),
            "session_end_utc": config.get("session_end_utc", "17:00"),
            "risk_reward_multiple": config.get("risk_reward_multiple", 2.0),
            "deviation": config.get("deviation_points", 10),
        }
        values.update(overrides)
        missing = [name for name in ("strategy_id", "magic", "symbol") if values.get(name) is None]
        if missing:
            raise ValueError(f"Missing required strategy config fields: {', '.join(missing)}")
        return cls(mt5_module, market_data, oms, risk_engine, **values)

    def attach(self) -> None:
        """Route events from the Step 1 feed into this engine.

        Existing feed callback is preserved and called after this engine. Call
        once before starting the feed. If callback management is done elsewhere,
        set the feed's ``on_event`` callback to ``engine.on_market_event``.
        """
        if self._previous_feed_callback is not None:
            return
        previous = getattr(self.market_data, "on_event", None)
        self._previous_feed_callback = previous

        def dispatch(event: dict[str, Any]) -> None:
            self.on_market_event(event)
            if previous is not None:
                previous(event)

        self.market_data.on_event = dispatch

    def detach(self) -> None:
        """Restore the feed callback that was active before :meth:`attach`."""
        if self._previous_feed_callback is None:
            return
        self.market_data.on_event = self._previous_feed_callback
        self._previous_feed_callback = None

    def on_market_event(self, event: dict[str, Any]) -> Optional[Any]:
        """Feed adapter: route tick records and ignore Level 2 book rows."""
        if event.get("symbol") != self.symbol:
            return None
        if event.get("event_type") != "tick":
            return None
        return self.on_tick(event)

    def on_tick(self, tick: dict[str, Any]) -> Optional[Any]:
        """Consume one Level 1 tick and close an aggregate candle as needed."""
        if tick.get("symbol", self.symbol) != self.symbol:
            return None
        bid, ask = tick.get("bid"), tick.get("ask")
        if bid is None or ask is None or float(bid) <= 0 or float(ask) <= 0:
            return None
        manage_positions = getattr(self.oms, "manage_positions", None)
        if callable(manage_positions):
            try:
                manage_positions(
                    self.strategy_id, magic=self.magic, symbol=self.symbol,
                    atr_period=self.atr_period,
                )
            except Exception:
                # A protective-stop management error is visible and retried on
                # the next tick; it must not prevent candle aggregation.
                self.logger.exception("Position protection update failed")
        price = (float(bid) + float(ask)) / 2.0
        stamp = tick.get("timestamp")
        if isinstance(stamp, datetime):
            ts = stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
            ts = ts.astimezone(timezone.utc)
        else:
            time_msc = tick.get("time_msc")
            if time_msc is None:
                return None
            ts = datetime.fromtimestamp(float(time_msc) / 1000, tz=timezone.utc)
        candle_start = self._floor_time(ts)

        with self._lock:
            if self._candle is None:
                self._candle = _Candle(candle_start, price, price, price, price)
                return None
            if candle_start < self._candle.start:
                # Ignore delayed/out-of-order ticks so candles stay monotonic.
                return None
            if candle_start == self._candle.start:
                self._candle.update(price)
                return None
            closed = self._candle.as_dict(self.symbol)
            self._candle = _Candle(candle_start, price, price, price, price)
        return self.on_candle_close(closed)

    def on_candle_close(self, candle: dict[str, Any]) -> Optional[Any]:
        """Evaluate a completed candle, emit a crossover signal, and route it.

        Candle dict fields: ``symbol``, ``time`` (timezone-aware datetime),
        ``open``, ``high``, ``low``, and ``close``. Duplicate or out-of-order
        close timestamps are ignored.
        """
        if candle.get("symbol", self.symbol) != self.symbol:
            return None
        close = float(candle["close"])
        if close <= 0:
            return None
        stamp = candle.get("time", datetime.now(timezone.utc))
        if not isinstance(stamp, datetime):
            raise ValueError("candle time must be a datetime")
        stamp = stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
        stamp = stamp.astimezone(timezone.utc)

        with self._lock:
            if getattr(self, "_last_candle_time", None) is not None and stamp <= self._last_candle_time:
                return None
            self._last_candle_time = stamp
            try:
                high = float(candle["high"])
                low = float(candle["low"])
            except (KeyError, TypeError, ValueError):
                high = low = float("nan")
            if (math.isfinite(high) and math.isfinite(low)
                    and high >= close >= low > 0):
                self._adx_bars.append((high, low, close))
            else:
                # Missing OHLC breaks the contiguous directional-movement
                # history, so require a fresh ADX warm-up before entries.
                self._adx_bars.clear()
            adx = _calculate_adx(list(self._adx_bars), self.adx_period)
            self.last_adx = adx
            previous_closes = list(self._closes)
            self._closes.append(close)
            closes = list(self._closes)
            if len(closes) < self.slow_period:
                return None
            fast = sum(closes[-self.fast_period:]) / self.fast_period
            slow = sum(closes[-self.slow_period:]) / self.slow_period
            if len(previous_closes) < self.slow_period:
                return None
            prior_fast = sum(previous_closes[-self.fast_period:]) / self.fast_period
            prior_slow = sum(previous_closes[-self.slow_period:]) / self.slow_period
            side = None
            if prior_fast <= prior_slow and fast > slow and self._last_cross != "buy":
                side = "buy"
            elif prior_fast >= prior_slow and fast < slow and self._last_cross != "sell":
                side = "sell"
            if side is None:
                return None

            # Filters apply at the crossover candle close. An entry is never
            # deferred until a later session or until ADX subsequently rises.
            # Candle timestamps are candle-open times (as with MT5 rates),
            # while the signal executes on its close. Gate the execution time.
            entry_time = stamp + timedelta(minutes=self.timeframe_minutes)
            seconds_utc = (entry_time.hour * 3600 + entry_time.minute * 60
                           + entry_time.second + entry_time.microsecond / 1_000_000)
            session_start = (self.session_start_utc.hour * 3600
                             + self.session_start_utc.minute * 60
                             + self.session_start_utc.second
                             + self.session_start_utc.microsecond / 1_000_000)
            session_end = (self.session_end_utc.hour * 3600
                           + self.session_end_utc.minute * 60
                           + self.session_end_utc.second
                           + self.session_end_utc.microsecond / 1_000_000)
            if not (session_start <= seconds_utc < session_end):
                self.last_entry_block_reason = "outside_utc_session"
                self.logger.info(
                    "Suppressed %s crossover at %s: outside UTC session %s-%s",
                    side, entry_time.isoformat(), self.session_start_utc.isoformat(),
                    self.session_end_utc.isoformat(),
                )
                return None
            if adx is None or adx <= self.adx_threshold or adx >= self.adx_max_threshold:
                if adx is None or adx <= self.adx_threshold:
                    self.last_entry_block_reason = "adx_below_operational_band"
                    detail = ("unavailable" if adx is None else f"{adx:.2f}")
                else:
                    self.last_entry_block_reason = "adx_above_exhaustion_ceiling"
                    detail = f"{adx:.2f}"
                self.logger.info(
                    "Suppressed %s crossover at %s: ADX %s is outside operational band (%.2f, %.2f)",
                    side, entry_time.isoformat(),
                    detail, self.adx_threshold, self.adx_max_threshold,
                )
                return None

            self.last_entry_block_reason = None
            self._last_cross = side
            signal = Signal(
                strategy_id=self.strategy_id, symbol=self.symbol, side=side,
                timestamp=stamp, close=close, fast_ma=fast, slow_ma=slow,
                reason=(f"{self.fast_period}/{self.slow_period} SMA crossover; "
                        f"ADX({self.adx_period})={adx:.2f} within "
                        f"({self.adx_threshold:.0f}, {self.adx_max_threshold:.0f}); "
                        f"UTC session {self.session_start_utc:%H:%M}-{self.session_end_utc:%H:%M}"),
            )
            self.last_signal = signal

        if self.on_signal is not None:
            try:
                self.on_signal(signal)
            except Exception:
                self.logger.exception("Signal callback failed")
        return self._route_signal(signal)

    def _route_signal(self, signal: Signal) -> Any:
        """Position-size -> risk-gate -> OMS, in that order."""
        self._sequence += 1
        client_order_id = f"{self.strategy_id}-{self._sequence}"
        try:
            size = calculate_atr_position_size(
                self.mt5,
                signal.symbol,
                signal.side,
                risk_fraction=self.risk_fraction,
                atr_period=self.atr_period,
                atr_multiple=self.atr_multiple,
                minimum_stop_points=self.minimum_stop_points,
            )
            self.last_position_size = size
            symbol_info = self.mt5.symbol_info(signal.symbol)
            digits = int(getattr(symbol_info, "digits", 8)) if symbol_info is not None else 8
            target_distance = size.stop_distance * self.risk_reward_multiple
            tp = (size.entry_price + target_distance if signal.side == "buy"
                  else size.entry_price - target_distance)
            tp = round(tp, digits)
            if tp <= 0:
                raise ValueError("Calculated take-profit price is not positive")
            self.last_take_profit = tp
            result = self.risk_engine.submit(
                signal.symbol,
                self.oms.market,
                self.strategy_id,
                signal.symbol,
                signal.side,
                size.volume,
                magic=self.magic,
                client_order_id=client_order_id,
                deviation=self.deviation,
                tp=tp,
                sl=size.stop_price,
                comment=(f"{signal.reason}; risk={self.risk_fraction:.4f}; "
                         f"RR=1:{self.risk_reward_multiple:g}"),
            )
            self.last_order_state = result
            self.logger.info(
                "Dispatched %s %s volume=%s stop=%s target=%s via OMS",
                signal.side, signal.symbol, size.volume, size.stop_price, tp,
            )
            return result
        except RiskBlocked as exc:
            self.logger.warning("Signal blocked by risk gate: %s", exc)
            return None
        except Exception:
            # A failed size/gate/OMS step does not submit a fallback order.
            self.logger.exception("Signal pipeline failed for %s", client_order_id)
            return None

    def _floor_time(self, ts: datetime) -> datetime:
        minute = (ts.minute // self.timeframe_minutes) * self.timeframe_minutes
        return ts.replace(minute=minute, second=0, microsecond=0)

    def start(self, *, start_feed: bool = True, start_risk_monitor: bool = True) -> None:
        """Attach callbacks, start risk monitoring, then start the data feed."""
        self.attach()
        if start_risk_monitor:
            self.risk_engine.start()
        if start_feed:
            self.market_data.start()

    def stop(self, *, stop_feed: bool = False, stop_risk_monitor: bool = False) -> None:
        """Detach callback and optionally stop shared feed/risk components."""
        self.detach()
        if stop_feed:
            self.market_data.stop()
        if stop_risk_monitor:
            self.risk_engine.stop()


def _calculate_adx(bars: list[tuple[float, float, float]], period: int) -> Optional[float]:
    """Return the latest Wilder ADX from contiguous ``(high, low, close)`` bars.

    The initial ADX is the mean of ``period`` DX values, which requires at
    least ``2 * period`` OHLC bars. Subsequent values use Wilder smoothing.
    """
    if period < 1 or len(bars) < 2 * period:
        return None

    true_ranges: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    for previous, current in zip(bars, bars[1:]):
        prev_high, prev_low, prev_close = previous
        high, low, _close = current
        up_move = high - prev_high
        down_move = prev_low - low
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))

    smoothed_tr = sum(true_ranges[:period])
    smoothed_plus = sum(plus_dm[:period])
    smoothed_minus = sum(minus_dm[:period])
    dx_values: list[float] = []
    for index in range(period - 1, len(true_ranges)):
        if index > period - 1:
            smoothed_tr = smoothed_tr - smoothed_tr / period + true_ranges[index]
            smoothed_plus = smoothed_plus - smoothed_plus / period + plus_dm[index]
            smoothed_minus = smoothed_minus - smoothed_minus / period + minus_dm[index]
        if smoothed_tr <= 0:
            dx_values.append(0.0)
            continue
        plus_di = 100.0 * smoothed_plus / smoothed_tr
        minus_di = 100.0 * smoothed_minus / smoothed_tr
        di_sum = plus_di + minus_di
        dx_values.append(100.0 * abs(plus_di - minus_di) / di_sum if di_sum else 0.0)

    if len(dx_values) < period:
        return None
    adx = sum(dx_values[:period]) / period
    for dx in dx_values[period:]:
        adx = (adx * (period - 1) + dx) / period
    return adx
