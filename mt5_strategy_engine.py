"""Decoupled event-driven strategy engine for the MT5 platform modules.

Default strategy: moving-average cross on completed candles. Tick events from
``MT5MarketData`` are aggregated into UTC candles; the engine can also accept
completed candles directly through :meth:`on_candle_close`.
"""

from __future__ import annotations

import logging
import json
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
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
        self.deviation = deviation
        self.logger = logger or logging.getLogger(__name__)
        self.on_signal = on_signal

        self._lock = threading.RLock()
        self._closes: deque[float] = deque(maxlen=slow_period + 1)
        self._candle: Optional[_Candle] = None
        self._last_cross: Optional[str] = None
        self._sequence = 0
        self._previous_feed_callback: Optional[Callable[[dict[str, Any]], None]] = None
        self.last_signal: Optional[Signal] = None
        self.last_order_state: Any = None

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
            self._last_cross = side
            signal = Signal(
                strategy_id=self.strategy_id, symbol=self.symbol, side=side,
                timestamp=stamp, close=close, fast_ma=fast, slow_ma=slow,
                reason=f"{self.fast_period}/{self.slow_period} SMA crossover",
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
            )
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
                sl=size.stop_price,
                comment=f"{signal.reason}; risk={self.risk_fraction:.4f}",
            )
            self.last_order_state = result
            self.logger.info(
                "Dispatched %s %s volume=%s stop=%s via OMS",
                signal.side, signal.symbol, size.volume, size.stop_price,
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
