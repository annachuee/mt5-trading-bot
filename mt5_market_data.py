"""Concurrent MT5 Level 1 tick and Level 2 market-depth collector.

Requires a running/accessible MetaTrader 5 terminal and the MetaTrader5,
pandas, and numpy packages. MT5's Python API is polled; it does not expose a
Python callback for every tick or book change, so very short-lived updates
may be missed between polls.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import pandas as pd


class MT5MarketData:
    """Collect Level 1 ticks and Level 2 book snapshots into a DataFrame.

    Credentials should be supplied from a secret manager or environment
    variables by the caller. They are never logged by this class. If omitted,
    ``initialize`` uses the terminal's currently logged-in account.

    DataFrame rows have ``event_type`` equal to ``tick`` or ``book``. Book
    snapshots produce one row per level, with ``book_snapshot_id`` grouping
    levels observed in the same poll. Call :meth:`dataframe` for a copy.
    """

    def __init__(
        self,
        symbol: str,
        *,
        terminal_path: Optional[str] = None,
        login: Optional[int] = None,
        password: Optional[str] = None,
        server: Optional[str] = None,
        # A 150 ms interval reduces repeated IPC calls on older Windows VPS
        # terminals while still keeping the dashboard responsive. Callers may
        # tune this through MT5_POLL_INTERVAL_SECONDS.
        poll_interval: float = 0.15,
        reconnect_delay: float = 2.0,
        max_reconnect_delay: float = 30.0,
        tick_batch_size: int = 1000,
        max_rows: Optional[int] = 100_000,
        logger: Optional[logging.Logger] = None,
        on_event: Optional[Callable[[dict[str, Any]], None]] = None,
    ) -> None:
        if not symbol:
            raise ValueError("symbol must be non-empty")
        if poll_interval <= 0 or reconnect_delay <= 0 or max_reconnect_delay <= 0:
            raise ValueError("poll and reconnect intervals must be positive")
        if tick_batch_size <= 0:
            raise ValueError("tick_batch_size must be positive")
        if max_rows is not None and max_rows <= 0:
            raise ValueError("max_rows must be positive or None")

        self.symbol = symbol
        self.terminal_path = terminal_path
        self.login = login
        self.password = password
        self.server = server
        self.poll_interval = poll_interval
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.tick_batch_size = tick_batch_size
        self.max_rows = max_rows
        self.logger = logger or logging.getLogger(__name__)
        self.on_event = on_event

        self._mt5: Any = None
        self._rows: deque[dict[str, Any]] = deque(maxlen=max_rows)
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._connected = False
        self._book_subscribed = False
        self._tick_cursor: Optional[datetime] = None
        self._seen_ticks: set[tuple[Any, ...]] = set()
        self._seen_order: deque[tuple[Any, ...]] = deque(maxlen=20_000)

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self) -> None:
        """Initialize MT5 and subscribe to the symbol's market depth."""
        if self._mt5 is None:
            try:
                import MetaTrader5 as mt5
            except ImportError as exc:
                raise RuntimeError("Install the MetaTrader5 package first") from exc
            self._mt5 = mt5

        kwargs: dict[str, Any] = {}
        if self.login is not None:
            kwargs["login"] = int(self.login)
        if self.password is not None:
            kwargs["password"] = self.password
        if self.server is not None:
            kwargs["server"] = self.server
        if self.terminal_path:
            ok = self._mt5.initialize(self.terminal_path, **kwargs)
        else:
            ok = self._mt5.initialize(**kwargs)
        if not ok:
            error = self._mt5.last_error()
            self._mt5.shutdown()
            raise ConnectionError(f"MT5 initialize failed: {error}")

        if not self._mt5.symbol_select(self.symbol, True):
            error = self._mt5.last_error()
            self._mt5.shutdown()
            raise ConnectionError(f"Could not select {self.symbol}: {error}")

        if not self._mt5.market_book_add(self.symbol):
            error = self._mt5.last_error()
            self._mt5.shutdown()
            raise ConnectionError(
                f"Market Depth subscription failed for {self.symbol}: {error}"
            )

        self._book_subscribed = True
        self._connected = True
        # Start slightly in the past so ticks arriving during connection setup
        # are picked up. MT5 timestamps are UTC.
        self._tick_cursor = datetime.now(timezone.utc) - timedelta(seconds=1)
        self.logger.info("Connected to MT5 for %s", self.symbol)

    def disconnect(self) -> None:
        """Release book subscription and close the MT5 Python connection."""
        if self._mt5 is not None:
            if self._book_subscribed:
                try:
                    self._mt5.market_book_release(self.symbol)
                except Exception:
                    self.logger.exception("Failed to release Market Depth")
            self._book_subscribed = False
            try:
                self._mt5.shutdown()
            finally:
                self._connected = False

    def start(self, *, daemon: bool = True) -> None:
        """Start the background collector; returns immediately."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"mt5-feed-{self.symbol}", daemon=daemon
        )
        self._thread.start()

    def stop(self, *, timeout: Optional[float] = 10.0) -> None:
        """Stop collection and wait for the worker to exit."""
        self._stop_event.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                raise TimeoutError("MT5 collector did not stop before timeout")
        else:
            self.disconnect()

    def dataframe(self) -> pd.DataFrame:
        """Return a thread-safe snapshot of the collected event log."""
        with self._lock:
            return pd.DataFrame(list(self._rows))

    def _record(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._rows.append(row)
        if self.on_event is not None:
            try:
                self.on_event(row.copy())
            except Exception:
                self.logger.exception("on_event callback failed")

    def _run(self) -> None:
        delay = self.reconnect_delay
        while not self._stop_event.is_set():
            if not self._connected:
                try:
                    self.connect()
                    delay = self.reconnect_delay
                except Exception as exc:
                    self.logger.error("MT5 connection failed: %s", exc)
                    self.disconnect()
                    self._stop_event.wait(delay)
                    delay = min(delay * 2, self.max_reconnect_delay)
                    continue
            try:
                self._collect_once()
                delay = self.reconnect_delay
            except Exception as exc:
                error = self._mt5.last_error() if self._mt5 is not None else None
                self.logger.warning("MT5 feed error (%s; %s); reconnecting", exc, error)
                self.disconnect()
                self._stop_event.wait(delay)
                delay = min(delay * 2, self.max_reconnect_delay)
                continue
            self._stop_event.wait(self.poll_interval)
        self.disconnect()

    def _collect_once(self) -> None:
        self._collect_ticks()
        self._collect_book()

    def _collect_ticks(self) -> None:
        mt5 = self._mt5
        assert self._tick_cursor is not None
        ticks = mt5.copy_ticks_from(
            self.symbol, self._tick_cursor, self.tick_batch_size, mt5.COPY_TICKS_ALL
        )
        if ticks is None:
            raise RuntimeError("copy_ticks_from returned None")
        if len(ticks) == 0:
            return

        max_msc = 0
        for tick in ticks:
            item = {key: self._native(value) for key, value in zip(ticks.dtype.names, tick)}
            time_msc = int(item.get("time_msc") or int(item.get("time", 0)) * 1000)
            max_msc = max(max_msc, time_msc)
            # Overlap the cursor at the same millisecond; content-based
            # deduplication prevents repeated rows while retaining distinct ticks.
            fingerprint = tuple(item.get(k) for k in ticks.dtype.names)
            if fingerprint in self._seen_ticks:
                continue
            if len(self._seen_order) == self._seen_order.maxlen:
                expired = self._seen_order[0]
                self._seen_ticks.discard(expired)
            self._seen_order.append(fingerprint)
            self._seen_ticks.add(fingerprint)
            ts = datetime.fromtimestamp(time_msc / 1000, tz=timezone.utc)
            self._record({
                "event_type": "tick", "symbol": self.symbol,
                "timestamp": ts, "time_msc": time_msc,
                **{k: v for k, v in item.items() if k not in {"time", "time_msc"}},
            })

        if max_msc:
            self._tick_cursor = datetime.fromtimestamp(max_msc / 1000, tz=timezone.utc)

    def _collect_book(self) -> None:
        levels = self._mt5.market_book_get(self.symbol)
        if levels is None:
            raise RuntimeError("market_book_get returned None")
        observed_at = datetime.now(timezone.utc)
        snapshot_id = observed_at.timestamp()
        for level_no, level in enumerate(levels):
            values = level._asdict() if hasattr(level, "_asdict") else dict(level)
            self._record({
                "event_type": "book", "symbol": self.symbol,
                "timestamp": observed_at, "book_snapshot_id": snapshot_id,
                "level": level_no,
                **{key: self._native(value) for key, value in values.items()},
            })

    @staticmethod
    def _native(value: Any) -> Any:
        """Convert numpy scalar values to ordinary Python values."""
        return value.item() if hasattr(value, "item") else value

    def __enter__(self) -> "MT5MarketData":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()


if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO)
    feed = MT5MarketData(
        symbol=os.getenv("MT5_SYMBOL", "EURUSD"),
        terminal_path=os.getenv("MT5_TERMINAL_PATH"),
        login=int(os.environ["MT5_LOGIN"]) if os.getenv("MT5_LOGIN") else None,
        password=os.getenv("MT5_PASSWORD"),
        server=os.getenv("MT5_SERVER"),
    )
    try:
        feed.start()
        while True:
            time.sleep(1)
            print(feed.dataframe().tail())
    except KeyboardInterrupt:
        pass
    finally:
        feed.stop()
