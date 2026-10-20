"""Run the real strategy/sizer/risk/OMS path against an in-memory MT5 double.

This is an offline integration smoke run. It never imports or connects to MT5.
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np

from mt5_oms import MT5OMS
from mt5_pretrade_risk import MT5PreTradeRisk
from mt5_strategy_engine import MT5StrategyEngine


class MockMT5:
    TIMEFRAME_M15 = 15
    ORDER_TYPE_BUY, ORDER_TYPE_SELL = 0, 1
    TRADE_ACTION_DEAL = 1
    ORDER_TIME_GTC = 0
    ORDER_FILLING_IOC = 1
    SYMBOL_FILLING_IOC = 2
    TRADE_RETCODE_DONE = 10009

    def __init__(self) -> None:
        self._tick = SimpleNamespace(bid=1.10000, ask=1.10010)
        self._ticket = 700001

    def terminal_info(self):
        return SimpleNamespace(connected=True)

    def account_info(self):
        return SimpleNamespace(equity=10000.0, balance=10000.0)

    def symbol_info(self, symbol):
        return SimpleNamespace(visible=True, point=0.00001, digits=5,
                               volume_min=0.01, volume_max=100.0,
                               volume_step=0.01, filling_mode=self.SYMBOL_FILLING_IOC)

    def symbol_select(self, symbol, enable):
        return True

    def symbol_info_tick(self, symbol):
        return self._tick

    def copy_rates_from_pos(self, symbol, timeframe, start, count):
        dtype = [("high", "f8"), ("low", "f8"), ("close", "f8")]
        rows = np.zeros(count, dtype=dtype)
        for i in range(count):
            close = 1.1000 + i * 0.00001
            rows[i] = (close + 0.0005, close - 0.0005, close)
        return rows

    def order_calc_profit(self, order_type, symbol, volume, price_open, price_close):
        return (price_close - price_open) * 100000 * volume

    def order_send(self, request):
        self._ticket += 1
        price = request.get("price", 1.1001)
        logging.getLogger("mock.mt5").info(
            "Mock order_send accepted action=%s type_filling=%s",
            request.get("action"), request.get("type_filling"),
        )
        return SimpleNamespace(retcode=self.TRADE_RETCODE_DONE, order=self._ticket,
                               deal=self._ticket + 100000, volume=request["volume"],
                               price=price, comment="mock fill")

    def orders_get(self, **kwargs):
        return ()

    def positions_get(self, **kwargs):
        return ()

    def last_error(self):
        return (0, "mock")


class MockFeed:
    """Sends one simulated tick per M15 interval to trigger a deterministic SMA cross."""

    def __init__(self, symbol: str, logger: logging.Logger) -> None:
        self.symbol = symbol
        self.logger = logger
        self.on_event = None

    def start(self) -> None:
        self.logger.info("Mock feed started for %s; emitting deterministic M15 ticks", self.symbol)
        start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        # Downtrend followed by a strong reversal creates a single upward cross.
        prices = [1.1100 - i * 0.0002 for i in range(35)]
        prices += [prices[-1] + (i + 1) * 0.0015 for i in range(12)]
        for i, price in enumerate(prices):
            ts = start + timedelta(minutes=15 * i)
            if self.on_event:
                self.on_event({"symbol": self.symbol, "event_type": "tick",
                               "timestamp": ts, "bid": price - 0.00005,
                               "ask": price + 0.00005})
        self.logger.info("Mock feed emitted %d ticks", len(prices))

    def stop(self) -> None:
        self.logger.info("Mock feed stopped")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument("--config", default="config/strategy_params.json")
    parser.add_argument("--log", default="backtest_results/mock_engine_smoke.log")
    args = parser.parse_args()

    import pathlib
    log_path = pathlib.Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        handlers=[logging.FileHandler(log_path, mode="w"), logging.StreamHandler()])
    logger = logging.getLogger("mock.engine")
    mt5 = MockMT5()
    feed = MockFeed("EURUSD", logger)
    oms = MT5OMS(mt5)
    risk = MT5PreTradeRisk(mt5, ["EURUSD"], max_daily_loss_pct=0.05,
                           spread_min_samples=1, max_spread_points=20.0,
                           auto_kill_switch=False)
    engine = MT5StrategyEngine.from_config(mt5, feed, oms, risk, args.config, logger=logger)
    logger.info("Starting offline mock engine; no live MT5 connection or broker orders")
    engine.start(start_feed=True, start_risk_monitor=True)
    deadline = time.monotonic() + args.seconds
    try:
        while time.monotonic() < deadline:
            time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    finally:
        engine.stop()
        risk.stop()
        logger.info("Mock engine stopped after %.1f seconds", args.seconds)
    if engine.last_order_state is None or engine.last_order_state.status != "filled":
        logger.error("Lifecycle did not complete: last order state=%r", engine.last_order_state)
        return 1
    logger.info("Lifecycle verified: ATR sizing -> risk gate -> OMS filled ticket=%s",
                engine.last_order_state.ticket)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
