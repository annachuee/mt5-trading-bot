"""Windows production entry point for the live MT5 strategy.

Secrets are loaded from a local, untracked .env file. This process refuses to
start unless live trading is explicitly enabled and (by default) the active
strategy config contains a holdout-qualified optimization record.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from dotenv import load_dotenv

from mt5_market_data import MT5MarketData
from mt5_oms import MT5OMS
from mt5_pretrade_risk import MT5PreTradeRisk
from mt5_strategy_engine import MT5StrategyEngine


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "strategy_params.json"


class DryRunOMS:
    """OMS-compatible sink that records intent without sending any order."""

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    def market(self, strategy_id: str, symbol: str, side: str, volume: float,
               **kwargs: Any) -> Any:
        self.logger.info(
            "DRY RUN: suppressed %s %s %s volume=%.2f client_order_id=%s",
            strategy_id, side, symbol, volume, kwargs.get("client_order_id", ""),
        )
        return SimpleNamespace(status="dry_run", ticket=None, requested_volume=volume)


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required .env setting {name} is missing")
    return value


def _validate_config(require_optimized: bool) -> dict[str, Any]:
    try:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(f"Strategy config not found: {CONFIG_PATH}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid strategy config JSON at line {exc.lineno}") from exc
    if not isinstance(config, dict):
        raise RuntimeError("Strategy config must be a JSON object")
    fast, slow = config.get("fast_period"), config.get("slow_period")
    if not isinstance(fast, int) or not isinstance(slow, int) or not 0 < fast < slow:
        raise RuntimeError("Strategy config must have integer periods satisfying 0 < fast_period < slow_period")
    if require_optimized:
        opt = config.get("optimization")
        if not isinstance(opt, dict):
            raise RuntimeError(
                "Active config has no holdout-qualified optimization metadata. "
                "Run and review a qualifying optimizer result before production; "
                "ALLOW_UNOPTIMIZED_LIVE=true is an explicit override."
            )
        try:
            qualified = (
                float(opt["holdout_return"]) > 0
                and float(opt["holdout_profit_factor"]) > 1.0
                and float(opt["holdout_sharpe_ratio"]) > 0
                and int(opt["holdout_closed_trades"]) >= 5
            )
        except (KeyError, TypeError, ValueError):
            qualified = False
        if not qualified:
            raise RuntimeError("Active config optimization metadata does not pass holdout qualification")
    return config


def main() -> int:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        raise RuntimeError(f"Missing {env_path}; copy .env.example and set production credentials")
    # python-dotenv keeps secrets out of source code and does not override
    # process-level environment variables provided by a service manager.
    load_dotenv(env_path, override=False)
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger = logging.getLogger("mt5.production")

    trading_enabled_text = _required_env("TRADING_ENABLED").lower()
    if trading_enabled_text not in {"true", "1", "yes", "false", "0", "no"}:
        raise RuntimeError("TRADING_ENABLED must be true or false")
    trading_enabled = trading_enabled_text in {"true", "1", "yes"}
    require_optimized = os.getenv("REQUIRE_OPTIMIZED_CONFIG", "false").strip().lower() not in {
        "false", "0", "no"
    }
    if trading_enabled and not require_optimized and os.getenv(
        "ALLOW_UNOPTIMIZED_LIVE", "false"
    ).strip().lower() not in {"true", "1", "yes"}:
        raise RuntimeError(
            "Live trading requires REQUIRE_OPTIMIZED_CONFIG=true. "
            "ALLOW_UNOPTIMIZED_LIVE=true is an explicit override."
        )
    config = _validate_config(require_optimized)
    symbol = str(config["symbol"])
    daily_loss_pct = float(_required_env("RISK_MAX_DAILY_LOSS_PCT"))
    if not 0 < daily_loss_pct < 1:
        raise RuntimeError("RISK_MAX_DAILY_LOSS_PCT must be a fraction between 0 and 1")
    spread_text = os.getenv("RISK_MAX_SPREAD_POINTS", "").strip()
    max_spread_points = float(spread_text) if spread_text else None
    if max_spread_points is not None and max_spread_points <= 0:
        raise RuntimeError("RISK_MAX_SPREAD_POINTS must be positive")

    terminal_path = _required_env("MT5_TERMINAL_PATH")
    login_text = _required_env("MT5_LOGIN")
    try:
        login = int(login_text)
    except ValueError as exc:
        raise RuntimeError("MT5_LOGIN must be a numeric account login") from exc
    password = _required_env("MT5_PASSWORD")
    server = _required_env("MT5_SERVER")

    feed = MT5MarketData(
        symbol,
        terminal_path=terminal_path,
        login=login,
        password=password,
        server=server,
        poll_interval=float(os.getenv("MT5_POLL_INTERVAL_SECONDS", "0.05")),
        reconnect_delay=float(os.getenv("MT5_RECONNECT_DELAY_SECONDS", "2")),
    )
    risk: MT5PreTradeRisk | None = None
    engine: MT5StrategyEngine | None = None
    stopped = threading.Event()

    def request_stop(_signum: int, _frame: Any) -> None:
        logger.info("Shutdown requested")
        stopped.set()

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)

    try:
        feed.connect()
        mt5 = feed._mt5
        terminal = mt5.terminal_info()
        account = mt5.account_info()
        if terminal is None or not terminal.connected or account is None:
            raise RuntimeError("MT5 terminal or account did not report a connected state")
        require_demo = os.getenv("REQUIRE_DEMO_ACCOUNT", "true").strip().lower() not in {
            "false", "0", "no"
        }
        if require_demo:
            demo_mode = getattr(mt5, "ACCOUNT_TRADE_MODE_DEMO", None)
            if demo_mode is None or int(getattr(account, "trade_mode", -1)) != int(demo_mode):
                raise RuntimeError(
                    "Connected MT5 account is not identified as a DEMO account; "
                    "the production runner requires demo mode by default"
                )
        if mt5.symbol_info(symbol) is None:
            raise RuntimeError(f"Symbol {symbol} is unavailable in the connected MT5 account")

        oms: Any = MT5OMS(mt5, strict_ioc=True) if trading_enabled else DryRunOMS(
            logging.getLogger("mt5.dry_run_oms")
        )
        risk = MT5PreTradeRisk(
            mt5,
            [symbol],
            max_daily_loss_pct=daily_loss_pct,
            max_spread_points=max_spread_points,
            auto_kill_switch=True,
            logger=logging.getLogger("mt5.risk"),
        )
        engine = MT5StrategyEngine.from_config(
            mt5, feed, oms, risk, CONFIG_PATH,
            logger=logging.getLogger("mt5.strategy"),
        )
        logger.info(
            "Starting %s engine: strategy=%s symbol=%s fast=%d slow=%d risk_fraction=%.4f",
            "live" if trading_enabled else "dry-run",
            engine.strategy_id, engine.symbol, engine.fast_period,
            engine.slow_period, engine.risk_fraction,
        )
        engine.start(start_feed=True, start_risk_monitor=True)
        while not stopped.wait(1.0):
            if not feed.connected:
                logger.warning("Market data feed is reconnecting; risk gate will fail closed")
    finally:
        if engine is not None:
            engine.stop()
        if risk is not None:
            risk.stop()
        feed.stop()
        logger.info("Production engine stopped")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(levelname)s: %(message)s")
        logging.getLogger("mt5.production").error("Startup/runtime failure: %s", exc)
        raise SystemExit(1)
