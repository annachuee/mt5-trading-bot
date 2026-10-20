"""Grid search the Backtrader tick simulator and update live strategy params.

Searches inclusive fast/slow SMA ranges, reports profit factor, maximum
drawdown, and Sharpe ratio for every pair on both train and chronological
holdout data, then writes the best training-ranked parameters only if they pass
holdout qualification to the active
``config/strategy_params.json``. Existing config fields are preserved.

The optimizer refuses to update the active config unless the selected holdout
candidate has positive return, profit factor > 1, positive Sharpe, and the
configured minimum number of closed trades. It writes atomically. No live order
or strategy execution is performed by this module.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from mt5_backtest import BacktestConfig, load_ticks, run_backtest


def grid_search(
    data: pd.DataFrame,
    base: BacktestConfig,
    *,
    fast_min: int = 5,
    fast_max: int = 20,
    fast_step: int = 1,
    slow_min: int = 30,
    slow_max: int = 100,
    slow_step: int = 5,
    train_fraction: float = 0.8,
    minimum_oos_trades: int = 5,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Evaluate every pair; rank on training data and validate on holdout."""
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")
    if min(fast_min, slow_min, fast_step, slow_step) < 1:
        raise ValueError("MA bounds and steps must be positive")
    if fast_max < fast_min or slow_max < slow_min:
        raise ValueError("MA maximum must be >= minimum")
    if minimum_oos_trades < 1:
        raise ValueError("minimum_oos_trades must be positive")

    split = int(len(data) * train_fraction)
    train, holdout = data.iloc[:split], data.iloc[split:]
    if len(train) < max(base.atr_period, slow_min) * 4:
        raise ValueError("Training split is too small for the requested MA lookbacks")
    if len(holdout) < max(base.atr_period, slow_min) * 4:
        raise ValueError("Holdout split is too small for the requested MA lookbacks")

    fast_values = range(fast_min, fast_max + 1, fast_step)
    slow_values = range(slow_min, slow_max + 1, slow_step)
    pairs = [(fast, slow) for fast in fast_values for slow in slow_values if fast < slow]
    if not pairs:
        raise ValueError("No valid fast/slow parameter combinations")

    records: list[dict[str, Any]] = []
    best_train: Optional[dict[str, Any]] = None
    for index, (fast, slow) in enumerate(pairs, 1):
        cfg = _config_with_ma(base, fast, slow)
        train_result = run_backtest(train, cfg)
        holdout_result = run_backtest(holdout, cfg)
        train_score = risk_adjusted_score(train_result)
        holdout_score = risk_adjusted_score(holdout_result)
        record = {
            "fast_period": fast,
            "slow_period": slow,
            "train_return": train_result["total_return"],
            "train_profit_factor": train_result["profit_factor"],
            "train_max_drawdown": train_result["max_drawdown"],
            "train_sharpe_ratio": train_result["sharpe_ratio"],
            "train_closed_trades": train_result["closed_trades"],
            "holdout_return": holdout_result["total_return"],
            "holdout_profit_factor": holdout_result["profit_factor"],
            "holdout_max_drawdown": holdout_result["max_drawdown"],
            "holdout_sharpe_ratio": holdout_result["sharpe_ratio"],
            "holdout_closed_trades": holdout_result["closed_trades"],
            "train_risk_adjusted_score": train_score,
            "holdout_risk_adjusted_score": holdout_score,
            "train_qualified": _qualifies(train_result, minimum_oos_trades),
            "holdout_qualified": _qualifies(holdout_result, minimum_oos_trades),
        }
        records.append(record)
        if record["train_qualified"] and (
            best_train is None or train_score > best_train["train_risk_adjusted_score"]
        ):
            best_train = record
        if index % 25 == 0 or index == len(pairs):
            print(f"Evaluated {index}/{len(pairs)} parameter pairs", flush=True)

    # Select on the training split only. Holdout results are validation and
    # must not determine which pair wins, or the holdout becomes training data.
    selected = best_train if best_train and best_train["holdout_qualified"] else {}
    return pd.DataFrame.from_records(records), selected


def risk_adjusted_score(metrics: dict[str, Any]) -> float:
    """Holdout objective: Sharpe + log(PF) minus a 2x drawdown penalty."""
    pf = float(metrics["profit_factor"])
    # Cap infinite/extreme PF so a tiny denominator cannot dominate selection.
    pf_component = math.log1p(min(pf, 10.0)) if math.isfinite(pf) else math.log1p(10.0)
    sharpe = float(metrics["sharpe_ratio"])
    drawdown = float(metrics["max_drawdown"])
    if not all(map(math.isfinite, (pf_component, sharpe, drawdown))):
        return float("-inf")
    return sharpe + pf_component - 2.0 * drawdown


def _qualifies(metrics: dict[str, Any], minimum_trades: int) -> bool:
    pf = float(metrics["profit_factor"])
    return (
        metrics["closed_trades"] >= minimum_trades
        and metrics["total_return"] > 0
        and pf > 1.0
        and metrics["sharpe_ratio"] > 0
    )


def write_active_config(
    config_path: Path,
    selected: dict[str, Any],
    *,
    symbol: str,
    data_path: Path,
    data_sha256: str,
    train_fraction: float,
) -> dict[str, Any]:
    """Atomically update active config, preserving unrelated strategy fields."""
    if not selected:
        raise ValueError("No qualified parameter pair; active config was not changed")
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Active strategy config not found: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Active strategy config is invalid JSON at line {exc.lineno}") from exc
    if not isinstance(config, dict):
        raise ValueError("Active strategy config must contain a JSON object")
    if config.get("symbol") and str(config["symbol"]).upper() != symbol.upper():
        raise ValueError(
            f"Optimizer symbol {symbol} does not match active config symbol {config['symbol']}"
        )

    config["fast_period"] = int(selected["fast_period"])
    config["slow_period"] = int(selected["slow_period"])
    config["optimization"] = {
        "method": "backtrader_tick_grid_search",
        "selected_at_utc": datetime.now(timezone.utc).isoformat(),
        "train_fraction": train_fraction,
        "data_file": data_path.name,
        "data_sha256": data_sha256,
        "holdout_return": _finite_or_none(selected["holdout_return"]),
        "holdout_profit_factor": _finite_or_none(selected["holdout_profit_factor"]),
        "holdout_max_drawdown": _finite_or_none(selected["holdout_max_drawdown"]),
        "holdout_sharpe_ratio": _finite_or_none(selected["holdout_sharpe_ratio"]),
        "holdout_closed_trades": int(selected["holdout_closed_trades"]),
        "train_risk_adjusted_score": _finite_or_none(selected["train_risk_adjusted_score"]),
    }
    config_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = config_path.with_suffix(config_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n",
                         encoding="utf-8")
    os.replace(temp_path, config_path)
    return config


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _config_with_ma(base: BacktestConfig, fast: int, slow: int) -> BacktestConfig:
    values = base.__dict__.copy()
    values["fast_period"] = fast
    values["slow_period"] = slow
    return BacktestConfig(**values)


def _finite_or_none(value: Any) -> float | None:
    value = float(value)
    return value if math.isfinite(value) else None


def main() -> int:
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ticks", type=Path, help="Historical tick CSV or Parquet file")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--timestamp-col", default="time")
    parser.add_argument("--pip-size", type=float, required=True)
    parser.add_argument("--tick-size", type=float, required=True)
    parser.add_argument("--tick-value-loss", type=float, required=True)
    parser.add_argument("--initial-cash", type=float, default=100_000.0)
    parser.add_argument("--risk-fraction", type=float, default=0.01)
    parser.add_argument("--latency-ms", type=int, default=100)
    parser.add_argument("--slippage-pips", type=float, default=0.5)
    parser.add_argument("--fee-per-lot-side", type=float, default=0.0)
    parser.add_argument("--fixed-fee-per-order", type=float, default=0.0)
    parser.add_argument("--volume-min", type=float, default=0.01)
    parser.add_argument("--volume-max", type=float, default=100.0)
    parser.add_argument("--volume-step", type=float, default=0.01)
    parser.add_argument("--atr-period", type=int, default=14)
    parser.add_argument("--atr-multiple", type=float, default=1.0)
    parser.add_argument("--fast-min", type=int, default=5)
    parser.add_argument("--fast-max", type=int, default=20)
    parser.add_argument("--fast-step", type=int, default=1)
    parser.add_argument("--slow-min", type=int, default=30)
    parser.add_argument("--slow-max", type=int, default=100)
    parser.add_argument("--slow-step", type=int, default=5)
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--minimum-oos-trades", type=int, default=5)
    parser.add_argument("--config", type=Path, default=root / "config" / "strategy_params.json")
    parser.add_argument("--results", type=Path, default=root / "backtest_results" / "grid_search.csv")
    parser.add_argument("--no-write-config", action="store_true",
                        help="Report the winner but leave active config unchanged")
    args = parser.parse_args()

    if not args.ticks.is_file():
        parser.error(f"historical tick file does not exist: {args.ticks}")
    if not 0 < args.risk_fraction < 1:
        parser.error("--risk-fraction must be between 0 and 1")

    data = load_ticks(args.ticks, timestamp_col=args.timestamp_col)
    base = BacktestConfig(
        symbol=args.symbol, pip_size=args.pip_size, tick_size=args.tick_size,
        tick_value_loss=args.tick_value_loss, initial_cash=args.initial_cash,
        risk_fraction=args.risk_fraction, latency_ms=args.latency_ms,
        slippage_pips=args.slippage_pips, fee_per_lot_side=args.fee_per_lot_side,
        fixed_fee_per_order=args.fixed_fee_per_order, volume_min=args.volume_min,
        volume_max=args.volume_max, volume_step=args.volume_step,
        atr_period=args.atr_period, atr_multiple=args.atr_multiple,
    )
    metrics, selected = grid_search(
        data, base,
        fast_min=args.fast_min, fast_max=args.fast_max, fast_step=args.fast_step,
        slow_min=args.slow_min, slow_max=args.slow_max, slow_step=args.slow_step,
        train_fraction=args.train_fraction,
        minimum_oos_trades=args.minimum_oos_trades,
    )
    args.results.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(args.results, index=False)
    print(f"Saved {len(metrics)} parameter results to {args.results}")
    if not selected:
    print("Top training candidate failed holdout qualification; active config was not changed.")
        return 2

    print("Selected holdout parameters:")
    print(json.dumps({k: _finite_or_none(v) if isinstance(v, (float, int)) else v
                      for k, v in selected.items()}, indent=2))
    if not args.no_write_config:
        active = write_active_config(
            args.config,
            selected,
            symbol=args.symbol,
            data_path=args.ticks,
            data_sha256=sha256_file(args.ticks),
            train_fraction=args.train_fraction,
        )
        print(f"Updated active strategy config: {args.config}")
        print(json.dumps(active, indent=2))
    else:
        print("--no-write-config set; active strategy config unchanged.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
