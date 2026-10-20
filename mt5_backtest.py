"""Tick-level Backtrader simulator for the platform's MA-cross strategy.

Input is historical tick data (CSV or Parquet) with a UTC timestamp column and
either bid/ask columns or a price column. Simulated entries use artificial
latency, fixed pip slippage, configurable per-unit and per-order commissions,
ATR sizing, and ATR stop exits. The script also runs expanding walk-forward
validation and random-parameter Monte Carlo trials.

Example:
    python mt5_backtest.py ticks.parquet --symbol EURUSD --pip-size 0.0001 \
      --tick-size 0.00001 --tick-value-loss 1.0 --fee-per-lot-side 3.5

Commission figures are venue/account-specific inputs. This is a simulator,
not a guarantee of live fill or risk behavior.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import random
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import backtrader as bt
import numpy as np
import pandas as pd


@dataclass(frozen=True)
class BacktestConfig:
    symbol: str
    pip_size: float
    tick_size: float
    tick_value_loss: float
    initial_cash: float = 100_000.0
    risk_fraction: float = 0.01
    atr_period: int = 14
    atr_multiple: float = 1.0
    timeframe_minutes: int = 15
    latency_ms: int = 100
    slippage_pips: float = 0.5
    fee_per_lot_side: float = 0.0
    fixed_fee_per_order: float = 0.0
    volume_min: float = 0.01
    volume_max: float = 100.0
    volume_step: float = 0.01
    fast_period: int = 10
    slow_period: int = 30
    bars_per_year: int = 252 * 24 * 4


class TickData(bt.feeds.PandasData):
    """Backtrader feed where every row represents one market tick."""
    params = (
        ("datetime", None), ("open", "open"), ("high", "high"),
        ("low", "low"), ("close", "close"), ("volume", "volume"),
        ("openinterest", None),
    )


class VenueCommission(bt.CommInfoBase):
    """Per-lot-side plus fixed-per-order commission in account currency."""
    params = (
        ("per_lot_side", 0.0),
        ("fixed_per_order", 0.0),
        ("stocklike", True),
        ("commtype", bt.CommInfoBase.COMM_FIXED),
    )

    def _getcommission(self, size: float, price: float, pseudoexec: bool) -> float:
        if not size:
            return 0.0
        return abs(size) * self.p.per_lot_side + self.p.fixed_per_order


class TickMACross(bt.Strategy):
    params = (
        ("config", None),
        ("fast_period", 10),
        ("slow_period", 30),
    )

    def __init__(self) -> None:
        self.cfg: BacktestConfig = self.p.config
        self.closes: list[float] = []
        self.ranges: list[tuple[float, float, float]] = []
        self.candle: Optional[dict[str, Any]] = None
        self.pending_signal: Optional[tuple[pd.Timestamp, str, float]] = None
        self.order: Any = None
        self.stop_order: Any = None
        self.pending_stop: Optional[tuple[str, float]] = None
        self.pending_entry_side: Optional[str] = None
        self.closed_trades = 0
        self.equity: list[float] = []
        self.equity_samples: list[float] = []
        self.closed_trade_pnls: list[float] = []
        self._last_side: Optional[str] = None

    def next(self) -> None:
        dt = self.data.datetime.datetime(0).replace(tzinfo=None)
        stamp = pd.Timestamp(dt, tz="UTC")
        price = float(self.data.close[0])
        self.equity.append(float(self.broker.getvalue()))
        bucket = stamp.floor(f"{self.cfg.timeframe_minutes}min")

        if self.candle is None:
            self.candle = {"time": bucket, "open": price, "high": price,
                           "low": price, "close": price}
        elif bucket == self.candle["time"]:
            self.candle["high"] = max(self.candle["high"], price)
            self.candle["low"] = min(self.candle["low"], price)
            self.candle["close"] = price
        elif bucket > self.candle["time"]:
            completed = self.candle
            self.candle = {"time": bucket, "open": price, "high": price,
                           "low": price, "close": price}
            self._on_candle_close(completed)
            self._dispatch_delayed(stamp, price)
            self.equity_samples.append(float(self.broker.getvalue()))
            return

        self._dispatch_delayed(stamp, price)

    def _on_candle_close(self, candle: dict[str, Any]) -> None:
        close = float(candle["close"])
        self.closes.append(close)
        self.ranges.append((float(candle["high"]), float(candle["low"]), close))
        need = max(self.p.slow_period + 1, self.cfg.atr_period + 1)
        if len(self.closes) < need or self.order is not None:
            return

        fast_now = float(np.mean(self.closes[-self.p.fast_period:]))
        slow_now = float(np.mean(self.closes[-self.p.slow_period:]))
        fast_prev = float(np.mean(self.closes[-self.p.fast_period - 1:-1]))
        slow_prev = float(np.mean(self.closes[-self.p.slow_period - 1:-1]))
        side = None
        if fast_prev <= slow_prev and fast_now > slow_now:
            side = "buy"
        elif fast_prev >= slow_prev and fast_now < slow_now:
            side = "sell"
        if side is None or side == self._last_side:
            return
        atr = self._atr()
        if atr <= 0 or not math.isfinite(atr):
            return
        self._last_side = side
        execute_after = candle["time"] + pd.Timedelta(minutes=self.cfg.timeframe_minutes)
        execute_after += pd.Timedelta(milliseconds=self.cfg.latency_ms)
        self.pending_signal = (execute_after, side, atr)

        # Reverse signals close the current exposure first. A new entry is
        # considered on a later signal after the close has completed.
        if self.position:
            if self.stop_order is not None:
                self.cancel(self.stop_order)
                self.stop_order = None
            self.order = self.close()
            self.pending_signal = None

    def _dispatch_delayed(self, stamp: pd.Timestamp, price: float) -> None:
        if self.pending_signal is None or self.order is not None:
            return
        execute_at, side, atr = self.pending_signal
        if stamp < execute_at:
            return
        self.pending_signal = None
        stop_distance = atr * self.cfg.atr_multiple
        loss_per_lot = (stop_distance / self.cfg.tick_size) * self.cfg.tick_value_loss
        if loss_per_lot <= 0 or not math.isfinite(loss_per_lot):
            return
        risk_cash = max(0.0, float(self.broker.getvalue()) * self.cfg.risk_fraction)
        raw_size = risk_cash / loss_per_lot
        size = self._quantize_size(raw_size)
        if size <= 0:
            return
        self.pending_entry_side = side
        if side == "buy":
            self.pending_stop = (side, price - stop_distance)
            self.order = self.buy(size=size)
        else:
            self.pending_stop = (side, price + stop_distance)
            self.order = self.sell(size=size)

    def notify_order(self, order: Any) -> None:
        if order.status in (order.Submitted, order.Accepted, order.Partial):
            return
        if order.status == order.Completed:
            if self.pending_stop is not None and order.isbuy() == (self.pending_stop[0] == "buy"):
                side, stop_price = self.pending_stop
                if side == "buy":
                    self.stop_order = self.sell(
                        size=abs(order.executed.size), exectype=bt.Order.Stop,
                        price=stop_price,
                    )
                else:
                    self.stop_order = self.buy(
                        size=abs(order.executed.size), exectype=bt.Order.Stop,
                        price=stop_price,
                    )
                self.pending_stop = None
            elif self.stop_order is order:
                self.stop_order = None
        elif order.status in (order.Canceled, order.Margin, order.Rejected, order.Expired):
            self.pending_stop = None
        if order is self.order:
            self.order = None

    def notify_trade(self, trade: Any) -> None:
        if trade.isclosed:
            self.closed_trades += 1
            self.closed_trade_pnls.append(float(trade.pnlcomm))

    def stop(self) -> None:
        final_value = float(self.broker.getvalue())
        if not self.equity_samples or self.equity_samples[-1] != final_value:
            self.equity_samples.append(final_value)

    def _atr(self) -> float:
        period = self.cfg.atr_period
        sample = self.ranges[-(period + 1):]
        true_ranges = []
        for i in range(1, len(sample)):
            high, low, _ = sample[i]
            previous_close = sample[i - 1][2]
            true_ranges.append(max(high - low, abs(high - previous_close),
                                   abs(low - previous_close)))
        return float(np.mean(true_ranges[-period:])) if len(true_ranges) >= period else 0.0

    def _quantize_size(self, raw: float) -> float:
        cfg = self.cfg
        if raw < cfg.volume_min:
            return 0.0  # Do not round up beyond the configured risk budget.
        capped = min(raw, cfg.volume_max)
        steps = math.floor((capped - cfg.volume_min + cfg.volume_step * 1e-10)
                           / cfg.volume_step)
        volume = cfg.volume_min + steps * cfg.volume_step
        return round(volume, max(0, len(f"{cfg.volume_step:.12f}".rstrip("0").partition(".")[2])))


def load_ticks(path: str | Path, *, timestamp_col: str = "time") -> pd.DataFrame:
    """Load and normalize historical tick CSV/Parquet into Backtrader rows."""
    path = Path(path)
    if path.suffix.lower() in {".parquet", ".pq"}:
        raw = pd.read_parquet(path)
    else:
        raw = pd.read_csv(path)
    if timestamp_col not in raw.columns:
        raise ValueError(f"Missing timestamp column {timestamp_col!r}")
    raw[timestamp_col] = _parse_tick_times(raw[timestamp_col], timestamp_col)
    raw = raw.dropna(subset=[timestamp_col]).sort_values(timestamp_col)
    if {"bid", "ask"}.issubset(raw.columns):
        raw["close"] = (pd.to_numeric(raw["bid"], errors="coerce")
                        + pd.to_numeric(raw["ask"], errors="coerce")) / 2.0
    elif "price" in raw.columns:
        raw["close"] = pd.to_numeric(raw["price"], errors="coerce")
    elif "close" in raw.columns:
        raw["close"] = pd.to_numeric(raw["close"], errors="coerce")
    else:
        raise ValueError("Ticks must contain bid/ask, price, or close")
    raw = raw.dropna(subset=["close"])
    if raw.empty:
        raise ValueError("No usable tick rows found")
    # A tick becomes a flat OHLC bar; bid/ask spread is not implicitly folded
    # into these prices, so model it separately if required by the venue.
    result = pd.DataFrame(index=pd.DatetimeIndex(raw[timestamp_col], name="datetime"))
    result["open"] = raw["close"].to_numpy()
    result["high"] = raw["close"].to_numpy()
    result["low"] = raw["close"].to_numpy()
    result["close"] = raw["close"].to_numpy()
    result["volume"] = pd.to_numeric(raw.get("volume", 0), errors="coerce").fillna(0).to_numpy() \
        if isinstance(raw.get("volume", 0), pd.Series) else 0.0
    # Backtrader expects a naive datetime index; timestamps remain UTC values.
    result.index = result.index.tz_convert("UTC").tz_localize(None)
    return result


def run_backtest(data: pd.DataFrame, config: BacktestConfig) -> dict[str, Any]:
    """Run one Backtrader simulation and return core risk/performance metrics."""
    if len(data) < max(config.slow_period, config.atr_period) * 4:
        raise ValueError("Not enough tick data for requested lookbacks")
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.adddata(TickData(dataname=data))
    cerebro.addstrategy(TickMACross, config=config,
                        fast_period=config.fast_period, slow_period=config.slow_period)
    cerebro.broker.setcash(config.initial_cash)
    cerebro.broker.set_slippage_fixed(
        fixed=config.slippage_pips * config.pip_size,
        slip_open=True, slip_limit=True, slip_match=True, slip_out=False,
    )
    cerebro.broker.addcommissioninfo(VenueCommission(
        per_lot_side=config.fee_per_lot_side,
        fixed_per_order=config.fixed_fee_per_order,
    ))
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")
    results = cerebro.run()
    strat = results[0]
    equity = np.asarray(strat.equity, dtype=float)
    if len(equity):
        peaks = np.maximum.accumulate(equity)
        drawdowns = np.divide(peaks - equity, peaks, out=np.zeros_like(equity), where=peaks != 0)
        max_dd = float(np.max(drawdowns))
        final_equity = float(equity[-1])
    else:
        max_dd, final_equity = 0.0, config.initial_cash
    trade_pnls = np.asarray(strat.closed_trade_pnls, dtype=float)
    gains = float(trade_pnls[trade_pnls > 0].sum()) if len(trade_pnls) else 0.0
    losses = float(-trade_pnls[trade_pnls < 0].sum()) if len(trade_pnls) else 0.0
    profit_factor = gains / losses if losses > 0 else (float("inf") if gains > 0 else 0.0)
    sampled = np.asarray(strat.equity_samples, dtype=float)
    if len(sampled) > 1:
        returns = np.diff(sampled) / sampled[:-1]
        std = float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
        sharpe = (float(np.mean(returns)) / std * math.sqrt(config.bars_per_year)
                  if std > 0 else 0.0)
    else:
        sharpe = 0.0
    return {
        "initial_cash": config.initial_cash,
        "final_equity": final_equity,
        "total_return": final_equity / config.initial_cash - 1.0,
        "max_drawdown": max_dd,
        "profit_factor": profit_factor,
        "sharpe_ratio": sharpe,
        "closed_trades": int(strat.closed_trades),
        "fast_period": config.fast_period,
        "slow_period": config.slow_period,
        "latency_ms": config.latency_ms,
        "slippage_pips": config.slippage_pips,
        "commission_per_lot_side": config.fee_per_lot_side,
    }


def walk_forward(
    data: pd.DataFrame,
    base: BacktestConfig,
    fast_values: Iterable[int],
    slow_values: Iterable[int],
    *,
    folds: int = 5,
    train_fraction: float = 0.7,
) -> list[dict[str, Any]]:
    """Expanding-window WFA: optimize on each train window, score next OOS."""
    if folds < 1 or not 0.5 <= train_fraction < 1:
        raise ValueError("folds must be >=1 and train_fraction in [0.5, 1)")
    candidates = [(f, s) for f, s in itertools.product(fast_values, slow_values) if f < s]
    if not candidates:
        raise ValueError("No valid fast/slow parameter combinations")
    n = len(data)
    first_train_end = int(n * train_fraction)
    remaining = n - first_train_end
    results: list[dict[str, Any]] = []
    for fold in range(folds):
        train_end = first_train_end + int(remaining * fold / folds)
        oos_end = first_train_end + int(remaining * (fold + 1) / folds)
        if train_end < 2 or oos_end <= train_end:
            continue
        train = data.iloc[:train_end]
        oos = data.iloc[train_end:oos_end]
        best: Optional[dict[str, Any]] = None
        for fast, slow in candidates:
            candidate_cfg = _with_params(base, fast, slow)
            try:
                score = run_backtest(train, candidate_cfg)
            except ValueError:
                continue
            # Penalize drawdown in selection score rather than selecting on raw return.
            objective = score["total_return"] - score["max_drawdown"]
            if best is None or objective > best["objective"]:
                best = {"fast_period": fast, "slow_period": slow,
                        "train_return": score["total_return"],
                        "train_max_drawdown": score["max_drawdown"],
                        "objective": objective}
        if best is None:
            continue
        oos_result = run_backtest(oos, _with_params(base, best["fast_period"], best["slow_period"]))
        results.append({
            "fold": fold + 1,
            "train_start": str(train.index[0]), "train_end": str(train.index[-1]),
            "oos_start": str(oos.index[0]), "oos_end": str(oos.index[-1]),
            **best,
            "oos_return": oos_result["total_return"],
            "oos_max_drawdown": oos_result["max_drawdown"],
            "oos_closed_trades": oos_result["closed_trades"],
            "decay": best["train_return"] - oos_result["total_return"],
        })
    return results


def monte_carlo_parameters(
    data: pd.DataFrame,
    base: BacktestConfig,
    fast_values: Iterable[int],
    slow_values: Iterable[int],
    *,
    simulations: int = 200,
    seed: int = 7,
) -> dict[str, Any]:
    """Randomly shuffle/sample valid MA pairs and summarize return/DD tails."""
    candidates = [(f, s) for f, s in itertools.product(fast_values, slow_values) if f < s]
    if not candidates:
        raise ValueError("No valid fast/slow parameter combinations")
    if simulations < 1:
        raise ValueError("simulations must be positive")
    rng = random.Random(seed)
    shuffled = candidates[:]
    rng.shuffle(shuffled)
    trials: list[dict[str, Any]] = []
    for i in range(simulations):
        if i and i % len(shuffled) == 0:
            rng.shuffle(shuffled)
        fast, slow = shuffled[i % len(shuffled)]
        outcome = run_backtest(data, _with_params(base, fast, slow))
        trials.append({"simulation": i + 1, **outcome})
    returns = np.asarray([t["total_return"] for t in trials])
    drawdowns = np.asarray([t["max_drawdown"] for t in trials])
    return {
        "simulations": simulations,
        "seed": seed,
        "return_mean": float(np.mean(returns)),
        "return_p05": float(np.quantile(returns, 0.05)),
        "return_median": float(np.median(returns)),
        "return_p95": float(np.quantile(returns, 0.95)),
        "max_drawdown_mean": float(np.mean(drawdowns)),
        "max_drawdown_p95": float(np.quantile(drawdowns, 0.95)),
        "max_drawdown_worst": float(np.max(drawdowns)),
        "trials": trials,
    }


def _with_params(base: BacktestConfig, fast: int, slow: int) -> BacktestConfig:
    values = asdict(base)
    values.update(fast_period=int(fast), slow_period=int(slow))
    return BacktestConfig(**values)


def _parse_tick_times(values: pd.Series, column_name: str) -> pd.Series:
    """Parse ISO timestamps or MT5 epoch seconds/milliseconds as UTC."""
    if pd.api.types.is_numeric_dtype(values.dtype):
        non_null = pd.to_numeric(values, errors="coerce").dropna()
        if non_null.empty:
            return pd.to_datetime(values, utc=True, errors="coerce")
        unit = "ms" if "msc" in column_name.lower() or float(non_null.median()) > 1e11 else "s"
        return pd.to_datetime(values, unit=unit, utc=True, errors="coerce")
    return pd.to_datetime(values, utc=True, errors="coerce")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ticks", help="Historical tick CSV or Parquet file")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--timestamp-col", default="time")
    parser.add_argument("--pip-size", type=float, required=True)
    parser.add_argument("--tick-size", type=float, required=True)
    parser.add_argument("--tick-value-loss", type=float, required=True,
                        help="Account-currency loss value per tick for one lot")
    parser.add_argument("--initial-cash", type=float, default=100_000.0)
    parser.add_argument("--risk-fraction", type=float, default=0.01)
    parser.add_argument("--latency-ms", type=int, default=100)
    parser.add_argument("--slippage-pips", type=float, default=0.5)
    parser.add_argument("--fee-per-lot-side", type=float, default=0.0)
    parser.add_argument("--fixed-fee-per-order", type=float, default=0.0)
    parser.add_argument("--volume-min", type=float, default=0.01)
    parser.add_argument("--volume-max", type=float, default=100.0)
    parser.add_argument("--volume-step", type=float, default=0.01)
    parser.add_argument("--fast-values", default="5,10,15")
    parser.add_argument("--slow-values", default="20,30,50")
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--train-fraction", type=float, default=0.7)
    parser.add_argument("--mc-simulations", type=int, default=200)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--output", default="backtest_results")
    args = parser.parse_args()

    data = load_ticks(args.ticks, timestamp_col=args.timestamp_col)
    fast_values = [int(v) for v in args.fast_values.split(",") if v.strip()]
    slow_values = [int(v) for v in args.slow_values.split(",") if v.strip()]
    base = BacktestConfig(
        symbol=args.symbol, pip_size=args.pip_size, tick_size=args.tick_size,
        tick_value_loss=args.tick_value_loss, initial_cash=args.initial_cash,
        risk_fraction=args.risk_fraction, latency_ms=args.latency_ms,
        slippage_pips=args.slippage_pips, fee_per_lot_side=args.fee_per_lot_side,
        fixed_fee_per_order=args.fixed_fee_per_order, volume_min=args.volume_min,
        volume_max=args.volume_max, volume_step=args.volume_step,
    )
    wfa = walk_forward(data, base, fast_values, slow_values,
                       folds=args.folds, train_fraction=args.train_fraction)
    mc = monte_carlo_parameters(data, base, fast_values, slow_values,
                                simulations=args.mc_simulations, seed=args.seed)
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(wfa).to_csv(out / "walk_forward.csv", index=False)
    pd.DataFrame(mc["trials"]).to_csv(out / "monte_carlo_trials.csv", index=False)
    summary = {"config": asdict(base), "rows": len(data), "walk_forward": wfa,
               "monte_carlo_summary": {k: v for k, v in mc.items() if k != "trials"}}
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
