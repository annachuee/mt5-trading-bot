"""ATR-based, fixed-risk position sizing for an initialized MT5 connection."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Optional


class PositionSizingError(RuntimeError):
    """Raised when a safe broker-valid volume cannot be calculated."""


class RiskBelowMinimumError(PositionSizingError):
    """Broker minimum volume would exceed the configured loss budget."""


@dataclass(frozen=True)
class PositionSize:
    symbol: str
    side: str
    volume: float
    risk_budget: float
    estimated_loss: float
    equity: float
    risk_fraction: float
    atr: float
    atr_period: int
    atr_multiple: float
    entry_price: float
    stop_price: float
    stop_distance: float
    minimum_stop_points: float
    volume_min: float
    volume_max: float
    volume_step: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def calculate_atr_position_size(
    mt5: Any,
    symbol: str,
    side: str,
    *,
    risk_fraction: float = 0.01,
    atr_period: int = 14,
    atr_multiple: float = 1.0,
    minimum_stop_points: float = 150.0,
    equity: Optional[float] = None,
) -> PositionSize:
    """Calculate a broker-valid lot size from completed M15 candles.

    The stop is ``atr_multiple * ATR`` from the current ask (buy) or bid
    (sell). Risk is estimated in the account deposit currency via
    ``mt5.order_calc_profit`` for a one-lot loss from entry to stop. Volume is
    rounded down to the broker's volume step, capped at its maximum, and never
    rounded up to the minimum if that would exceed the risk budget.

    ``mt5`` must be the already initialized MetaTrader5 module. This function
    calculates a size and stop level only; it does not submit an order.
    """
    direction = side.strip().lower()
    if direction not in {"buy", "sell"}:
        raise ValueError("side must be 'buy' or 'sell'")
    if not 0 < risk_fraction < 1:
        raise ValueError("risk_fraction must be in (0, 1)")
    if atr_period < 1 or not math.isfinite(atr_multiple) or atr_multiple <= 0:
        raise ValueError("atr_period and atr_multiple must be positive and finite")
    if not math.isfinite(minimum_stop_points) or minimum_stop_points < 0:
        raise ValueError("minimum_stop_points must be finite and non-negative")

    symbol_info = mt5.symbol_info(symbol)
    if symbol_info is None:
        raise PositionSizingError(f"Unknown symbol: {symbol}")
    if not getattr(symbol_info, "visible", True) and not mt5.symbol_select(symbol, True):
        raise PositionSizingError(f"Could not select {symbol}: {mt5.last_error()}")

    if equity is None:
        account = mt5.account_info()
        if account is None:
            raise PositionSizingError(f"account_info failed: {mt5.last_error()}")
        equity = float(account.equity)
    if not math.isfinite(equity) or equity <= 0:
        raise ValueError("equity must be a finite positive number")
    risk_budget = float(equity) * risk_fraction

    # Start at bar index 1 to exclude the still-forming current M15 candle.
    # Need period completed bars plus the preceding close for True Range.
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M15, 1, atr_period + 1)
    if rates is None or len(rates) < atr_period + 1:
        raise PositionSizingError(
            f"Insufficient M15 history for {symbol}; need {atr_period + 1} completed bars"
        )
    atr = _wilder_atr(rates, atr_period)
    if not math.isfinite(atr) or atr <= 0:
        raise PositionSizingError(f"Invalid ATR for {symbol}: {atr}")

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        raise PositionSizingError(f"No current tick for {symbol}: {mt5.last_error()}")
    entry_price = float(tick.ask if direction == "buy" else tick.bid)
    point = float(getattr(symbol_info, "point", 0.0) or 0.0)
    if not math.isfinite(point) or point <= 0:
        raise PositionSizingError(f"Invalid point size for {symbol}")
    atr_stop_distance = atr * atr_multiple
    minimum_stop_distance = minimum_stop_points * point
    stop_distance = max(atr_stop_distance, minimum_stop_distance)
    raw_stop = entry_price - stop_distance if direction == "buy" else entry_price + stop_distance
    digits = int(getattr(symbol_info, "digits", 8))
    stop_price = round(raw_stop, digits)
    stop_distance = abs(entry_price - stop_price)
    # Price precision can round the initial level inward. Extend it by broker
    # points until the configured minimum is preserved after normalization.
    while stop_distance + point * 1e-9 < minimum_stop_distance:
        stop_distance += point
        raw_stop = entry_price - stop_distance if direction == "buy" else entry_price + stop_distance
        stop_price = round(raw_stop, digits)
        stop_distance = abs(entry_price - stop_price)
    if entry_price <= 0 or stop_price <= 0:
        raise PositionSizingError("Calculated entry/stop price is not positive")

    order_type = mt5.ORDER_TYPE_BUY if direction == "buy" else mt5.ORDER_TYPE_SELL
    one_lot_pnl = mt5.order_calc_profit(order_type, symbol, 1.0, entry_price, stop_price)
    if one_lot_pnl is None:
        raise PositionSizingError(f"order_calc_profit failed: {mt5.last_error()}")
    loss_per_lot = abs(float(one_lot_pnl))
    if not math.isfinite(loss_per_lot) or loss_per_lot <= 0:
        raise PositionSizingError(f"Could not estimate stop loss for {symbol}")

    # Dynamic fixed-risk sizing: one-lot stop loss is obtained from the broker
    # in the account's deposit currency (including symbol contract and FX
    # conversion), so this is equivalent to
    # (equity * risk_fraction) / (stop points * value per point per lot).
    # No constant lot-size fallback is permitted if the broker calculation is
    # unavailable or invalid; the guards above raise instead.
    raw_volume = (float(equity) * risk_fraction) / loss_per_lot
    volume_min = float(symbol_info.volume_min)
    volume_max = float(symbol_info.volume_max)
    volume_step = float(symbol_info.volume_step)
    if not all(math.isfinite(v) and v > 0 for v in (volume_min, volume_max, volume_step)):
        raise PositionSizingError(f"Invalid broker volume limits for {symbol}")
    if volume_max < volume_min:
        raise PositionSizingError(f"Broker max volume is below min volume for {symbol}")

    # Round down from the broker minimum in step increments so sizing never
    # exceeds budget. This also handles brokers whose minimum is not a step
    # multiple measured from zero.
    capped = min(raw_volume, volume_max)
    if capped < volume_min:
        steps = -1
        volume = 0.0
    else:
        steps = math.floor((capped - volume_min + volume_step * 1e-10) / volume_step)
        volume = volume_min + steps * volume_step
    volume = round(volume, _step_precision(volume_step))

    if volume < volume_min - volume_step * 1e-9:
        min_lot_loss = loss_per_lot * volume_min
        raise RiskBelowMinimumError(
            f"Risk budget {risk_budget:.2f} is below minimum-lot stop risk "
            f"{min_lot_loss:.2f} for {symbol}; no valid size stays within budget"
        )
    if volume < volume_min or volume > volume_max:
        raise PositionSizingError(
            f"No broker-valid volume inside [{volume_min}, {volume_max}] at step {volume_step}"
        )

    estimated_loss = loss_per_lot * volume
    # Guard against floating point/rounding surprises.
    if estimated_loss > risk_budget * (1 + 1e-10):
        raise PositionSizingError("Quantized volume exceeds the configured risk budget")

    return PositionSize(
        symbol=symbol,
        side=direction,
        volume=volume,
        risk_budget=risk_budget,
        estimated_loss=estimated_loss,
        equity=float(equity),
        risk_fraction=risk_fraction,
        atr=atr,
        atr_period=atr_period,
        atr_multiple=atr_multiple,
        entry_price=entry_price,
        stop_price=stop_price,
        stop_distance=stop_distance,
        minimum_stop_points=float(minimum_stop_points),
        volume_min=volume_min,
        volume_max=volume_max,
        volume_step=volume_step,
    )


def _wilder_atr(rates: Any, period: int) -> float:
    """Compute Wilder ATR using exactly the latest ``period`` true ranges."""
    names = rates.dtype.names or ()
    required = {"high", "low", "close"}
    if not required.issubset(names):
        raise PositionSizingError("M15 rates lack high/low/close fields")
    trs: list[float] = []
    start = len(rates) - period
    for idx in range(start, len(rates)):
        high = float(rates[idx]["high"])
        low = float(rates[idx]["low"])
        previous_close = float(rates[idx - 1]["close"])
        tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
        if not math.isfinite(tr) or tr < 0:
            raise PositionSizingError("Invalid price data in M15 history")
        trs.append(tr)
    # A simple average of the latest N TR values is the standard initial
    # Wilder ATR seed for this finite lookback.
    return sum(trs) / period


def _step_precision(step: float) -> int:
    text = f"{step:.12f}".rstrip("0")
    return len(text.partition(".")[2])
