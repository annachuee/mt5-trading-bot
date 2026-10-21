"""Strategy-isolated order management for an initialized MT5 connection.

The caller owns the MT5 connection lifecycle and passes the imported
``MetaTrader5`` module (or compatible test double) to :class:`MT5OMS`.

MT5 only permits IOC/FOK for immediate deals when the symbol supports them.
Its documented policy for pending orders is RETURN, so strict IOC cannot be
guaranteed for resting limit/stop orders. This module therefore refuses those
requests by default instead of silently weakening the requested policy.
"""

from __future__ import annotations

import math
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


class OMSException(RuntimeError):
    """Base class for order management errors."""


class UnsupportedFillingPolicy(OMSException):
    """Raised when MT5 cannot honor the required IOC policy."""


class DuplicateOrder(OMSException):
    """Raised when an idempotency key is already active for a strategy."""


class OrderOwnershipError(OMSException):
    """Raised when an operation targets an order owned by another strategy."""


@dataclass
class OrderState:
    strategy_id: str
    client_order_id: str
    symbol: str
    side: str
    order_kind: str
    requested_volume: float
    magic: int
    ticket: Optional[int] = None
    deal: Optional[int] = None
    status: str = "submitting"
    filled_volume: float = 0.0
    remaining_volume: float = 0.0
    average_price: Optional[float] = None
    retcode: Optional[int] = None
    comment: str = ""
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class PositionProtectionState:
    """Per-position inputs and progress for ATR-based protective stops."""

    strategy_id: str
    ticket: int
    symbol: str
    side: str
    magic: int
    entry_price: float
    initial_atr_risk: float
    floating_pnl: float = 0.0
    favorable_distance: float = 0.0
    break_even_armed: bool = False
    last_stop: float = 0.0
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class MT5OMS:
    """Submit and track orders over an already initialized MT5 connection.

    A strategy-specific ``magic`` number is required for every submission.
    State is keyed by ``(strategy_id, client_order_id)`` and ticket ownership
    is checked before modifications. The in-memory registry is process-local;
    persist/synchronize it externally if it must survive restarts.
    """

    ACTIVE = {"submitting", "accepted", "partial", "pending", "unknown"}

    def __init__(self, mt5_module: Any, *, strict_ioc: bool = True) -> None:
        self.mt5 = mt5_module
        self.strict_ioc = strict_ioc
        self._lock = threading.RLock()
        self._protection_lock = threading.RLock()
        self.orders: dict[tuple[str, str], OrderState] = {}
        self._ticket_owner: dict[int, tuple[str, str]] = {}
        self.positions: dict[int, PositionProtectionState] = {}
        self._atr_cache: dict[tuple[str, int], tuple[int, float]] = {}

    def market(
        self, strategy_id: str, symbol: str, side: str, volume: float, *,
        magic: int, client_order_id: Optional[str] = None,
        deviation: int = 10, sl: float = 0.0, tp: float = 0.0,
        comment: str = "",
    ) -> OrderState:
        """Send a protected IOC market deal.

        Both stop-loss and take-profit boundaries are mandatory. ``deviation``
        caps price deviation in points.
        """
        side = self._side(side)
        self._validate_volume(volume)
        if not all(math.isfinite(float(level)) and float(level) > 0 for level in (sl, tp)):
            raise ValueError("market orders require positive finite SL and TP prices")
        self._require_ioc(symbol)
        info = self.mt5.symbol_info(symbol)
        tick = self.mt5.symbol_info_tick(symbol)
        if info is None or tick is None:
            raise OMSException(f"No symbol metadata/tick available for {symbol}")
        order_type = self.mt5.ORDER_TYPE_BUY if side == "buy" else self.mt5.ORDER_TYPE_SELL
        price = tick.ask if side == "buy" else tick.bid
        if side == "buy" and not (float(sl) < float(price) < float(tp)):
            raise ValueError("buy market protection must satisfy SL < ask < TP")
        if side == "sell" and not (float(tp) < float(price) < float(sl)):
            raise ValueError("sell market protection must satisfy TP < bid < SL")
        key, state = self._reserve(
            strategy_id, client_order_id, symbol, side, "market", volume, magic
        )
        request = {
            "action": self.mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type,
            "price": float(price),
            "deviation": int(deviation),
            "magic": int(magic),
            "comment": self._comment(strategy_id, key[1], comment),
            "type_time": self.mt5.ORDER_TIME_GTC,
            "type_filling": self.mt5.ORDER_FILLING_IOC,
        }
        if sl:
            request["sl"] = float(sl)
        if tp:
            request["tp"] = float(tp)
        return self._send(state, request)

    def limit(
        self, strategy_id: str, symbol: str, side: str, volume: float, price: float, *,
        magic: int, client_order_id: Optional[str] = None,
        expiration: Optional[datetime] = None, comment: str = "",
    ) -> OrderState:
        """Request a resting limit order; strict IOC mode rejects this operation.

        MT5 requires RETURN for pending orders. Set ``strict_ioc=False`` only
        when accepting the broker's pending-order fill policy.
        """
        return self._pending(
            strategy_id, symbol, side, volume, price, "limit", magic=magic,
            client_order_id=client_order_id, expiration=expiration, comment=comment,
        )

    def stop(
        self, strategy_id: str, symbol: str, side: str, volume: float, price: float, *,
        magic: int, client_order_id: Optional[str] = None,
        expiration: Optional[datetime] = None, comment: str = "",
    ) -> OrderState:
        """Request a resting stop order; strict IOC mode rejects this operation."""
        return self._pending(
            strategy_id, symbol, side, volume, price, "stop", magic=magic,
            client_order_id=client_order_id, expiration=expiration, comment=comment,
        )

    def _pending(
        self, strategy_id: str, symbol: str, side: str, volume: float, price: float,
        kind: str, *, magic: int, client_order_id: Optional[str],
        expiration: Optional[datetime], comment: str,
    ) -> OrderState:
        if self.strict_ioc:
            raise UnsupportedFillingPolicy(
                f"Cannot place a resting {kind} order with strict IOC: MT5 requires "
                "ORDER_FILLING_RETURN for pending orders."
            )
        side = self._side(side)
        self._validate_volume(volume)
        if price <= 0:
            raise ValueError("price must be positive")
        key, state = self._reserve(
            strategy_id, client_order_id, symbol, side, kind, volume, magic
        )
        order_type = {
            ("limit", "buy"): self.mt5.ORDER_TYPE_BUY_LIMIT,
            ("limit", "sell"): self.mt5.ORDER_TYPE_SELL_LIMIT,
            ("stop", "buy"): self.mt5.ORDER_TYPE_BUY_STOP,
            ("stop", "sell"): self.mt5.ORDER_TYPE_SELL_STOP,
        }[(kind, side)]
        request = {
            "action": self.mt5.TRADE_ACTION_PENDING,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type,
            "price": float(price),
            "magic": int(magic),
            "comment": self._comment(strategy_id, key[1], comment),
            "type_time": self.mt5.ORDER_TIME_GTC,
            "type_filling": self.mt5.ORDER_FILLING_RETURN,
        }
        if expiration is not None:
            if expiration.tzinfo is None:
                raise ValueError("expiration must be timezone-aware")
            request["type_time"] = self.mt5.ORDER_TIME_SPECIFIED
            request["expiration"] = int(expiration.timestamp())
        return self._send(state, request)

    def modify_pending(
        self, strategy_id: str, ticket: int, *, price: Optional[float] = None,
        sl: Optional[float] = None, tp: Optional[float] = None,
        expiration: Optional[datetime] = None,
    ) -> OrderState:
        """Modify an owned live pending order after confirming it still exists."""
        state = self._owned_state(strategy_id, ticket)
        active = self.mt5.orders_get(ticket=int(ticket))
        if not active:
            state.status = "unknown"
            self._touch(state)
            raise OMSException(f"Pending ticket {ticket} is no longer active")
        order = active[0]
        if int(getattr(order, "magic", -1)) != state.magic:
            raise OrderOwnershipError(f"Ticket {ticket} magic does not match registry")
        request: dict[str, Any] = {
            "action": self.mt5.TRADE_ACTION_MODIFY,
            "order": int(ticket),
            "symbol": state.symbol,
            "price": float(price if price is not None else order.price_open),
            "sl": float(sl if sl is not None else getattr(order, "sl", 0.0)),
            "tp": float(tp if tp is not None else getattr(order, "tp", 0.0)),
            "type_time": self.mt5.ORDER_TIME_GTC,
        }
        if expiration is not None:
            if expiration.tzinfo is None:
                raise ValueError("expiration must be timezone-aware")
            request["type_time"] = self.mt5.ORDER_TIME_SPECIFIED
            request["expiration"] = int(expiration.timestamp())
        return self._send(state, request)

    def reconcile(self, strategy_id: Optional[str] = None) -> dict[tuple[str, str], OrderState]:
        """Refresh active/past order state and accumulate partial deal fills.

        Call periodically from the strategy/event loop. A returned partial
        volume is tracked as filled while the pending remainder stays active.
        """
        with self._lock:
            targets = [
                (key, state) for key, state in self.orders.items()
                if state.status in self.ACTIVE and (strategy_id is None or key[0] == strategy_id)
            ]
        for key, state in targets:
            self._reconcile_one(key, state)
        with self._lock:
            return {key: state for key, state in self.orders.items()
                    if strategy_id is None or key[0] == strategy_id}

    def manage_positions(
        self, strategy_id: str, *, magic: int, symbol: Optional[str] = None,
        atr_period: int = 14, break_even_multiple: float = 1.0,
        break_even_buffer_pips: float = 1.0, trailing_atr_multiple: float = 2.0,
    ) -> dict[int, PositionProtectionState]:
        """Track this strategy's positions and maintain ATR protective stops.

        Initial risk is captured from the position's original SL the first time
        it is observed. At >= ``break_even_multiple`` of that risk in favorable
        movement, the stop is armed at entry plus/minus the configured pip
        buffer. From then on it trails by ``trailing_atr_multiple`` times the
        latest completed M15 ATR, and never loosens. Call on incoming ticks;
        ATR is refreshed once per new M15 bar.
        Positions without an initial SL are skipped because their initial risk
        cannot be recovered safely.
        """
        if not strategy_id or int(magic) <= 0:
            raise ValueError("strategy_id and positive strategy magic are required")
        if (atr_period < 1
                or not math.isfinite(break_even_multiple) or break_even_multiple <= 0
                or not math.isfinite(break_even_buffer_pips) or break_even_buffer_pips < 0
                or not math.isfinite(trailing_atr_multiple) or trailing_atr_multiple <= 0):
            raise ValueError("ATR period and protection multiples must be positive")
        with self._protection_lock:
            return self._manage_positions_locked(
                strategy_id, magic=magic, symbol=symbol, atr_period=atr_period,
                break_even_multiple=break_even_multiple,
                break_even_buffer_pips=break_even_buffer_pips,
                trailing_atr_multiple=trailing_atr_multiple,
            )

    def _manage_positions_locked(
        self, strategy_id: str, *, magic: int, symbol: Optional[str],
        atr_period: int, break_even_multiple: float, break_even_buffer_pips: float,
        trailing_atr_multiple: float,
    ) -> dict[int, PositionProtectionState]:
        positions = self.mt5.positions_get(symbol=symbol) if symbol else self.mt5.positions_get()
        if positions is None:
            raise OMSException(f"positions_get failed: {self.mt5.last_error()}")
        active_tickets: set[int] = set()
        for position in positions:
            if int(getattr(position, "magic", -1)) != int(magic):
                continue
            pos_symbol = str(getattr(position, "symbol", symbol or ""))
            if symbol is not None and pos_symbol != symbol:
                continue
            ticket = int(getattr(position, "ticket", 0))
            entry = float(getattr(position, "price_open", 0.0))
            current_sl = float(getattr(position, "sl", 0.0) or 0.0)
            pos_type = int(getattr(position, "type", -1))
            if ticket <= 0 or entry <= 0:
                continue
            if pos_type == int(self.mt5.POSITION_TYPE_BUY):
                side = "buy"
            elif pos_type == int(self.mt5.POSITION_TYPE_SELL):
                side = "sell"
            else:
                continue
            active_tickets.add(ticket)
            tracked = self.positions.get(ticket)
            floating_pnl = float(getattr(position, "profit", 0.0) or 0.0)
            if tracked is None:
                initial_risk = abs(entry - current_sl)
                if current_sl <= 0 or not math.isfinite(initial_risk) or initial_risk <= 0:
                    continue
                tracked = PositionProtectionState(
                    strategy_id=strategy_id, ticket=ticket, symbol=pos_symbol,
                    side=side, magic=int(magic), entry_price=entry,
                    initial_atr_risk=initial_risk, last_stop=current_sl,
                    floating_pnl=floating_pnl,
                )
                self.positions[ticket] = tracked
            elif (tracked.strategy_id != strategy_id or tracked.magic != int(magic)
                  or tracked.symbol != pos_symbol):
                raise OrderOwnershipError(f"Position ticket {ticket} is already tracked by another owner")
            elif current_sl <= 0:
                current_sl = tracked.last_stop
            tracked.floating_pnl = floating_pnl

            tick = self.mt5.symbol_info_tick(pos_symbol)
            info = self.mt5.symbol_info(pos_symbol)
            if tick is None or info is None:
                continue
            favorable = (float(tick.bid) - entry if side == "buy"
                         else entry - float(tick.ask))
            tracked.favorable_distance = favorable
            # Prices arrive on broker point grids; allow only a tiny floating
            # point tolerance so an exact 1R move is not missed by binary
            # representation noise.
            if (favorable + float(getattr(info, "point", 0.0) or 0.0) * 1e-9
                    < break_even_multiple * tracked.initial_atr_risk):
                continue
            tracked.break_even_armed = True
            atr = self._rolling_m15_atr(pos_symbol, atr_period)
            digits = int(getattr(info, "digits", 8))
            point = float(getattr(info, "point", 0.0) or 0.0)
            if not math.isfinite(point) or point <= 0:
                continue
            pip_size = point * (10.0 if digits in (3, 5) else 1.0)
            be_buffer = break_even_buffer_pips * pip_size
            min_gap = max(
                int(getattr(info, "trade_stops_level", 0) or 0),
                int(getattr(info, "trade_freeze_level", 0) or 0),
            ) * point
            if side == "buy":
                candidate = entry + be_buffer
                if atr is not None and math.isfinite(atr) and atr > 0:
                    candidate = max(candidate, float(tick.bid) - trailing_atr_multiple * atr)
                candidate = max(candidate, current_sl)
                candidate = round(candidate, digits)
                if candidate <= current_sl + float(getattr(info, "point", 0.0)) * 0.5:
                    continue
                if candidate > float(tick.bid) - min_gap:
                    continue
            else:
                candidate = entry - be_buffer
                if atr is not None and math.isfinite(atr) and atr > 0:
                    candidate = min(candidate, float(tick.ask) + trailing_atr_multiple * atr)
                candidate = min(candidate, current_sl)
                candidate = round(candidate, digits)
                if candidate >= current_sl - float(getattr(info, "point", 0.0)) * 0.5:
                    continue
                if candidate < float(tick.ask) + min_gap:
                    continue
            if self._send_position_stop(tracked, candidate, position):
                tracked.last_stop = candidate
                tracked.updated_at = datetime.now(timezone.utc)
        # Forget positions that are no longer open; position tickets are
        # broker identifiers and are not recycled as local strategy IDs.
        for ticket, tracked in list(self.positions.items()):
            if tracked.strategy_id == strategy_id and ticket not in active_tickets:
                self.positions.pop(ticket, None)
        return {ticket: state for ticket, state in self.positions.items()
                if state.strategy_id == strategy_id}

    def _rolling_m15_atr(self, symbol: str, period: int) -> Optional[float]:
        rates = self.mt5.copy_rates_from_pos(symbol, self.mt5.TIMEFRAME_M15, 1, period + 1)
        if rates is None or len(rates) < period + 1:
            return None
        names = rates.dtype.names or ()
        if not {"high", "low", "close"}.issubset(names):
            return None
        latest_bar = int(rates[-1]["time"]) if "time" in names else None
        key = (symbol, period)
        cached = self._atr_cache.get(key)
        if latest_bar is not None and cached is not None and cached[0] == latest_bar:
            return cached[1]
        ranges = []
        for index in range(len(rates) - period, len(rates)):
            high = float(rates[index]["high"])
            low = float(rates[index]["low"])
            previous_close = float(rates[index - 1]["close"])
            tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
            if not all(math.isfinite(value) for value in (high, low, previous_close, tr)) or tr < 0:
                return None
            ranges.append(tr)
        atr = sum(ranges) / period
        if latest_bar is not None:
            self._atr_cache[key] = (latest_bar, atr)
        return atr

    def _send_position_stop(self, tracked: PositionProtectionState, sl: float,
                            position: Any) -> bool:
        request = {
            "action": self.mt5.TRADE_ACTION_SLTP,
            "position": tracked.ticket,
            "symbol": tracked.symbol,
            "sl": float(sl),
            "tp": float(getattr(position, "tp", 0.0) or 0.0),
            "magic": tracked.magic,
        }
        result = self.mt5.order_send(request)
        if result is None:
            raise OMSException(f"SL/TP modification returned None: {self.mt5.last_error()}")
        retcode = int(getattr(result, "retcode", -1))
        if retcode != int(getattr(self.mt5, "TRADE_RETCODE_DONE", 10009)):
            # A rejected stop update leaves the existing broker-side stop in
            # force and is retried on the next tick.
            return False
        return True

    def state(self, strategy_id: str, client_order_id: str) -> Optional[OrderState]:
        with self._lock:
            return self.orders.get((strategy_id, client_order_id))

    def _reconcile_one(self, key: tuple[str, str], state: OrderState) -> None:
        if state.ticket is None:
            return
        active = self.mt5.orders_get(ticket=state.ticket)
        positions = self.mt5.positions_get(symbol=state.symbol)
        deals = self.mt5.history_deals_get(position=state.ticket)
        if active is None or positions is None or deals is None:
            state.status = "unknown"
            self._touch(state)
            return

        # Filter by strategy magic; MT5 position/order ticket relationships can
        # differ by account mode, so never infer ownership from symbol alone.
        owned_deals = [d for d in deals if int(getattr(d, "magic", -1)) == state.magic]
        filled = sum(float(getattr(d, "volume", 0.0)) for d in owned_deals)
        if filled > state.filled_volume:
            state.filled_volume = min(filled, state.requested_volume)
            value = sum(float(getattr(d, "price", 0.0)) * float(getattr(d, "volume", 0.0))
                        for d in owned_deals)
            state.average_price = value / filled if filled else None
        state.remaining_volume = max(0.0, state.requested_volume - state.filled_volume)

        if active:
            order = active[0]
            if int(getattr(order, "magic", -1)) != state.magic:
                state.status = "unknown"
            elif state.filled_volume > 0:
                state.status = "partial"
            else:
                state.status = "pending"
        elif state.filled_volume >= state.requested_volume:
            state.status = "filled"
        elif state.filled_volume > 0:
            # IOC remainder is canceled; retain partial fill outcome.
            state.status = "partial_canceled"
        else:
            state.status = "closed_or_canceled"
        self._touch(state)

    def _require_ioc(self, symbol: str) -> None:
        info = self.mt5.symbol_info(symbol)
        if info is None:
            raise OMSException(f"Unknown symbol: {symbol}")
        # SYMBOL_FILLING_IOC is a bit in SYMBOL_FILLING_MODE.
        if not (int(info.filling_mode) & int(self.mt5.SYMBOL_FILLING_IOC)):
            raise UnsupportedFillingPolicy(f"{symbol} does not advertise IOC support")

    def _reserve(
        self, strategy_id: str, client_order_id: Optional[str], symbol: str,
        side: str, kind: str, volume: float, magic: int,
    ) -> tuple[tuple[str, str], OrderState]:
        if not strategy_id or not symbol:
            raise ValueError("strategy_id and symbol are required")
        if int(magic) <= 0:
            raise ValueError("magic must be a positive strategy-specific integer")
        client_order_id = client_order_id or str(uuid.uuid4())
        key = (strategy_id, client_order_id)
        with self._lock:
            prior = self.orders.get(key)
            if prior is not None and prior.status in self.ACTIVE:
                raise DuplicateOrder(f"Active client_order_id {client_order_id!r} exists")
            state = OrderState(
                strategy_id=strategy_id, client_order_id=client_order_id,
                symbol=symbol, side=side, order_kind=kind,
                requested_volume=float(volume), remaining_volume=float(volume),
                magic=int(magic),
            )
            self.orders[key] = state
        return key, state

    def _send(self, state: OrderState, request: dict[str, Any]) -> OrderState:
        try:
            result = self.mt5.order_send(request)
        except Exception:
            state.status = "unknown"  # request may have reached server
            self._touch(state)
            raise
        if result is None:
            state.status = "unknown"
            state.comment = f"order_send returned None; last_error={self.mt5.last_error()}"
            self._touch(state)
            raise OMSException(state.comment)

        state.retcode = int(result.retcode)
        state.ticket = int(result.order) if getattr(result, "order", 0) else None
        state.deal = int(result.deal) if getattr(result, "deal", 0) else None
        state.comment = str(getattr(result, "comment", ""))
        done = getattr(self.mt5, "TRADE_RETCODE_DONE", 10009)
        partial = getattr(self.mt5, "TRADE_RETCODE_DONE_PARTIAL", 10010)
        placed = getattr(self.mt5, "TRADE_RETCODE_PLACED", 10008)
        if state.retcode == done:
            state.filled_volume = min(float(getattr(result, "volume", state.requested_volume)),
                                      state.requested_volume)
            state.remaining_volume = max(0.0, state.requested_volume - state.filled_volume)
            if state.order_kind == "market":
                # IOC cancels any unfilled remainder after the immediate attempt.
                state.status = "filled" if state.remaining_volume == 0 else "partial_canceled"
            else:
                state.status = "accepted"
            state.average_price = float(result.price) if getattr(result, "price", 0) else None
        elif state.retcode == partial:
            state.status = "partial"
            state.filled_volume = min(float(getattr(result, "volume", 0.0)), state.requested_volume)
            state.remaining_volume = max(0.0, state.requested_volume - state.filled_volume)
            state.average_price = float(result.price) if getattr(result, "price", 0) else None
        elif state.retcode == placed:
            state.status = "pending"
        else:
            state.status = "rejected"
        if state.ticket is not None:
            with self._lock:
                self._ticket_owner[state.ticket] = (state.strategy_id, state.client_order_id)
        self._touch(state)
        if state.status == "rejected":
            raise OMSException(f"MT5 rejected order ({state.retcode}): {state.comment}")
        return state

    def _owned_state(self, strategy_id: str, ticket: int) -> OrderState:
        with self._lock:
            key = self._ticket_owner.get(int(ticket))
            if key is None or key[0] != strategy_id:
                raise OrderOwnershipError(f"Strategy {strategy_id!r} does not own ticket {ticket}")
            return self.orders[key]

    @staticmethod
    def _side(side: str) -> str:
        value = side.strip().lower()
        if value not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        return value

    @staticmethod
    def _validate_volume(volume: float) -> None:
        if volume <= 0:
            raise ValueError("volume must be positive")

    @staticmethod
    def _touch(state: OrderState) -> None:
        state.updated_at = datetime.now(timezone.utc)

    @staticmethod
    def _comment(strategy_id: str, client_order_id: str, comment: str) -> str:
        # MT5 comments have broker/platform limits; keep strategy/id trace short.
        tag = f"{strategy_id[:8]}:{client_order_id[:12]}"
        return (f"{tag} {comment}".strip())[:31]

    def snapshot(self) -> dict[tuple[str, str], dict[str, Any]]:
        """Return a serializable shallow snapshot of the registry."""
        with self._lock:
            return {key: asdict(value) for key, value in self.orders.items()}
