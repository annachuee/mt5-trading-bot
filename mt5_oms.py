"""Strategy-isolated order management for an initialized MT5 connection.

The caller owns the MT5 connection lifecycle and passes the imported
``MetaTrader5`` module (or compatible test double) to :class:`MT5OMS`.

MT5 only permits IOC/FOK for immediate deals when the symbol supports them.
Its documented policy for pending orders is RETURN, so strict IOC cannot be
guaranteed for resting limit/stop orders. This module therefore refuses those
requests by default instead of silently weakening the requested policy.
"""

from __future__ import annotations

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
        self.orders: dict[tuple[str, str], OrderState] = {}
        self._ticket_owner: dict[int, tuple[str, str]] = {}

    def market(
        self, strategy_id: str, symbol: str, side: str, volume: float, *,
        magic: int, client_order_id: Optional[str] = None,
        deviation: int = 10, sl: float = 0.0, tp: float = 0.0,
        comment: str = "",
    ) -> OrderState:
        """Send an IOC market deal. ``deviation`` caps price deviation in points."""
        side = self._side(side)
        self._validate_volume(volume)
        self._require_ioc(symbol)
        info = self.mt5.symbol_info(symbol)
        tick = self.mt5.symbol_info_tick(symbol)
        if info is None or tick is None:
            raise OMSException(f"No symbol metadata/tick available for {symbol}")
        key, state = self._reserve(
            strategy_id, client_order_id, symbol, side, "market", volume, magic
        )
        order_type = self.mt5.ORDER_TYPE_BUY if side == "buy" else self.mt5.ORDER_TYPE_SELL
        price = tick.ask if side == "buy" else tick.bid
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
