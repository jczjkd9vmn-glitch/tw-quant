"""Trading simulation layer.

No class in this module sends real broker orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from tw_quant.risk.controls import RiskDecision, RiskManager


@dataclass(frozen=True)
class SimulatedOrder:
    created_at: datetime
    trade_date: pd.Timestamp
    symbol: str
    side: str
    quantity: float
    price: float
    reason: str
    risk_reasons: str
    status: str = "SIMULATED"


class SimulatedBroker:
    """Accepts only simulated orders that already passed risk checks."""

    def __init__(self, risk_manager: RiskManager | None = None):
        self.risk_manager = risk_manager or RiskManager()
        self.orders: list[SimulatedOrder] = []

    def place_order(
        self,
        signal: pd.Series | dict,
        quantity: float,
        price: float,
        risk_decision: RiskDecision,
        side: str = "BUY",
    ) -> SimulatedOrder:
        if not risk_decision.allowed:
            raise ValueError("risk decision rejected this simulated order")
        if not signal.get("buy_reasons"):
            raise ValueError("every simulated order must include a reason")
        if quantity <= 0 or price <= 0:
            raise ValueError("quantity and price must be positive")

        order = SimulatedOrder(
            created_at=datetime.now(timezone.utc).replace(tzinfo=None),
            trade_date=pd.to_datetime(signal["trade_date"]),
            symbol=str(signal["symbol"]),
            side=side,
            quantity=float(quantity),
            price=float(price),
            reason=str(signal["buy_reasons"]),
            risk_reasons="；".join(risk_decision.reasons),
        )
        self.orders.append(order)
        return order


class RealBroker:
    """Placeholder that intentionally refuses real orders in v1."""

    def place_order(self, *_args, **_kwargs):
        raise RuntimeError("第一版禁止真實下單；請使用 SimulatedBroker")
