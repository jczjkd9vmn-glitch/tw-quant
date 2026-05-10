"""Trading cost and slippage helpers for paper trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TradingCostConfig:
    commission_rate: float = 0.0
    commission_discount: float = 1.0
    min_commission: float = 0.0
    sell_tax_rate_stock: float = 0.0
    sell_tax_rate_etf: float = 0.0
    slippage_rate: float = 0.0

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> "TradingCostConfig":
        data = mapping or {}
        return cls(
            commission_rate=float(data.get("commission_rate", 0.0)),
            commission_discount=float(data.get("commission_discount", 1.0)),
            min_commission=float(data.get("min_commission", 0.0)),
            sell_tax_rate_stock=float(data.get("sell_tax_rate_stock", 0.0)),
            sell_tax_rate_etf=float(data.get("sell_tax_rate_etf", 0.0)),
            slippage_rate=float(data.get("slippage_rate", 0.0)),
        )


def calculate_entry(
    raw_price: float,
    shares: int,
    config: TradingCostConfig,
) -> dict[str, float]:
    entry_slippage = round(float(raw_price) * config.slippage_rate, 4)
    entry_price = round(float(raw_price) + entry_slippage, 4)
    position_value = round(entry_price * int(shares), 2)
    entry_commission = calculate_commission(position_value, config)
    return {
        "entry_price": entry_price,
        "entry_slippage": entry_slippage,
        "position_value": position_value,
        "entry_commission": entry_commission,
    }


def calculate_exit(
    raw_price: float,
    shares: float,
    stock_id: str,
    config: TradingCostConfig,
) -> dict[str, float]:
    exit_slippage = round(float(raw_price) * config.slippage_rate, 4)
    exit_price = round(max(float(raw_price) - exit_slippage, 0.0), 4)
    proceeds = round(exit_price * float(shares), 2)
    exit_commission = calculate_commission(proceeds, config)
    exit_tax = round(proceeds * _sell_tax_rate(stock_id, config), 2)
    return {
        "exit_price": exit_price,
        "exit_slippage": exit_slippage,
        "exit_proceeds": proceeds,
        "exit_commission": exit_commission,
        "exit_tax": exit_tax,
    }


def calculate_commission(amount: float, config: TradingCostConfig) -> float:
    amount = float(amount)
    if amount <= 0:
        return 0.0
    commission = amount * config.commission_rate * config.commission_discount
    if config.min_commission > 0:
        commission = max(commission, config.min_commission)
    return round(commission, 2)


def total_cost(
    *,
    entry_slippage: float,
    entry_commission: float,
    exit_slippage: float,
    exit_commission: float,
    exit_tax: float,
    shares: float,
) -> float:
    slippage_cost = (float(entry_slippage) + float(exit_slippage)) * float(shares)
    return round(slippage_cost + float(entry_commission) + float(exit_commission) + float(exit_tax), 2)


def _sell_tax_rate(stock_id: str, config: TradingCostConfig) -> float:
    return config.sell_tax_rate_etf if str(stock_id).strip().startswith("00") else config.sell_tax_rate_stock
