"""Configuration loading with a small YAML subset parser.

The project intentionally avoids adding a YAML dependency in v1. The parser
supports the simple nested key/value structure used by config.yaml.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "database": {"url": "sqlite:///data/tw_quant.sqlite"},
    "data": {
        "market": "TSE",
        "request_timeout_seconds": 15,
        "stop_on_data_anomaly": True,
        "twse_url": "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
    },
    "strategy": {
        "minimum_total_score": 70,
        "min_history_days": 40,
        "max_candidates": 20,
        "weights": {
            "trend": 0.30,
            "momentum": 0.25,
            "fundamental": 0.15,
            "chip": 0.15,
            "risk": 0.15,
        },
    },
    "risk": {
        "initial_equity": 1_000_000,
        "max_position_pct": 0.10,
        "max_portfolio_exposure_pct": 0.60,
        "max_trade_risk_pct": 0.01,
        "default_stop_loss_pct": 0.08,
        "min_liquidity_value": 5_000_000,
        "max_volatility_20": 0.08,
    },
    "trading_cost": {
        "commission_rate": 0.000399,
        "min_commission": 1,
        "sell_tax_rate_stock": 0.003,
        "sell_tax_rate_etf": 0.001,
        "sell_tax_rate_bond_etf": 0.0,
        "slippage_rate": 0.001,
    },
    "exit_strategy": {
        "take_profit_1_pct": 0.08,
        "take_profit_1_sell_pct": 0.50,
        "take_profit_2_pct": 0.15,
        "take_profit_2_sell_pct": 0.50,
        "trailing_stop_activate_pct": 0.08,
        "trailing_stop_drawdown_pct": 0.08,
        "ma_exit_window": 20,
        "max_holding_days": 30,
        "min_profit_for_holding": 0.03,
    },
    "multi_factor": {
        "enabled": True,
        "affect_ranking": False,
        "affect_risk_pass": False,
        "block_on_high_risk_event": True,
        "missing_data_score": 0,
    },
    "market_intel": {
        "enabled": True,
        "provider": "mock",
        "cache_enabled": True,
        "affect_ranking": False,
        "affect_trading": False,
        "enable_market_intel_filter": False,
    },
    "backtest": {
        "initial_cash": 1_000_000,
        "top_n": 10,
        "transaction_cost_bps": 14.25,
        "max_holding_days": 20,
    },
}


def load_config(path: str | Path = "config.yaml") -> dict[str, Any]:
    """Load config.yaml and merge it into defaults."""

    config = deepcopy(DEFAULT_CONFIG)
    config_path = Path(path)
    if not config_path.exists():
        return config

    parsed = _parse_simple_yaml(config_path.read_text(encoding="utf-8"))
    _deep_update(config, parsed)
    return config


def _deep_update(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)
        else:
            target[key] = value


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if "\t" in line:
            raise ValueError(f"config.yaml line {line_number}: tabs are not supported")

        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if ":" not in stripped:
            raise ValueError(f"config.yaml line {line_number}: expected key: value")

        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if not key:
            raise ValueError(f"config.yaml line {line_number}: empty key")

        while stack and indent <= stack[-1][0]:
            stack.pop()
        if not stack:
            raise ValueError(f"config.yaml line {line_number}: invalid indentation")

        parent = stack[-1][1]
        if raw_value == "":
            node: dict[str, Any] = {}
            parent[key] = node
            stack.append((indent, node))
        else:
            parent[key] = _parse_scalar(raw_value)

    return root


def _parse_scalar(value: str) -> Any:
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]

    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None

    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value
