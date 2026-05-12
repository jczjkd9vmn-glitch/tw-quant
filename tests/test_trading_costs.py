from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.trading.costs import TradingCostConfig, calculate_entry, calculate_exit
from tw_quant.trading.paper import run_paper_trade
from tw_quant.trading.paper_update import update_paper_positions
from tw_quant.trading.pending import execute_pending_orders


def test_entry_slippage_uses_less_favorable_buy_price() -> None:
    config = TradingCostConfig(commission_rate=0.000399, min_commission=1, slippage_rate=0.001)

    costs = calculate_entry(raw_price=100.0, shares=1000, config=config)

    assert costs["entry_price_raw"] == 100.0
    assert costs["entry_price"] == 100.1
    assert costs["buy_slippage_cost"] == 100.0
    assert costs["buy_commission"] == 39.94

    small_order = calculate_entry(raw_price=10.0, shares=1, config=config)
    assert small_order["buy_commission"] == 1.0


def test_exit_slippage_uses_less_favorable_sell_price() -> None:
    config = TradingCostConfig(
        commission_rate=0.000399,
        min_commission=1,
        sell_tax_rate_stock=0.003,
        slippage_rate=0.001,
    )

    costs = calculate_exit(raw_price=100.0, shares=1000, stock_id="2330", config=config)

    assert costs["exit_price_raw"] == 100.0
    assert costs["exit_price"] == 99.9
    assert costs["sell_slippage_cost"] == 100.0
    assert costs["sell_commission"] == 39.86
    assert costs["sell_tax"] == 299.7


def test_sell_tax_rate_uses_etf_and_bond_etf_rates() -> None:
    config = TradingCostConfig(
        commission_rate=0.000399,
        min_commission=1,
        sell_tax_rate_stock=0.003,
        sell_tax_rate_etf=0.001,
        sell_tax_rate_bond_etf=0.0,
        slippage_rate=0.001,
    )

    etf = calculate_exit(raw_price=100.0, shares=1000, stock_id="0050", config=config)
    bond_etf = calculate_exit(raw_price=100.0, shares=1000, stock_id="00679B", config=config)

    assert etf["sell_tax"] == 99.9
    assert bond_etf["sell_tax"] == 0.0


def test_buy_commission_is_deducted_from_cash(tmp_path: Path) -> None:
    _write_risk_report(tmp_path, stop_loss=80.0)
    run_paper_trade(reports_dir=tmp_path, capital=10_000)
    engine = _engine_with_prices(tmp_path, [_price_frame("20260509", open_price=100.0, close=110.0)])

    execute_pending_orders(
        engine,
        reports_dir=tmp_path,
        capital=10_000,
        trading_cost={"commission_rate": 0.01},
    )
    result = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260509", capital=10_000)

    trade = result.updated_trades.iloc[0]
    summary = result.summary.iloc[0]
    assert trade["entry_commission"] == 10.0
    assert summary["cash"] == 8990.0
    assert summary["total_cost"] == 10.0
    assert summary["total_equity_after_cost"] == 10090.0


def test_sell_commission_tax_slippage_and_after_cost_pnl(tmp_path: Path) -> None:
    _write_risk_report(tmp_path, stop_loss=95.0)
    run_paper_trade(reports_dir=tmp_path, capital=10_000)
    engine = _engine_with_prices(tmp_path, [_price_frame("20260509", open_price=100.0, close=90.0)])
    trading_cost = {
        "commission_rate": 0.01,
        "sell_tax_rate_stock": 0.003,
        "slippage_rate": 0.01,
    }

    execute_pending_orders(engine, reports_dir=tmp_path, capital=10_000, trading_cost=trading_cost)
    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        trading_cost=trading_cost,
    )

    trade = result.updated_trades.iloc[0]
    summary = result.summary.iloc[0]
    assert trade["status"] == "CLOSED"
    assert trade["entry_price_raw"] == 100.0
    assert trade["entry_price"] == 101.0
    assert trade["entry_slippage"] == 1.0
    assert trade["entry_commission"] == 9.09
    assert trade["buy_commission"] == 9.09
    assert trade["buy_slippage_cost"] == 9.0
    assert trade["exit_price_raw"] == 90.0
    assert trade["exit_price"] == 89.1
    assert trade["exit_slippage"] == 0.9
    assert trade["exit_commission"] == 8.02
    assert trade["sell_commission"] == 8.02
    assert trade["exit_tax"] == 2.41
    assert trade["sell_tax"] == 2.41
    assert trade["sell_slippage_cost"] == 8.1
    assert trade["total_cost"] == 36.62
    assert trade["realized_pnl"] == -126.62
    assert trade["realized_pnl_after_cost"] == -126.62
    assert trade["realized_pnl_pct_after_cost"] == -0.137917
    assert summary["realized_pnl_after_cost"] == -126.62
    assert summary["total_equity_after_cost"] == 9873.38


def _engine_with_prices(tmp_path: Path, frames: list[pd.DataFrame]):
    engine = create_db_engine(f"sqlite:///{(tmp_path / 'prices.sqlite').as_posix()}")
    init_db(engine)
    for frame in frames:
        save_daily_prices(engine, frame)
    return engine


def _write_risk_report(path: Path, stop_loss: float) -> None:
    pd.DataFrame(
        [
            {
                "rank": 1,
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "close": 100.0,
                "total_score": 90.0,
                "risk_reason": "通過風控",
                "reason": "趨勢向上",
                "stop_loss_price": stop_loss,
                "suggested_position_pct": 0.1,
            }
        ]
    ).to_csv(path / "risk_pass_candidates_20260508.csv", index=False, encoding="utf-8-sig")


def _price_frame(trade_date: str, open_price: float, close: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "symbol": "2330",
                "name": "台積電",
                "open": open_price,
                "high": max(open_price, close) + 5,
                "low": min(open_price, close) - 5,
                "close": close,
                "volume": 2_000_000,
                "turnover": close * 2_000_000,
                "market": "TSE",
                "source": "TEST",
            }
        ]
    )
