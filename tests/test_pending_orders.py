from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.trading.paper import run_paper_trade
from tw_quant.trading.pending import execute_pending_orders


def test_pending_order_executes_on_next_valid_trading_day_using_open(tmp_path: Path) -> None:
    _write_risk_report(tmp_path)
    run_paper_trade(reports_dir=tmp_path, capital=1_000_000)
    engine = _engine_with_prices(tmp_path, [_price_frame("20260509", open_price=101.0, close=103.0)])

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000)

    assert len(result.executed_orders) == 1
    order = result.executed_orders.iloc[0]
    assert order["actual_entry_date"] == "2026-05-09"
    assert float(order["entry_price"]) == 101.0
    assert order["entry_price_source"] == "OPEN"

    trades = pd.read_csv(tmp_path / "paper_trades.csv", dtype={"stock_id": str})
    trade = trades.iloc[0]
    assert trade["status"] == "OPEN"
    assert trade["trade_date"] == "2026-05-09"
    assert trade["signal_date"] == "2026-05-08"
    assert trade["actual_entry_date"] == "2026-05-09"
    assert trade["entry_price_source"] == "OPEN"
    assert float(trade["entry_price"]) == 101.0


def test_pending_order_falls_back_to_close_when_open_is_invalid(tmp_path: Path) -> None:
    _write_risk_report(tmp_path)
    run_paper_trade(reports_dir=tmp_path, capital=1_000_000)
    engine = _engine_with_prices(tmp_path, [_price_frame("20260509", open_price=0.0, close=103.0)])

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000)

    order = result.executed_orders.iloc[0]
    assert float(order["entry_price"]) == 103.0
    assert order["entry_price_source"] == "CLOSE_FALLBACK"
    assert "開盤價缺失或無效" in order["warning"]
    assert result.warnings


def test_pending_order_stays_pending_without_next_trading_day_data(tmp_path: Path) -> None:
    _write_risk_report(tmp_path)
    run_paper_trade(reports_dir=tmp_path, capital=1_000_000)
    engine = _engine_with_prices(tmp_path, [])

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000)

    pending = pd.read_csv(tmp_path / "pending_orders_20260508.csv", dtype={"stock_id": str})
    assert result.executed_orders.empty
    assert pending.iloc[0]["status"] == "PENDING"
    assert "尚無下一個有效交易日資料" in pending.iloc[0]["warning"]


def test_pending_order_skips_when_existing_open_position_and_preserves_old_trade(tmp_path: Path) -> None:
    _write_risk_report(tmp_path)
    old_trade = pd.DataFrame(
        [
            {
                "trade_date": "2026-05-01",
                "stock_id": "2330",
                "stock_name": "台積電",
                "entry_price": 900.0,
                "shares": 100,
                "position_value": 90_000.0,
                "stop_loss_price": 850.0,
                "suggested_position_pct": 0.1,
                "status": "OPEN",
                "current_price": 950.0,
                "market_value": 95_000.0,
                "unrealized_pnl": 5_000.0,
            }
        ]
    )
    old_trade.to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8-sig")
    run_paper_trade(reports_dir=tmp_path, capital=1_000_000)
    engine = _engine_with_prices(tmp_path, [_price_frame("20260509", open_price=1010.0, close=1015.0)])

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000)

    trades = pd.read_csv(tmp_path / "paper_trades.csv", dtype={"stock_id": str})
    pending = pd.read_csv(tmp_path / "pending_orders_20260508.csv", dtype={"stock_id": str})
    assert result.executed_orders.empty
    assert len(trades) == 1
    assert trades.iloc[0]["current_price"] == 950.0
    assert pending.iloc[0]["status"] == "SKIPPED_EXISTING_POSITION"
    assert "已有未平倉持倉" in pending.iloc[0]["skipped_reason"]


def _engine_with_prices(tmp_path: Path, frames: list[pd.DataFrame]):
    engine = create_db_engine(f"sqlite:///{(tmp_path / 'prices.sqlite').as_posix()}")
    init_db(engine)
    for frame in frames:
        save_daily_prices(engine, frame)
    return engine


def _write_risk_report(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "rank": 1,
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "close": 1000.0,
                "total_score": 90.0,
                "risk_reason": "通過風控",
                "reason": "趨勢向上",
                "stop_loss_price": 920.0,
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
                "high": max(open_price, close, 1.0) + 5,
                "low": min(value for value in [open_price, close] if value > 0) - 1,
                "close": close,
                "volume": 2_000_000,
                "turnover": close * 2_000_000,
                "market": "TSE",
                "source": "TEST",
            }
        ]
    )
