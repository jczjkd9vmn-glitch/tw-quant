from __future__ import annotations

import pandas as pd

from tw_quant.trading.paper import (
    PENDING_ORDER_COLUMNS,
    POSITION_COLUMNS,
    find_latest_risk_pass_report,
    run_paper_trade,
)


def test_paper_trade_creates_pending_orders_without_same_day_open_positions(tmp_path) -> None:
    _write_risk_report(tmp_path, "20260508")

    result = run_paper_trade(reports_dir=tmp_path, capital=1_000_000)

    assert result.warning == ""
    assert result.positions_path is None
    assert result.pending_orders_path is not None
    assert result.pending_orders_path.exists()
    assert not result.trades_path.exists()
    assert result.new_positions.empty
    assert result.positions.empty
    assert len(result.pending_orders) == 2
    assert list(result.pending_orders.columns) == PENDING_ORDER_COLUMNS

    first = result.pending_orders[result.pending_orders["stock_id"] == "00891"].iloc[0]
    assert first["stock_name"] == "中信關鍵半導體"
    assert first["signal_date"] == "2026-05-08"
    assert first["planned_entry_date"] == "NEXT_AVAILABLE_TRADING_DAY"
    assert first["status"] == "PENDING"


def test_paper_trade_preserves_existing_open_positions_without_rebuilding(tmp_path) -> None:
    _write_risk_report(tmp_path, "20260508")
    existing = pd.DataFrame(
        [
            {
                "trade_date": "2026-05-07",
                "stock_id": "00891",
                "stock_name": "中信關鍵半導體",
                "entry_price": 33.0,
                "shares": 1000,
                "position_value": 33_000.0,
                "stop_loss_price": 31.0,
                "suggested_position_pct": 0.1,
                "status": "OPEN",
            }
        ]
    )
    existing.to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8-sig")

    result = run_paper_trade(reports_dir=tmp_path, capital=1_000_000)

    trades = pd.read_csv(result.trades_path, dtype={"stock_id": str})
    assert result.skipped_existing == []
    assert result.new_positions.empty
    assert len(result.pending_orders) == 2
    assert len(trades) == 1
    assert len(trades[trades["stock_id"] == "00891"]) == 1
    assert len(result.positions) == 1
    assert list(result.positions.columns) == POSITION_COLUMNS


def test_paper_trade_warns_when_no_risk_report(tmp_path) -> None:
    result = run_paper_trade(reports_dir=tmp_path, capital=1_000_000)

    assert result.warning == "no risk_pass_candidates report found"
    assert result.positions_path is None
    assert result.positions.empty


def test_find_latest_risk_pass_report_uses_latest_date(tmp_path) -> None:
    _write_risk_report(tmp_path, "20260507")
    _write_risk_report(tmp_path, "20260508")

    latest = find_latest_risk_pass_report(tmp_path)

    assert latest is not None
    assert latest.name == "risk_pass_candidates_20260508.csv"


def _write_risk_report(path, date_label: str) -> None:
    trade_date = f"{date_label[:4]}-{date_label[4:6]}-{date_label[6:]}"
    frame = pd.DataFrame(
        [
            {
                "rank": 1,
                "trade_date": trade_date,
                "stock_id": "00891",
                "stock_name": "中信關鍵半導體",
                "close": 34.19,
                "total_score": 83.64,
                "trend_score": 100,
                "momentum_score": 94.56,
                "fundamental_score": 50,
                "chip_score": 50,
                "risk_score": 100,
                "is_candidate": 1,
                "risk_pass": 1,
                "risk_reason": "通過風控",
                "reason": "收盤價高於 20 日均線",
                "stop_loss_price": 32.27,
                "suggested_position_pct": 0.1,
            },
            {
                "rank": 2,
                "trade_date": trade_date,
                "stock_id": "3528",
                "stock_name": "安馳",
                "close": 91.0,
                "total_score": 84.67,
                "trend_score": 100,
                "momentum_score": 98.67,
                "fundamental_score": 50,
                "chip_score": 50,
                "risk_score": 100,
                "is_candidate": 1,
                "risk_pass": 1,
                "risk_reason": "通過風控",
                "reason": "收盤價高於 20 日均線",
                "stop_loss_price": 85.39,
                "suggested_position_pct": 0.1,
            },
        ]
    )
    frame.to_csv(path / f"risk_pass_candidates_{date_label}.csv", index=False, encoding="utf-8-sig")
