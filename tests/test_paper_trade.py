from __future__ import annotations

import pandas as pd

from tw_quant.trading.paper import POSITION_COLUMNS, find_latest_risk_pass_report, run_paper_trade


def test_paper_trade_creates_positions_and_trade_log(tmp_path) -> None:
    _write_risk_report(tmp_path, "20260508")

    result = run_paper_trade(reports_dir=tmp_path, capital=1_000_000)

    assert result.warning == ""
    assert result.positions_path is not None
    assert result.positions_path.exists()
    assert result.trades_path.exists()
    assert len(result.new_positions) == 2
    assert len(result.positions) == 2
    assert list(result.positions.columns) == POSITION_COLUMNS

    first = result.positions[result.positions["stock_id"] == "00891"].iloc[0]
    assert first["stock_name"] == "中信關鍵半導體"
    assert first["entry_price"] == 34.19
    assert first["shares"] == int(100_000 // 34.19)
    assert first["position_value"] == round(first["shares"] * 34.19, 2)
    assert first["status"] == "OPEN"

    trades = pd.read_csv(result.trades_path, dtype={"stock_id": str})
    assert len(trades) == 2
    assert set(trades["stock_id"]) == {"00891", "3528"}


def test_paper_trade_skips_existing_open_positions(tmp_path) -> None:
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
    assert result.skipped_existing == ["00891"]
    assert len(result.new_positions) == 1
    assert len(trades) == 2
    assert len(trades[trades["stock_id"] == "00891"]) == 1
    assert len(result.positions) == 2


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
