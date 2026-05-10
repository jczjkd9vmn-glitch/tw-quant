from __future__ import annotations

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.trading.paper_update import SUMMARY_COLUMNS, TRADE_COLUMNS, update_paper_positions


def test_update_paper_positions_updates_open_position(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    _write_trades(tmp_path, [_trade("2330", entry_price=100, shares=10, stop_loss=90)])
    save_daily_prices(engine, _prices("20260509", {"2330": 110}))

    result = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260509", capital=10_000)

    row = result.updated_trades.iloc[0]
    assert result.warning == ""
    assert result.portfolio_path is not None
    assert result.summary_path is not None
    assert result.portfolio_path.exists()
    assert result.summary_path.exists()
    assert list(result.updated_trades.columns) == TRADE_COLUMNS
    assert row["status"] == "OPEN"
    assert row["current_price"] == 110
    assert row["market_value"] == 1100
    assert row["unrealized_pnl"] == 100
    assert row["unrealized_pnl_pct"] == 0.1
    assert row["holding_days"] == 1
    assert not bool(row["stop_loss_hit"])


def test_update_paper_positions_closes_stop_loss(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    _write_trades(tmp_path, [_trade("2330", entry_price=100, shares=10, stop_loss=90)])
    save_daily_prices(engine, _prices("20260509", {"2330": 85}))

    result = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260509", capital=10_000)

    row = result.updated_trades.iloc[0]
    assert row["status"] == "CLOSED"
    assert row["exit_date"] == "2026-05-09"
    assert row["exit_price"] == 85
    assert row["realized_pnl"] == -150
    assert row["realized_pnl_pct"] == -0.15
    assert row["exit_reason"] == "STOP_LOSS"
    assert bool(row["stop_loss_hit"])


def test_update_paper_positions_warns_when_date_has_no_price_data(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    _write_trades(tmp_path, [_trade("2330", entry_price=100, shares=10, stop_loss=90)])

    result = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260509", capital=10_000)

    assert result.warning == "no price data found for 20260509"
    assert result.portfolio_path is None
    assert result.summary_path is None
    assert result.updated_trades.iloc[0]["status"] == "OPEN"


def test_update_paper_positions_summary_is_correct(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    _write_trades(
        tmp_path,
        [
            _trade("2330", entry_price=100, shares=10, stop_loss=90),
            _trade("2317", entry_price=50, shares=20, stop_loss=46),
        ],
    )
    save_daily_prices(engine, _prices("20260509", {"2330": 110, "2317": 45}))

    result = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260509", capital=10_000)
    summary = result.summary.iloc[0]

    assert list(result.summary.columns) == SUMMARY_COLUMNS
    assert summary["total_capital"] == 10_000
    assert summary["invested_value"] == 1000
    assert summary["market_value"] == 1100
    assert summary["cash"] == 8900
    assert summary["unrealized_pnl"] == 100
    assert summary["realized_pnl"] == -100
    assert summary["total_equity"] == 10000
    assert summary["open_positions"] == 1
    assert summary["closed_positions"] == 1


def _write_trades(path, rows: list[dict]) -> None:
    frame = pd.DataFrame(rows)
    frame.to_csv(path / "paper_trades.csv", index=False, encoding="utf-8-sig")


def _trade(stock_id: str, entry_price: float, shares: int, stop_loss: float) -> dict:
    return {
        "trade_date": "2026-05-08",
        "stock_id": stock_id,
        "stock_name": stock_id,
        "entry_price": entry_price,
        "shares": shares,
        "position_value": entry_price * shares,
        "stop_loss_price": stop_loss,
        "suggested_position_pct": 0.1,
        "status": "OPEN",
    }


def _prices(trade_date: str, close_by_symbol: dict[str, float]) -> pd.DataFrame:
    rows = []
    for symbol, close in close_by_symbol.items():
        rows.append(
            {
                "trade_date": pd.to_datetime(trade_date),
                "symbol": symbol,
                "name": symbol,
                "open": close,
                "high": close * 1.02,
                "low": close * 0.98,
                "close": close,
                "volume": 1_000_000,
                "turnover": close * 1_000_000,
                "market": "TSE",
                "source": "TEST",
            }
        )
    return pd.DataFrame(rows)
