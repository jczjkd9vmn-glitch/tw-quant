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


def test_update_paper_positions_handles_legacy_float_text_columns(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    legacy_frame = pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "signal_date": float("nan"),
                "planned_entry_date": float("nan"),
                "actual_entry_date": float("nan"),
                "entry_price_source": float("nan"),
                "stock_id": "2330",
                "stock_name": "TSMC",
                "entry_price": 100.0,
                "shares": 10,
                "position_value": 1000.0,
                "stop_loss_price": 80.0,
                "suggested_position_pct": 0.1,
                "status": "OPEN",
                "exit_date": float("nan"),
                "exit_reason": float("nan"),
            }
        ]
    )
    legacy_frame.to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8-sig")
    save_daily_prices(engine, _prices("20260510", {"2330": 70}))

    result = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260510", capital=10_000)

    row = result.updated_trades.iloc[0]
    assert row["status"] == "CLOSED"
    assert row["exit_date"] == "2026-05-10"
    assert isinstance(row["actual_entry_date"], str)


def test_take_profit_1_then_take_profit_2(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    _write_trades(tmp_path, [_trade("2330", entry_price=100, shares=10, stop_loss=80)])
    save_daily_prices(engine, _prices("20260509", {"2330": 110}))
    first = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260509", capital=10_000)
    row1 = first.updated_trades.iloc[0]
    assert row1["status"] == "OPEN"
    assert row1["exit_reason"] == "TAKE_PROFIT_1"
    assert row1["remaining_shares"] == 5

    save_daily_prices(engine, _prices("20260510", {"2330": 120}))
    second = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260510", capital=10_000)
    row2 = second.updated_trades.iloc[0]
    assert row2["status"] == "CLOSED"
    assert row2["exit_reason"] == "TAKE_PROFIT_2"
    assert row2["remaining_shares"] == 0


def test_trailing_stop_and_time_exit(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    _write_trades(
        tmp_path,
        [
            _trade("2330", entry_price=100, shares=10, stop_loss=70),
            _trade("2317", entry_price=100, shares=10, stop_loss=70) | {"trade_date": "2026-04-01"},
        ],
    )
    save_daily_prices(engine, _prices("20260509", {"2330": 110, "2317": 101}))
    update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260509", capital=10_000)

    save_daily_prices(engine, _prices("20260510", {"2330": 103, "2317": 102}))
    result = update_paper_positions(engine, reports_dir=tmp_path, trade_date="20260510", capital=10_000)
    rows = {r["stock_id"]: r for _, r in result.updated_trades.iterrows()}
    assert rows["2330"]["exit_reason"] == "TRAILING_STOP"
    assert rows["2330"]["status"] == "CLOSED"
    assert rows["2317"]["exit_reason"] == "TIME_EXIT"


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
