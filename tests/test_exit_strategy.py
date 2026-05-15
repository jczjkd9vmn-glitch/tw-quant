from __future__ import annotations

from datetime import timedelta

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.trading.paper_update import update_paper_positions


def test_take_profit_1_sells_half_and_keeps_position_open(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=10)])
    save_daily_prices(engine, _prices({"20260509": 108}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        exit_strategy=_exit_config(),
    )

    row = result.updated_trades.iloc[0]
    assert row["status"] == "OPEN"
    assert row["exit_reason"] == "take_profit_1"
    assert row["remaining_shares"] == 5
    assert bool(row["partial_exit_1_done"])


def test_default_exit_strategy_is_enabled_when_none_is_passed(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=10)])
    save_daily_prices(engine, _prices({"20260509": 108}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        exit_strategy=None,
    )

    row = result.updated_trades.iloc[0]
    assert row["status"] == "OPEN"
    assert row["exit_reason"] == "take_profit_1"
    assert row["remaining_shares"] == 5
    assert bool(row["partial_exit_1_done"])


def test_take_profit_2_sells_half_of_remaining_position(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=100, remaining_shares=50, partial_exit_1_done=True)])
    save_daily_prices(engine, _prices({"20260509": 115}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        exit_strategy=_exit_config(),
    )

    row = result.updated_trades.iloc[0]
    assert row["status"] == "OPEN"
    assert row["exit_reason"] == "take_profit_2"
    assert row["remaining_shares"] == 25
    assert bool(row["partial_exit_2_done"])


def test_trailing_stop_triggers_after_drawdown_from_high(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=10, highest_price_since_entry=120)])
    save_daily_prices(engine, _prices({"20260509": 110}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        exit_strategy={**_exit_config(), "take_profit_1_pct": 999, "take_profit_2_pct": 999},
    )

    row = result.updated_trades.iloc[0]
    assert row["status"] == "CLOSED"
    assert row["exit_reason"] == "trailing_stop"
    assert row["trailing_stop_price"] == 110.4


def test_ma_exit_when_close_breaks_20_day_average(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=10, stop_loss=70)])
    save_daily_prices(engine, _history_prices("2026-04-20", 19, 100))
    save_daily_prices(engine, _prices({"20260509": 90}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        exit_strategy={**_exit_config(), "take_profit_1_pct": 999, "take_profit_2_pct": 999},
    )

    row = result.updated_trades.iloc[0]
    assert row["status"] == "CLOSED"
    assert row["exit_reason"] == "ma20_break"


def test_time_exit_when_holding_too_long_and_profit_is_low(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=10, trade_date="2026-04-01", stop_loss=70)])
    save_daily_prices(engine, _history_prices("2026-04-02", 31, 100))
    save_daily_prices(engine, _prices({"20260509": 101}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        exit_strategy={**_exit_config(), "ma_exit_window": 0},
    )

    row = result.updated_trades.iloc[0]
    assert row["status"] == "CLOSED"
    assert row["exit_reason"] == "max_holding_days"


def test_exit_strategy_calculates_costs_on_each_sell(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=10)])
    save_daily_prices(engine, _prices({"20260509": 110}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        trading_cost={"commission_rate": 0.01, "sell_tax_rate_stock": 0.003, "slippage_rate": 0.01},
        exit_strategy=_exit_config(),
    )

    row = result.updated_trades.iloc[0]
    assert row["exit_slippage"] > 0
    assert row["exit_commission"] > 0
    assert row["exit_tax"] > 0
    assert row["realized_pnl_after_cost"] < 50


def test_take_profit_1_does_not_trigger_twice(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=10, remaining_shares=5, partial_exit_1_done=True)])
    save_daily_prices(engine, _prices({"20260509": 108}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        exit_strategy={**_exit_config(), "take_profit_2_pct": 999, "ma_exit_window": 0},
    )

    row = result.updated_trades.iloc[0]
    assert row["status"] == "OPEN"
    assert row["exit_reason"] in {"", None} or pd.isna(row["exit_reason"])
    assert row["remaining_shares"] == 5


def test_ma_exit_skips_when_history_is_less_than_20_days(tmp_path) -> None:
    engine = _engine()
    _write_trades(tmp_path, [_trade(shares=10, stop_loss=70)])
    save_daily_prices(engine, _history_prices("2026-05-01", 5, 100))
    save_daily_prices(engine, _prices({"20260509": 90}))

    result = update_paper_positions(
        engine,
        reports_dir=tmp_path,
        trade_date="20260509",
        capital=10_000,
        exit_strategy={**_exit_config(), "take_profit_1_pct": 999, "take_profit_2_pct": 999},
    )

    row = result.updated_trades.iloc[0]
    assert row["status"] == "OPEN"
    assert row["exit_reason"] in {"", None} or pd.isna(row["exit_reason"])


def _engine():
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    return engine


def _write_trades(path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path / "paper_trades.csv", index=False, encoding="utf-8-sig")


def _trade(
    shares: int,
    trade_date: str = "2026-05-08",
    stop_loss: float = 80,
    remaining_shares: int | None = None,
    partial_exit_1_done: bool = False,
    highest_price_since_entry: float | None = None,
) -> dict:
    remaining = remaining_shares if remaining_shares is not None else shares
    return {
        "trade_date": trade_date,
        "stock_id": "2330",
        "stock_name": "台積電",
        "entry_price": 100.0,
        "shares": shares,
        "original_shares": shares,
        "remaining_shares": remaining,
        "position_value": remaining * 100.0,
        "entry_commission": 0.0,
        "entry_slippage": 0.0,
        "total_cost": 0.0,
        "realized_pnl_after_cost": 0.0,
        "stop_loss_price": stop_loss,
        "suggested_position_pct": 0.1,
        "partial_exit_1_done": partial_exit_1_done,
        "partial_exit_2_done": False,
        "highest_price_since_entry": highest_price_since_entry or 100.0,
        "status": "OPEN",
    }


def _prices(close_by_date: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": pd.to_datetime(date),
                "symbol": "2330",
                "name": "台積電",
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000_000,
                "turnover": close * 1_000_000,
                "market": "TSE",
                "source": "TEST",
            }
            for date, close in close_by_date.items()
        ]
    )


def _history_prices(start: str, days: int, close: float) -> pd.DataFrame:
    start_date = pd.to_datetime(start)
    return _prices({(start_date + timedelta(days=day)).strftime("%Y%m%d"): close for day in range(days)})


def _exit_config() -> dict[str, float]:
    return {
        "take_profit_1_pct": 0.08,
        "take_profit_1_sell_pct": 0.50,
        "take_profit_2_pct": 0.15,
        "take_profit_2_sell_pct": 0.50,
        "trailing_stop_activate_pct": 0.08,
        "trailing_stop_drawdown_pct": 0.08,
        "ma_exit_window": 20,
        "max_holding_days": 30,
        "min_profit_for_holding": 0.03,
    }
