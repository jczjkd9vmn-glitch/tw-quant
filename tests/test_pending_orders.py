from __future__ import annotations

from pathlib import Path

import pandas as pd

import tw_quant.trading.pending as pending_module
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


def test_pending_order_does_not_backfill_retroactively_when_runner_is_late(tmp_path: Path) -> None:
    _write_risk_report(tmp_path)
    run_paper_trade(reports_dir=tmp_path, capital=1_000_000)
    engine = _engine_with_prices(
        tmp_path,
        [
            _price_frame("20260509", open_price=101.0, close=103.0),
            _price_frame("20260510", open_price=110.0, close=112.0),
        ],
    )

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000)

    order = result.executed_orders.iloc[0]
    assert order["attempted_execution_date"] == "2026-05-10"
    assert order["actual_entry_date"] == "2026-05-10"
    assert int(order["order_age_trading_days"]) == 2
    assert float(order["entry_price"]) == 110.0
    assert order["actual_entry_date"] != "2026-05-09"


def test_pending_order_expiry_uses_elapsed_trading_days_to_attempted_execution_date(tmp_path: Path) -> None:
    _write_risk_report(tmp_path)
    run_paper_trade(reports_dir=tmp_path, capital=1_000_000)
    engine = _engine_with_prices(
        tmp_path,
        [
            _price_frame("20260509", open_price=101.0, close=103.0),
            _price_frame("20260510", open_price=110.0, close=112.0),
            _price_frame("20260511", open_price=120.0, close=121.0),
        ],
    )

    result = execute_pending_orders(
        engine,
        reports_dir=tmp_path,
        capital=1_000_000,
        config={
            "pending_order": {"expire_after_trading_days": 1},
            "paper_trading_guardrails": {"enabled": False},
            "market_regime": {"enabled": False},
        },
    )

    pending = pd.read_csv(tmp_path / "pending_orders_20260508.csv", dtype={"stock_id": str})
    rejected = pd.read_csv(tmp_path / "rejected_paper_orders_20260511.csv", dtype={"stock_id": str})
    order = pending.iloc[0]
    assert result.executed_orders.empty
    assert order["status"] == "EXPIRED"
    assert order["attempted_execution_date"] == "2026-05-11"
    assert int(order["order_age_trading_days"]) == 3
    assert rejected.iloc[0]["final_order_status"] == "EXPIRED"


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


def test_pending_order_loads_price_history_once_for_multiple_orders(tmp_path: Path, monkeypatch) -> None:
    pd.DataFrame(
        [
            _pending_row("2330", "台積電"),
            _pending_row("2317", "鴻海"),
        ]
    ).to_csv(tmp_path / "pending_orders_20260508.csv", index=False, encoding="utf-8")
    engine = _engine_with_prices(
        tmp_path,
        [
            _price_frame("20260509", open_price=101.0, close=103.0, symbol="2330", name="台積電"),
            _price_frame("20260509", open_price=151.0, close=153.0, symbol="2317", name="鴻海"),
        ],
    )
    calls = 0
    original_load = pending_module.load_price_history

    def counting_load(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original_load(*args, **kwargs)

    monkeypatch.setattr(pending_module, "load_price_history", counting_load)

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000)

    assert calls == 1
    assert len(result.executed_orders) == 2


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
    old_trade.to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8")
    run_paper_trade(reports_dir=tmp_path, capital=1_000_000)
    engine = _engine_with_prices(tmp_path, [_price_frame("20260509", open_price=1010.0, close=1015.0)])

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000)

    trades = pd.read_csv(tmp_path / "paper_trades.csv", dtype={"stock_id": str})
    pending = pd.read_csv(tmp_path / "pending_orders_20260508.csv", dtype={"stock_id": str})
    rejected = pd.read_csv(tmp_path / "rejected_paper_orders_20260508.csv", dtype={"stock_id": str})
    assert result.executed_orders.empty
    assert len(trades) == 1
    assert trades.iloc[0]["current_price"] == 950.0
    assert pending.empty
    assert rejected.iloc[0]["status"] == "REJECTED_GUARDRAIL"
    assert "已有未平倉持倉" in rejected.iloc[0]["rejected_reason"]


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
                "candidate_grade": "A",
            }
        ]
    ).to_csv(path / "risk_pass_candidates_20260508.csv", index=False, encoding="utf-8")


def _pending_row(stock_id: str, stock_name: str) -> dict[str, object]:
    return {
        "signal_date": "2026-05-08",
        "planned_entry_date": "NEXT_AVAILABLE_TRADING_DAY",
        "stock_id": stock_id,
        "stock_name": stock_name,
        "stop_loss_price": 90.0,
        "suggested_position_pct": 0.1,
        "status": "PENDING",
    }


def _price_frame(
    trade_date: str,
    open_price: float,
    close: float,
    symbol: str = "2330",
    name: str = "台積電",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "symbol": symbol,
                "name": name,
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
