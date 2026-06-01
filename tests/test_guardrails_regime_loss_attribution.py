from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.market_regime import evaluate_market_regime
from tw_quant.trading.paper import run_paper_trade
from tw_quant.trading.guardrails import build_guardrail_context, evaluate_candidate_entry
from tw_quant.validation.loss_attribution import generate_loss_attribution


def test_market_regime_uses_equal_weight_fallback_from_sqlite(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    for day in range(1, 26):
        save_daily_prices(
            engine,
            pd.DataFrame(
                [
                    _price("2026-05", day, "2330", 100 + day, 1_000_000),
                    _price("2026-05", day, "2317", 80 + day * 0.5, 800_000),
                ]
            ),
        )

    result = evaluate_market_regime(
        engine=engine,
        config={"market_regime": {"fallback_to_equal_weight_market": True}},
        trade_date="2026-05-25",
        reports_dir=tmp_path,
    )

    assert result.source == "equal_weight_market"
    assert result.market_regime_score > 60
    assert result.output_path is not None and result.output_path.exists()


def test_paper_guardrails_allow_only_a_grade_and_write_rejected_report(tmp_path: Path) -> None:
    _write_risk_pass(
        tmp_path,
        [
            _candidate("2330", "A", total_score=90, confidence_score=80, liquidity_score=80),
            _candidate("2317", "B", total_score=85, confidence_score=70, liquidity_score=70),
        ],
    )
    engine = _engine(tmp_path)
    _seed_uptrend(engine)

    result = run_paper_trade(
        reports_dir=tmp_path,
        capital=1_000_000,
        config=_config(),
        engine=engine,
    )

    assert len(result.pending_orders[result.pending_orders["status"] == "PENDING"]) == 1
    assert result.pending_orders.iloc[0]["stock_id"] == "2330"
    assert len(result.rejected_orders) == 1
    assert result.rejected_orders.iloc[0]["stock_id"] == "2317"
    assert "低於新增紙上交易門檻" in result.rejected_orders.iloc[0]["rejected_reason"]
    assert result.rejected_orders_path is not None and result.rejected_orders_path.exists()


def test_paper_guardrails_block_new_entries_on_total_drawdown(tmp_path: Path) -> None:
    _write_risk_pass(tmp_path, [_candidate("2330", "A")])
    pd.DataFrame([{"total_equity_after_cost": 928_077, "realized_pnl_after_cost_today": 0}]).to_csv(
        tmp_path / "paper_summary_20260515.csv",
        index=False,
    )

    engine = _engine(tmp_path)
    _seed_uptrend(engine)
    result = run_paper_trade(
        reports_dir=tmp_path,
        capital=1_000_000,
        config=_config(),
        engine=engine,
    )

    assert result.new_entries_allowed is False
    assert result.pending_orders.empty
    assert len(result.rejected_orders) == 1
    assert "總回撤" in result.pause_new_entries_reason


def test_paper_guardrails_block_low_market_regime(tmp_path: Path) -> None:
    _write_risk_pass(tmp_path, [_candidate("2330", "A")])
    engine = _engine(tmp_path)
    for day in range(1, 26):
        save_daily_prices(engine, pd.DataFrame([_price("2026-05", day, "2330", 130 - day, 1_000_000)]))

    result = run_paper_trade(
        reports_dir=tmp_path,
        capital=1_000_000,
        config=_config(),
        engine=engine,
    )

    assert result.market_regime_score < 60
    assert result.pending_orders.empty
    assert "market_regime_score" in result.pause_new_entries_reason


def test_guardrail_blocks_missing_candidate_grade_by_default(tmp_path: Path) -> None:
    context = build_guardrail_context(tmp_path, capital=1_000_000, config=_config())

    decision = evaluate_candidate_entry(
        pd.Series({"stock_id": "2330", "stock_name": "台積電", "event_blocked": False}),
        context,
        created_today=0,
    )

    assert decision.allowed is False
    assert decision.status == "BLOCKED"
    assert decision.reason == "缺少 candidate_grade，需人工確認"


def test_guardrail_allows_legacy_missing_grade_only_when_enabled(tmp_path: Path) -> None:
    config = _config()
    config["paper_trading_guardrails"]["allow_legacy_missing_grade"] = True
    context = build_guardrail_context(tmp_path, capital=1_000_000, config=config)

    decision = evaluate_candidate_entry(
        pd.Series({"stock_id": "2330", "stock_name": "台積電", "event_blocked": False}),
        context,
        created_today=0,
    )

    assert decision.allowed is True
    assert decision.reason == "legacy report missing candidate_grade; grade guardrail skipped"


def test_loss_attribution_outputs_loss_reason_fields(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "trade_date": "2026-05-10",
                "actual_entry_date": "2026-05-10",
                "entry_price": 100,
                "exit_price": 92,
                "status": "CLOSED",
                "realized_pnl_pct_after_cost": -0.08,
                "exit_reason": "stop_loss",
                "holding_days": 5,
                "liquidity_score": 45,
                "sector_strength_score": 40,
                "confidence_score": 55,
            },
            {
                "stock_id": "2317",
                "stock_name": "鴻海",
                "trade_date": "2026-05-12",
                "entry_price": 100,
                "status": "OPEN",
                "unrealized_pnl_pct": -0.03,
                "holding_days": 2,
            },
        ]
    ).to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame([{"trade_date": "2026-05-15", "market_regime_score": 45}]).to_csv(
        tmp_path / "market_regime_20260515.csv",
        index=False,
    )
    pd.DataFrame(
        [
            {"trade_date": "2026-05-15", "stock_id": "2330", "candidate_grade": "C"},
            {"trade_date": "2026-05-15", "stock_id": "2317", "candidate_grade": "B"},
        ]
    ).to_csv(tmp_path / "candidates_20260515.csv", index=False, encoding="utf-8-sig")

    result = generate_loss_attribution(tmp_path, trade_date="2026-05-15")

    assert result.output_path is not None and result.output_path.exists()
    assert len(result.attribution) == 2
    row = result.attribution[result.attribution["stock_id"] == "2330"].iloc[0]
    assert row["loss_bucket"] == "large_loss"
    assert "停損出場" in row["likely_loss_reason"]
    assert "市場環境偏弱" in row["likely_loss_reason"]


def _engine(tmp_path: Path):
    engine = create_db_engine(f"sqlite:///{(tmp_path / 'prices.sqlite').as_posix()}")
    init_db(engine)
    return engine


def _price(month_prefix: str, day: int, symbol: str, close: float, volume: int) -> dict:
    date_text = f"{month_prefix}-{day:02d}"
    return {
        "trade_date": date_text,
        "symbol": symbol,
        "name": f"測試{symbol}",
        "open": close - 1,
        "high": close + 2,
        "low": close - 2,
        "close": close,
        "volume": volume,
        "turnover": close * volume,
        "market": "TSE",
        "source": "TEST",
    }


def _seed_uptrend(engine) -> None:
    for index, trade_date in enumerate(pd.date_range("2026-03-02", periods=75, freq="D"), start=1):
        if trade_date.weekday() >= 5:
            continue
        save_daily_prices(
            engine,
            pd.DataFrame(
                [
                    {
                        **_price("2026-05", 1, "2330", 100 + index, 1_000_000),
                        "trade_date": trade_date.strftime("%Y-%m-%d"),
                    }
                ]
            ),
        )


def _candidate(symbol: str, grade: str, **overrides) -> dict:
    row = {
        "rank": 1,
        "trade_date": "2026-05-15",
        "stock_id": symbol,
        "stock_name": f"測試{symbol}",
        "close": 100.0,
        "total_score": 88,
        "risk_pass": 1,
        "risk_reason": "通過風控",
        "reason": "趨勢向上",
        "stop_loss_price": 92,
        "suggested_position_pct": 0.1,
        "candidate_grade": grade,
        "grade_reason": "測試分級",
        "confidence_score": 80,
        "liquidity_score": 80,
        "sector_strength_score": 60,
        "event_blocked": False,
    }
    row.update(overrides)
    return row


def _write_risk_pass(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path / "risk_pass_candidates_20260515.csv", index=False, encoding="utf-8-sig")


def _config() -> dict:
    return {
        "database": {"url": "sqlite:///:memory:"},
        "paper_trading_guardrails": {
            "enabled": True,
            "min_grade_for_new_entry": "A",
            "max_total_drawdown_pct": 0.05,
            "max_daily_loss_pct": 0.02,
            "max_consecutive_stop_loss": 3,
            "pause_new_entries_days": 5,
            "max_open_positions": 8,
            "max_daily_new_positions": 2,
        },
        "market_regime": {
            "enabled": True,
            "min_score_for_new_entries": 60,
            "fallback_to_equal_weight_market": True,
        },
    }
