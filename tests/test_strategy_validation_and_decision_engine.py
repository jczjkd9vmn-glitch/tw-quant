from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.config import load_config
from tw_quant.decision.engine import generate_trading_decisions
from tw_quant.decision.grading import grade_candidate
from tw_quant.validation.strategy_validation import generate_strategy_validation


def test_strategy_validation_generates_models_and_filters(tmp_path: Path) -> None:
    _write_candidate_reports(tmp_path)
    trades_path = tmp_path / "paper_trades.csv"
    _paper_trades().to_csv(trades_path, index=False, encoding="utf-8-sig")
    original_trades = trades_path.read_text(encoding="utf-8-sig")

    result = generate_strategy_validation(tmp_path, min_trades_required=10)
    validation = result.validation

    assert result.output_path is not None and result.output_path.exists()
    assert set(validation["model_name"]) >= {
        "baseline_total_score",
        "multi_factor_rank",
        "confidence_filter",
        "liquidity_filter",
        "sector_strength_filter",
        "event_risk_filter",
        "combined_model",
    }
    counts = validation.set_index("model_name")["selected_count"].to_dict()
    assert counts["confidence_filter"] < counts["baseline_total_score"]
    assert counts["liquidity_filter"] < counts["baseline_total_score"]
    assert counts["sector_strength_filter"] < counts["baseline_total_score"]
    assert counts["event_risk_filter"] < counts["baseline_total_score"]
    assert "歷史交易樣本不足" in validation.set_index("model_name").loc["baseline_total_score", "notes"]
    assert trades_path.read_text(encoding="utf-8-sig") == original_trades
    assert not list(tmp_path.glob("pending_orders_*.csv"))


def test_strategy_validation_handles_missing_data_without_crashing(tmp_path: Path) -> None:
    result = generate_strategy_validation(tmp_path)

    assert result.warning == "no candidates report found"
    assert result.validation.empty


def test_candidate_grading_rules() -> None:
    assert grade_candidate(pd.Series(_candidate("2330", confidence_score=80, liquidity_score=80, sector_strength_score=65))).candidate_grade == "A"
    assert grade_candidate(pd.Series(_candidate("2331", confidence_score=55))).candidate_grade == "C"
    assert grade_candidate(pd.Series(_candidate("2332", is_attention_stock=True))).candidate_grade == "C"
    assert grade_candidate(pd.Series(_candidate("2333", is_disposition_stock=True))).candidate_grade == "D"
    assert grade_candidate(pd.Series(_candidate("2334", liquidity_score=35))).candidate_grade == "D"
    assert grade_candidate(pd.Series(_candidate("2335", risk_pass=0))).candidate_grade == "D"
    assert grade_candidate(pd.Series(_candidate("2336", confidence_score=65))).grade_reason


def test_decision_engine_outputs_advisory_decisions(tmp_path: Path) -> None:
    _write_candidate_reports(tmp_path)
    _paper_trades(open_position=True).to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8-sig")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("", encoding="utf-8")

    result = generate_trading_decisions(tmp_path, config_path=config_path)
    decisions = result.decisions

    assert result.output_path is not None and result.output_path.exists()
    by_symbol = decisions.set_index(["stock_id", "source"])
    assert by_symbol.loc[("2330", "candidate"), "decision"] == "BUY_CANDIDATE"
    assert bool(by_symbol.loc[("2330", "candidate"), "can_auto_trade"]) is False
    assert "review_level" in decisions.columns
    assert "review_reason" in decisions.columns
    assert by_symbol.loc[("2330", "candidate"), "review_level"] in {"STANDARD_REVIEW", "DATA_REVIEW", "RISK_REVIEW"}
    assert by_symbol.loc[("2317", "candidate"), "decision"] == "WATCH_ONLY"
    assert by_symbol.loc[("3008", "candidate"), "decision"] == "WATCH_ONLY"
    assert by_symbol.loc[("2603", "candidate"), "decision"] == "NO_TRADE"
    assert by_symbol.loc[("2454", "position"), "decision"] in {"REDUCE", "EXIT"}
    assert decisions["reason"].fillna("").astype(str).str.len().min() > 0
    assert not list(tmp_path.glob("paper_positions_*.csv"))
    assert not list(tmp_path.glob("pending_orders_*.csv"))


def test_auto_trading_defaults_are_safe() -> None:
    config = load_config("missing-test-config.yaml")

    assert config["auto_trading"]["enabled"] is False
    assert config["auto_trading"]["can_place_real_orders"] is False
    assert config["auto_trading"]["require_manual_approval"] is True
    assert config["decision_engine"]["output_advisory_only"] is True
    assert config["decision_engine"]["default_can_auto_trade"] is False
    assert config["decision_engine"]["min_grade_for_buy_candidate"] == "A"
    assert config["paper_trading_guardrails"]["enabled"] is True
    assert config["paper_trading_guardrails"]["min_grade_for_new_entry"] == "A"
    assert config["paper_trading_guardrails"]["allow_legacy_missing_grade"] is False
    assert config["market_regime"]["enabled"] is True


def _write_candidate_reports(path: Path) -> None:
    candidates = pd.DataFrame(
        [
            _candidate("2330", "台積電", confidence_score=80, liquidity_score=80, sector_strength_score=65),
            _candidate("2317", "鴻海", confidence_score=55, liquidity_score=70, sector_strength_score=55, risk_flags="資料不足"),
            _candidate("3008", "大立光", confidence_score=68, liquidity_score=70, sector_strength_score=55),
            _candidate("2603", "長榮", confidence_score=75, liquidity_score=80, sector_strength_score=70, event_risk_level="HIGH", event_blocked=True),
            _candidate("1101", "台泥", confidence_score=75, liquidity_score=45, sector_strength_score=70),
            _candidate("2303", "聯電", confidence_score=75, liquidity_score=70, sector_strength_score=40),
        ]
    )
    candidates.to_csv(path / "candidates_20260515.csv", index=False, encoding="utf-8-sig")
    candidates[candidates["risk_pass"] == 1].to_csv(path / "risk_pass_candidates_20260515.csv", index=False, encoding="utf-8-sig")


def _candidate(symbol: str, name: str = "測試股", **overrides) -> dict:
    row = {
        "trade_date": "2026-05-15",
        "stock_id": symbol,
        "stock_name": name,
        "risk_pass": 1,
        "total_score": 86.0,
        "risk_score": 80.0,
        "multi_factor_score": 78.0,
        "final_market_score": 76.0,
        "confidence_score": 75.0,
        "liquidity_score": 70.0,
        "slippage_risk_score": 70.0,
        "sector_strength_score": 55.0,
        "event_risk_level": "NONE",
        "event_blocked": False,
        "is_attention_stock": False,
        "is_disposition_stock": False,
        "risk_flags": "",
        "suggested_position_pct": 0.1,
    }
    row.update(overrides)
    return row


def _paper_trades(open_position: bool = False) -> pd.DataFrame:
    rows = [
        {
            "stock_id": "2330",
            "stock_name": "台積電",
            "status": "CLOSED",
            "realized_pnl_pct_after_cost": 0.08,
            "holding_days": 5,
            "exit_reason": "take_profit_1",
        },
        {
            "stock_id": "1101",
            "stock_name": "台泥",
            "status": "CLOSED",
            "realized_pnl_pct_after_cost": -0.04,
            "holding_days": 4,
            "exit_reason": "stop_loss",
        },
    ]
    if open_position:
        rows.append(
            {
                "trade_date": "2026-05-10",
                "stock_id": "2454",
                "stock_name": "聯發科",
                "status": "OPEN",
                "entry_price": 100.0,
                "current_price": 94.0,
                "stop_loss_price": 93.0,
                "risk_light": "紅燈",
                "liquidity_score": 45.0,
                "confidence_score": 55.0,
                "exit_reason": "",
            }
        )
    return pd.DataFrame(rows)
