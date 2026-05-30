from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.reporting.candidate_coverage import (
    build_candidate_coverage_report,
    generate_candidate_coverage_report,
)
from tw_quant.reporting.position_review import (
    build_position_review_summary,
    generate_position_review_summary,
)
from tw_quant.scoring.multi_factor import apply_multi_factor_scores


def test_etf_is_not_marked_financial_missing_when_eps_roe_missing() -> None:
    candidates = pd.DataFrame(
        [
            {
                "trade_date": "2026-05-27",
                "stock_id": "00923",
                "stock_name": "群益台ESG低碳50",
                "market_type": "ETF",
                "total_score": 80,
                "risk_score": 70,
                "risk_pass": 1,
                "eps": None,
                "roe": None,
                "liquidity_score": 80,
            }
        ]
    )

    result = apply_multi_factor_scores(candidates, config={"multi_factor": {"enabled": True}}, data_dir=Path("missing-data-dir"))
    row = result.candidates.iloc[0]

    assert "財報資料不足" not in str(row["data_quality_flags"])
    assert "ETF_METADATA_MISSING" in str(row["data_quality_flags"])

    coverage = build_candidate_coverage_report(result.candidates)
    missing = str(coverage.iloc[0]["missing_fields"])
    assert "FINANCIAL_MISSING" not in missing
    assert "ETF_METADATA_MISSING" in missing


def test_candidate_coverage_report_is_generated(tmp_path: Path) -> None:
    _candidate_frame().to_csv(tmp_path / "candidates_20260527.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-27",
                "stock_id": "2330",
                "stock_name": "台積電",
                "source": "candidate",
                "decision": "BUY_CANDIDATE",
                "candidate_grade": "A",
                "review_level": "STANDARD_REVIEW",
            }
        ]
    ).to_csv(tmp_path / "trading_decisions_20260527.csv", index=False, encoding="utf-8-sig")

    result = generate_candidate_coverage_report(tmp_path)

    assert result.output_path is not None and result.output_path.exists()
    assert len(result.coverage) == 1
    assert set(
        [
            "has_industry",
            "has_valuation",
            "has_financials",
            "has_revenue",
            "has_institutional",
            "has_margin",
            "has_event_data",
            "missing_fields",
        ]
    ).issubset(result.coverage.columns)
    assert result.coverage.iloc[0]["decision"] == "BUY_CANDIDATE"


def test_candidate_coverage_treats_market_relative_fallback_as_missing_industry() -> None:
    coverage = build_candidate_coverage_report(
        pd.DataFrame(
            [
                {
                    "trade_date": "2026-05-27",
                    "stock_id": "2330",
                    "stock_name": "台積電",
                    "industry": "全市場",
                    "sector_strength_mode": "market_relative_fallback",
                    "sector_strength_warning": "缺少產業分類，使用全市場相對強弱",
                    "pe_ratio": 18,
                    "eps": 8,
                    "revenue_yoy": 15,
                    "foreign_net_buy": 1000,
                    "margin_balance": 5000,
                    "event_risk_level": "LOW",
                }
            ]
        )
    )

    assert bool(coverage.iloc[0]["has_industry"]) is False
    assert "INDUSTRY_MISSING" in str(coverage.iloc[0]["missing_fields"])


def test_relative_strength_is_positive_not_blocking_risk() -> None:
    candidates = pd.DataFrame(
        [
            {
                "trade_date": "2026-05-27",
                "stock_id": "2330",
                "stock_name": "台積電",
                "total_score": 82,
                "risk_score": 80,
                "risk_pass": 1,
                "relative_strength_20d": 0.08,
                "relative_strength_5d": 0.02,
                "sector_strength_score": 72,
            }
        ]
    )

    result = apply_multi_factor_scores(candidates, config={"multi_factor": {"enabled": True}}, data_dir=Path("missing-data-dir"))
    row = result.candidates.iloc[0]

    assert "相對強勢" in str(row["positive_signals"])
    assert "相對強勢" not in str(row["blocking_risks"])


def test_attention_and_disposition_are_blocking_risks(tmp_path: Path) -> None:
    attention_path = tmp_path / "attention_disposition.csv"
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-27",
                "stock_id": "2330",
                "stock_name": "台積電",
                "is_attention_stock": True,
                "attention_reason": "公布注意交易資訊",
                "is_disposition_stock": False,
                "disposition_start_date": "",
                "disposition_end_date": "",
                "disposition_reason": "",
            },
            {
                "trade_date": "2026-05-27",
                "stock_id": "2317",
                "stock_name": "鴻海",
                "is_attention_stock": False,
                "attention_reason": "",
                "is_disposition_stock": True,
                "disposition_start_date": "2026-05-27",
                "disposition_end_date": "2026-06-03",
                "disposition_reason": "處置交易",
            },
        ]
    ).to_csv(attention_path, index=False, encoding="utf-8-sig")
    candidates = pd.DataFrame(
        [
            {"trade_date": "2026-05-27", "stock_id": "2330", "stock_name": "台積電", "total_score": 80, "risk_score": 70, "risk_pass": 1},
            {"trade_date": "2026-05-27", "stock_id": "2317", "stock_name": "鴻海", "total_score": 80, "risk_score": 70, "risk_pass": 1},
        ]
    )

    result = apply_multi_factor_scores(
        candidates,
        config={"multi_factor": {"enabled": True}, "event_risk": {"block_disposition_stock": True}},
        data_dir=tmp_path,
    )
    by_symbol = result.candidates.set_index("stock_id")

    assert "注意股" in str(by_symbol.loc["2330", "blocking_risks"])
    assert "處置股" in str(by_symbol.loc["2317", "blocking_risks"])


def test_position_review_summary_is_generated(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-20",
                "stock_id": "2330",
                "stock_name": "台積電",
                "status": "OPEN",
                "entry_price": 100,
                "current_price": 107.2,
                "stop_loss_price": 94,
                "unrealized_pnl_pct": 0.072,
            },
            {
                "trade_date": "2026-05-20",
                "stock_id": "2317",
                "stock_name": "鴻海",
                "status": "CLOSED",
            },
        ]
    ).to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-27",
                "stock_id": "2330",
                "stock_name": "台積電",
                "source": "position",
                "decision": "HOLD",
                "review_reason": "一般檢查",
            }
        ]
    ).to_csv(tmp_path / "trading_decisions_20260527.csv", index=False, encoding="utf-8-sig")

    result = generate_position_review_summary(
        tmp_path,
        config={"exit_strategy": {"take_profit_1_pct": 0.08}},
        trade_date="2026-05-27",
    )

    assert result.output_path is not None and result.output_path.exists()
    assert len(result.review) == 1
    assert bool(result.review.iloc[0]["near_take_profit"]) is True
    assert {"hold", "reduce", "exit_review", "near_stop_loss", "near_take_profit", "data_quality_warning"}.issubset(
        result.review.columns
    )


def test_build_position_review_summary_without_files() -> None:
    review = build_position_review_summary(
        pd.DataFrame(
            [
                {
                    "trade_date": "2026-05-27",
                    "stock_id": "2330",
                    "stock_name": "台積電",
                    "status": "OPEN",
                    "current_price": 97.0,
                    "stop_loss_price": 95.0,
                    "unrealized_pnl_pct": -0.03,
                    "data_quality_flags": "資料不足",
                }
            ]
        ),
        pd.DataFrame(),
        {"local_factors": {"holding_risk_light": {"near_stop_loss_pct": 0.03}}},
        "2026-05-27",
    )

    assert bool(review.iloc[0]["near_stop_loss"]) is True
    assert bool(review.iloc[0]["exit_review"]) is True
    assert bool(review.iloc[0]["data_quality_warning"]) is True


def _candidate_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-05-27",
                "stock_id": "2330",
                "stock_name": "台積電",
                "industry": "半導體",
                "pe_ratio": 18,
                "eps": 8,
                "roe": 20,
                "revenue_yoy": 15,
                "foreign_net_buy": 1000,
                "margin_balance": 5000,
                "event_risk_level": "LOW",
            }
        ]
    )
