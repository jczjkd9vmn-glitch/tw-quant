from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.reporting.factor_diagnostics import (
    BENCHMARK_DIAGNOSTICS_COLUMNS,
    FACTOR_ATTRIBUTION_COLUMNS,
    GUARDRAIL_IMPACT_COLUMNS,
    generate_factor_diagnostics,
)


def test_factor_diagnostics_generates_csv_schema_without_paper_trades(tmp_path: Path) -> None:
    _write_minimal_candidates(tmp_path)
    _write_minimal_benchmark_inputs(tmp_path)

    result = generate_factor_diagnostics(tmp_path, trade_date="2026-06-04")

    assert list(result.factor_attribution.columns) == FACTOR_ATTRIBUTION_COLUMNS
    assert list(result.benchmark_diagnostics.columns) == BENCHMARK_DIAGNOSTICS_COLUMNS
    assert list(result.guardrail_impact.columns) == GUARDRAIL_IMPACT_COLUMNS
    assert (tmp_path / "factor_attribution_20260604.csv").exists()
    assert (tmp_path / "factor_attribution_summary_20260604.csv").exists()
    assert (tmp_path / "benchmark_diagnostics_20260604.csv").exists()
    assert (tmp_path / "guardrail_impact_20260604.csv").exists()
    assert "DATA_INSUFFICIENT" in set(result.factor_attribution["data_quality_warning"].dropna())
    assert "no paper_trades.csv" in result.warning


def test_factor_diagnostics_missing_factor_columns_do_not_crash(tmp_path: Path) -> None:
    pd.DataFrame([{"trade_date": "2026-06-04", "stock_id": "2330", "stock_name": "台積電"}]).to_csv(
        tmp_path / "candidates_20260604.csv",
        index=False,
        encoding="utf-8-sig",
    )
    _write_minimal_benchmark_inputs(tmp_path)

    result = generate_factor_diagnostics(tmp_path, trade_date="2026-06-04")

    missing_total_score = result.factor_attribution[
        (result.factor_attribution["factor_name"] == "total_score")
        & (result.factor_attribution["bucket"] == "MISSING")
    ]
    assert not missing_total_score.empty
    assert missing_total_score.iloc[0]["conclusion"] == "DATA_INSUFFICIENT"


def test_factor_diagnostics_marks_small_samples_insufficient(tmp_path: Path) -> None:
    _write_minimal_candidates(tmp_path)
    _write_minimal_benchmark_inputs(tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-04",
                "stock_id": "2330",
                "stock_name": "台積電",
                "status": "CLOSED",
                "realized_pnl_pct_after_cost": 0.03,
                "realized_pnl_after_cost": 3000,
                "holding_days": 3,
                "entry_price_source": "OPEN",
            }
        ]
    ).to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8-sig")

    result = generate_factor_diagnostics(tmp_path, trade_date="2026-06-04")

    traded_rows = result.factor_attribution[result.factor_attribution["trade_count"] > 0]
    assert not traded_rows.empty
    assert set(traded_rows["data_quality_warning"]) == {"DATA_INSUFFICIENT"}
    assert traded_rows["notes"].str.contains("trade_count < 20").any()


def test_guardrail_impact_schema_with_rejected_orders(tmp_path: Path) -> None:
    _write_minimal_candidates(tmp_path)
    _write_minimal_benchmark_inputs(tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-04",
                "stock_id": "2330",
                "rejected_reason": "market_regime_score 低於門檻",
            }
        ]
    ).to_csv(tmp_path / "rejected_paper_orders_20260604.csv", index=False, encoding="utf-8-sig")

    result = generate_factor_diagnostics(tmp_path, trade_date="2026-06-04")

    assert list(result.guardrail_impact.columns) == GUARDRAIL_IMPACT_COLUMNS
    assert result.guardrail_impact.iloc[0]["rejected_count"] == 1
    assert "DATA_INSUFFICIENT" in result.guardrail_impact.iloc[0]["notes"]


def test_benchmark_diagnostics_uses_0050_fallback_with_warning(tmp_path: Path) -> None:
    _write_minimal_candidates(tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-04",
                "total_capital": 1_000_000,
                "total_equity_after_cost": 1_020_000,
            }
        ]
    ).to_csv(tmp_path / "paper_summary_20260604.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {"trade_date": "2026-06-03", "total_equity_after_cost": 1_000_000},
            {"trade_date": "2026-06-04", "total_equity_after_cost": 1_020_000},
        ]
    ).to_csv(tmp_path / "daily_summary_20260604.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-04",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.01,
                "stock_return_20d": 0.02,
            }
        ]
    ).to_csv(tmp_path / "sector_strength.csv", index=False, encoding="utf-8-sig")

    result = generate_factor_diagnostics(tmp_path, trade_date="2026-06-04")

    row = result.benchmark_diagnostics.iloc[0]
    assert row["benchmark_source"] == "0050 fallback"
    assert "fallback" in row["benchmark_warning"]


def _write_minimal_candidates(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-04",
                "stock_id": "2330",
                "stock_name": "台積電",
                "total_score": 80,
                "multi_factor_score": 75,
                "final_market_score": 70,
                "confidence_score": 90,
                "liquidity_score": 80,
                "sector_strength_score": 60,
                "fundamental_score": 70,
                "valuation_score": 55,
                "financial_score": 65,
                "institutional_score": 50,
                "event_risk_score": 80,
                "candidate_grade": "B",
                "risk_pass": 1,
                "event_risk_level": "NONE",
                "is_attention_stock": False,
                "is_disposition_stock": False,
            }
        ]
    ).to_csv(path / "candidates_20260604.csv", index=False, encoding="utf-8-sig")


def _write_minimal_benchmark_inputs(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-04",
                "market_regime_score": 65,
                "source": "index",
                "market_return_5d": 0.01,
                "market_return_20d": 0.02,
            }
        ]
    ).to_csv(path / "market_regime_20260604.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-04",
                "market_regime_score": 65,
                "fallback_used": False,
            }
        ]
    ).to_csv(path / "market_recap_20260604.csv", index=False, encoding="utf-8-sig")
