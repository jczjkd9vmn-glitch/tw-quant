from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report
from tw_quant.config import load_config
from tw_quant.reporting.market_regime_threshold_optimizer import (
    generate_market_regime_threshold_optimization,
)


def test_threshold_optimizer_allows_or_blocks_score_53_by_threshold(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")
    frame = result.frame
    threshold_60 = _row(frame, 60)
    threshold_50 = _row(frame, 50)

    assert threshold_60["would_allow_new_entries"] is False
    assert threshold_50["would_allow_new_entries"] is True
    assert threshold_60["blocked_candidate_count"] != threshold_50["blocked_candidate_count"]
    assert threshold_60["data_sufficiency_status"] == "OBSERVATION_ONLY"
    assert (tmp_path / "market_regime_threshold_optimization_20260610.csv").exists()


def test_threshold_optimizer_marks_data_insufficient_without_forward_returns(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path, include_forward_returns=False)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")

    assert set(result.frame["data_sufficiency_status"]) == {"DATA_INSUFFICIENT"}
    assert set(result.frame["recommendation"]) == {"DATA_INSUFFICIENT"}
    assert result.status == "DATA_INSUFFICIENT"


def test_threshold_optimizer_walk_forward_uses_train_for_recommendation(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")
    frame = result.frame
    threshold_50 = _row(frame, 50)
    threshold_60 = _row(frame, 60)

    assert threshold_50["walk_forward_split"] == "train_first_70pct_validation_last_30pct"
    assert threshold_50["train_estimated_excess_return"] < threshold_60["train_estimated_excess_return"]
    assert threshold_50["validation_estimated_excess_return"] > threshold_60["validation_estimated_excess_return"]
    assert threshold_50["recommendation"] != "CONSIDER_LOWERING"


def test_threshold_optimizer_outputs_dynamic_exposure_proxy(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")
    dynamic = _row(result.frame, "DYNAMIC_EXPOSURE")

    assert dynamic["threshold"] == "DYNAMIC_EXPOSURE"
    assert dynamic["dynamic_exposure_pct"] > 0
    assert "score<45:0%" in dynamic["notes"]


def test_html_report_shows_market_regime_threshold_optimization(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path)
    generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "Market Regime 門檻最佳化觀察" in html
    assert "Observation only / proxy 診斷" in html
    assert "目前正式門檻" in html
    assert "Dynamic exposure proxy" in html


def test_formal_market_regime_threshold_remains_configured_at_60() -> None:
    config = load_config()

    assert config["market_regime"]["min_score_for_new_entries"] == 60


def _row(frame: pd.DataFrame, threshold: object) -> dict[str, object]:
    matches = frame[frame["threshold"].astype(str) == str(threshold)]
    assert not matches.empty
    return matches.iloc[0].to_dict()


def _write_optimizer_reports(tmp_path: Path, *, include_forward_returns: bool = True) -> None:
    start = date(2026, 6, 1)
    scores = [45, 50, 60, 65, 70, 52, 58, 50, 70, 53]
    forward_returns = [0.0, -0.10, 0.05, 0.04, 0.03, -0.08, -0.07, 0.40, 0.05, 0.50]
    for index, score in enumerate(scores):
        trade_date = start + timedelta(days=index)
        label = trade_date.strftime("%Y%m%d")
        date_text = trade_date.isoformat()
        pd.DataFrame(
            [
                {
                    "trade_date": date_text,
                    "market_regime_score": score,
                    "candidate_rows": 1,
                    "new_entries_allowed": score >= 60,
                    "guardrail_status": "OK" if score >= 60 else "BLOCKED",
                }
            ]
        ).to_csv(tmp_path / f"daily_summary_{label}.csv", index=False, encoding="utf-8")
        pd.DataFrame(
            [
                {
                    "trade_date": date_text,
                    "market_regime_score": score,
                    "market_return_5d": 0.0,
                    "market_return_20d": 0.0,
                }
            ]
        ).to_csv(tmp_path / f"market_regime_{label}.csv", index=False, encoding="utf-8")
        candidate = {
            "trade_date": date_text,
            "stock_id": "2330",
            "stock_name": "台積電",
            "total_score": 80,
        }
        if include_forward_returns:
            candidate["forward_return_5d"] = forward_returns[index] / 2
            candidate["forward_return_20d"] = forward_returns[index]
        pd.DataFrame([candidate]).to_csv(tmp_path / f"candidates_{label}.csv", index=False, encoding="utf-8")
        pd.DataFrame(
            [
                {
                    "trade_date": date_text,
                    "benchmark_return_5d": 0.0,
                    "benchmark_return_20d": 0.0,
                }
            ]
        ).to_csv(tmp_path / f"benchmark_diagnostics_{label}.csv", index=False, encoding="utf-8")
        pd.DataFrame(
            [
                {
                    "trade_date": date_text,
                    "cash": 900_000,
                    "total_equity_after_cost": 1_000_000,
                }
            ]
        ).to_csv(tmp_path / f"paper_summary_{label}.csv", index=False, encoding="utf-8")
        if score < 60:
            pd.DataFrame(
                [
                    {
                        "trade_date": date_text,
                        "stock_id": "2330",
                        "rejection_reason": f"market_regime_score {score} 低於新增持倉門檻 60",
                    }
                ]
            ).to_csv(tmp_path / f"rejected_paper_orders_{label}.csv", index=False, encoding="utf-8")
