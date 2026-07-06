from __future__ import annotations

from datetime import date, timedelta
from html import unescape
from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report
from scripts.market_regime_readiness_html import patch_generated_market_regime_readiness_html
from tw_quant.reporting.market_regime_threshold_optimizer import generate_market_regime_threshold_optimization


def test_5d_observation_is_available_when_5d_coverage_is_ready(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path, include_forward_returns=False)
    _write_candidate_forward_labels(tmp_path, include_20d=False)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")
    threshold_60 = _row(result.frame, 60)

    assert threshold_60["label_5d_coverage"] >= 0.70
    assert threshold_60["forward_return_5d_mean"] is not None
    assert threshold_60["readiness_status"] == "DATA_INSUFFICIENT_20D"


def test_20d_low_coverage_blocks_threshold_change_recommendation(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path, include_forward_returns=False)
    _write_candidate_forward_labels(tmp_path, include_20d=False)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")
    threshold_60 = _row(result.frame, 60)

    assert threshold_60["label_20d_coverage"] < 0.60
    assert bool(threshold_60["can_recommend_threshold_change"]) is False
    assert threshold_60["recommendation"] in {"DATA_INSUFFICIENT", "OBSERVATION_ONLY"}
    assert threshold_60["recommendation"] != "CONSIDER_LOWERING"


def test_low_validation_samples_force_observation_only(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path, candidates_per_day=5)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")
    threshold_60 = _row(result.frame, 60)

    assert threshold_60["validation_eligible_sample_count"] < 30
    assert threshold_60["recommendation"] in {"DATA_INSUFFICIENT", "OBSERVATION_ONLY"}
    assert bool(threshold_60["can_recommend_threshold_change"]) is False


def test_threshold_change_is_allowed_only_when_20d_and_validation_samples_are_ready(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")
    threshold_60 = _row(result.frame, 60)

    assert threshold_60["label_20d_coverage"] >= 0.60
    assert threshold_60["validation_eligible_sample_count"] >= 30
    assert threshold_60["validation_blocked_sample_count"] >= 10
    assert threshold_60["readiness_status"] == "READY_FOR_20D_OBSERVATION"
    assert bool(threshold_60["can_recommend_threshold_change"]) is True


def test_no_forward_labels_remains_data_insufficient(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path, include_forward_returns=False)

    result = generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")

    assert set(result.frame["data_sufficiency_status"]) == {"DATA_INSUFFICIENT"}
    assert set(result.frame["recommendation"]) == {"DATA_INSUFFICIENT"}
    assert set(result.frame["can_recommend_threshold_change"]) == {False}


def test_html_report_shows_readiness_status_and_reason(tmp_path: Path) -> None:
    _write_optimizer_reports(tmp_path, include_forward_returns=False)
    _write_candidate_forward_labels(tmp_path, include_20d=False)
    generate_market_regime_threshold_optimization(tmp_path, trade_date="2026-06-10")

    output_path = generate_html_report(tmp_path)
    patch_generated_market_regime_readiness_html(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "Readiness status" in html
    assert "Readiness reason" in html
    assert _t("20d &#x6a23;&#x672c;&#x4e0d;&#x8db3;") in html
    assert _t("&#x76ee;&#x524d;&#x50c5;&#x4f9b; observation-only") in html
    assert (
        _t("&#x4e0d;&#x53ef;&#x4f5c;&#x70ba;&#x6b63;&#x5f0f;&#x964d;&#x4f4e;&#x9580;&#x6abb;&#x4f9d;&#x64da;") in html
    )
    assert _t("&#x662f;&#x5426;&#x5141;&#x8a31;&#x6b63;&#x5f0f;&#x8abf;&#x6574;&#x9580;&#x6abb;") in html
    assert _t("&#x5426;") in html


def _row(frame: pd.DataFrame, threshold: object) -> dict[str, object]:
    matches = frame[frame["threshold"].astype(str) == str(threshold)]
    assert not matches.empty
    return matches.iloc[0].to_dict()


def _write_optimizer_reports(
    tmp_path: Path,
    *,
    include_forward_returns: bool = True,
    candidates_per_day: int = 30,
) -> None:
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
                    "candidate_rows": candidates_per_day,
                    "new_entries_allowed": score >= 60,
                    "guardrail_status": "OK" if score >= 60 else "BLOCKED",
                }
            ]
        ).to_csv(tmp_path / f"daily_summary_{label}.csv", index=False, encoding="utf-8")
        pd.DataFrame(
            [{"trade_date": date_text, "market_regime_score": score, "market_return_5d": 0.0, "market_return_20d": 0.0}]
        ).to_csv(tmp_path / f"market_regime_{label}.csv", index=False, encoding="utf-8")
        pd.DataFrame(
            _candidate_rows(date_text, forward_returns[index], include_forward_returns, candidates_per_day)
        ).to_csv(
            tmp_path / f"candidates_{label}.csv",
            index=False,
            encoding="utf-8",
        )
        pd.DataFrame([{"trade_date": date_text, "benchmark_return_5d": 0.0, "benchmark_return_20d": 0.0}]).to_csv(
            tmp_path / f"benchmark_diagnostics_{label}.csv", index=False, encoding="utf-8"
        )
        pd.DataFrame([{"trade_date": date_text, "cash": 900_000, "total_equity_after_cost": 1_000_000}]).to_csv(
            tmp_path / f"paper_summary_{label}.csv", index=False, encoding="utf-8"
        )
        if score < 60:
            pd.DataFrame(_rejected_rows(date_text, score, candidates_per_day)).to_csv(
                tmp_path / f"rejected_paper_orders_{label}.csv",
                index=False,
                encoding="utf-8",
            )


def _candidate_rows(
    date_text: str, forward_return: float, include_forward_returns: bool, count: int
) -> list[dict[str, object]]:
    rows = []
    for index in range(count):
        row: dict[str, object] = {
            "trade_date": date_text,
            "stock_id": f"2330{index:02d}",
            "stock_name": "TSMC",
            "total_score": 80,
        }
        if include_forward_returns:
            row["forward_return_5d"] = forward_return / 2
            row["forward_return_20d"] = forward_return
        rows.append(row)
    return rows


def _rejected_rows(date_text: str, score: int, count: int) -> list[dict[str, object]]:
    return [
        {
            "trade_date": date_text,
            "stock_id": f"2330{index:02d}",
            "rejection_reason": f"market_regime_score {score} below threshold 60",
        }
        for index in range(count)
    ]


def _write_candidate_forward_labels(tmp_path: Path, *, include_20d: bool = True) -> None:
    start = date(2026, 6, 1)
    scores = [45, 50, 60, 65, 70, 52, 58, 50, 70, 53]
    forward_returns = [0.0, -0.10, 0.05, 0.04, 0.03, -0.08, -0.07, 0.40, 0.05, 0.50]
    rows = []
    for index, score in enumerate(scores):
        trade_date = start + timedelta(days=index)
        for candidate_index in range(30):
            rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "symbol": f"2330{candidate_index:02d}",
                    "name": "TSMC",
                    "candidate_score": 80,
                    "market_regime_score": score,
                    "official_threshold": 60,
                    "blocked_by_market_regime": score < 60,
                    "close_on_trade_date": 100,
                    "close_plus_5d": 100 * (1 + forward_returns[index] / 2),
                    "close_plus_20d": 100 * (1 + forward_returns[index]) if include_20d else "",
                    "forward_return_5d": forward_returns[index] / 2,
                    "forward_return_20d": forward_returns[index] if include_20d else "",
                    "benchmark_return_5d": 0.0,
                    "benchmark_return_20d": 0.0 if include_20d else "",
                    "excess_return_5d": forward_returns[index] / 2,
                    "excess_return_20d": forward_returns[index] if include_20d else "",
                    "forward_return_5d_status": "OBSERVATION_ONLY",
                    "forward_return_20d_status": "OBSERVATION_ONLY" if include_20d else "DATA_INSUFFICIENT",
                    "data_sufficiency_status": "OBSERVATION_ONLY" if include_20d else "DATA_INSUFFICIENT",
                    "is_observation_only": True,
                }
            )
    pd.DataFrame(rows).to_csv(tmp_path / "candidate_forward_returns_20260610.csv", index=False, encoding="utf-8")


def _t(value: str) -> str:
    return unescape(value)
