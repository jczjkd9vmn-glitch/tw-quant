from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from scripts.generate_html_report import generate_html_report
from tw_quant.reporting.missing_industry_priority import (
    generate_missing_industry_priority_report,
    priority_level_for_score,
)
from tw_quant.workflow.daily import run_all_daily


def test_missing_industry_priority_report_generates_only_fallback_rows(tmp_path: Path) -> None:
    _write_priority_inputs(tmp_path)

    result = generate_missing_industry_priority_report(
        data_dir=tmp_path / "data",
        reports_dir=tmp_path / "reports",
        trade_date="2026-05-29",
    )

    assert result.output_path is not None and result.output_path.exists()
    exported = pd.read_csv(result.output_path, dtype={"stock_id": str})
    assert list(exported["stock_id"]) == ["1001", "3003", "4004"]
    assert set(exported["latest_relative_mode"]) == {"market_relative_fallback"}
    assert "2002" not in set(exported["stock_id"])


def test_candidates_and_decisions_raise_priority_score(tmp_path: Path) -> None:
    _write_priority_inputs(tmp_path)

    result = generate_missing_industry_priority_report(
        data_dir=tmp_path / "data",
        reports_dir=tmp_path / "reports",
        trade_date="2026-05-29",
    )
    frame = result.priority.set_index("stock_id")

    assert frame.loc["1001", "appear_in_candidates_count"] == 1
    assert frame.loc["1001", "appear_in_trading_decisions_count"] == 1
    assert frame.loc["1001", "priority_score"] > frame.loc["4004", "priority_score"]
    assert frame.loc["1001", "priority_level"] == "HIGH"
    assert frame.loc["3003", "priority_level"] == "MEDIUM"
    assert frame.loc["4004", "priority_level"] == "LOW"


def test_priority_level_thresholds() -> None:
    assert priority_level_for_score(8) == "HIGH"
    assert priority_level_for_score(4) == "MEDIUM"
    assert priority_level_for_score(3) == "LOW"


def test_html_contains_missing_industry_priority_section(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("scripts.generate_html_report.ROOT", tmp_path)
    (tmp_path / "config.yaml").write_text("database:\n  url: sqlite:///:memory:\n", encoding="utf-8")
    reports = tmp_path / "reports"
    reports.mkdir()
    pd.DataFrame(
        [
            {
                "stock_id": "1001",
                "stock_name": "缺產業測試",
                "market_type": "UNKNOWN",
                "latest_relative_mode": "market_relative_fallback",
                "fallback_reason": "缺少產業分類，使用全市場相對強弱",
                "appear_in_candidates_count": 1,
                "appear_in_trading_decisions_count": 1,
                "appear_in_risk_pass_count": 0,
                "appear_in_position_review_count": 0,
                "appear_in_ai_enrichment_count": 0,
                "recent_appearance_count": 2,
                "liquidity_score": 92,
                "avg_volume": 100000,
                "turnover_value": 2500000,
                "last_seen_date": "2026-05-29",
                "priority_score": 14,
                "priority_level": "HIGH",
                "suggested_action": "優先查證並補產業分類",
            }
        ]
    ).to_csv(reports / "missing_industry_priority.csv", index=False, encoding="utf-8-sig")

    html = generate_html_report(reports, docs_dir=None).read_text(encoding="utf-8")

    assert "缺產業分類優先補資料清單" in html
    assert "優先查證並補產業分類" in html


def test_run_all_daily_reports_missing_industry_priority_message(tmp_path: Path) -> None:
    output_path = tmp_path / "reports" / "missing_industry_priority.csv"

    def fake_missing_priority(*_args, **_kwargs):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame({"stock_id": ["1001"], "priority_level": ["HIGH"]})
        frame.to_csv(output_path, index=False)
        return SimpleNamespace(priority=frame, output_path=output_path, warning="")

    result = run_all_daily(
        config_path=_config(tmp_path),
        trade_date="20260508",
        capital=1_000_000,
        reports_dir=tmp_path / "reports",
        run_daily_func=_fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        execute_func=_fake_execute,
        update_func=_fake_update,
        validation_func=_fake_validation,
        decision_func=_fake_decisions,
        missing_industry_priority_func=fake_missing_priority,
    )

    assert any("missing_industry_priority OK rows=1 high_priority=1" in message for message in result.messages)


def _write_priority_inputs(base: Path) -> None:
    data_dir = base / "data"
    reports_dir = base / "reports"
    data_dir.mkdir()
    reports_dir.mkdir()
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-29",
                "stock_id": "1001",
                "stock_name": "高優先",
                "sector_strength_mode": "market_relative_fallback",
                "sector_strength_warning": "缺少產業分類，使用全市場相對強弱",
            },
            {
                "trade_date": "2026-05-29",
                "stock_id": "2002",
                "stock_name": "已有分類",
                "sector_strength_mode": "industry_relative",
                "sector_strength_warning": "",
            },
            {
                "trade_date": "2026-05-29",
                "stock_id": "3003",
                "stock_name": "中優先",
                "sector_strength_mode": "market_relative_fallback",
                "sector_strength_warning": "缺少產業分類，使用全市場相對強弱",
            },
            {
                "trade_date": "2026-05-29",
                "stock_id": "4004",
                "stock_name": "低優先",
                "sector_strength_mode": "market_relative_fallback",
                "sector_strength_warning": "缺少產業分類，使用全市場相對強弱",
            },
        ]
    ).to_csv(data_dir / "sector_strength.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {"trade_date": "2026-05-29", "stock_id": "1001", "stock_name": "高優先", "liquidity_score": 92, "avg_volume_20d": 100000, "latest_turnover": 3000000},
            {"trade_date": "2026-05-29", "stock_id": "3003", "stock_name": "中優先", "liquidity_score": 90, "avg_volume_20d": 90000, "latest_turnover": 2000000},
            {"trade_date": "2026-05-29", "stock_id": "4004", "stock_name": "低優先", "liquidity_score": 20, "avg_volume_20d": 1000, "latest_turnover": 10000},
        ]
    ).to_csv(data_dir / "liquidity.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame([{"trade_date": "2026-05-29", "stock_id": "1001", "stock_name": "高優先"}]).to_csv(
        reports_dir / "candidates_20260529.csv", index=False, encoding="utf-8-sig"
    )
    pd.DataFrame([{"trade_date": "2026-05-29", "stock_id": "1001", "stock_name": "高優先"}]).to_csv(
        reports_dir / "trading_decisions_20260529.csv", index=False, encoding="utf-8-sig"
    )
    pd.DataFrame([{"trade_date": "2026-05-29", "stock_id": "3003", "stock_name": "中優先"}]).to_csv(
        reports_dir / "ai_enrichment_20260529.csv", index=False, encoding="utf-8-sig"
    )


def _config(tmp_path: Path) -> str:
    path = tmp_path / "config.yaml"
    path.write_text("database:\n  url: sqlite:///:memory:\n", encoding="utf-8")
    return str(path)


def _fake_run_daily(**_kwargs):
    return SimpleNamespace(
        trade_date=pd.Timestamp("2026-05-08"),
        fetched_rows=0,
        scored_rows=10,
        candidate_rows=2,
        message="",
    )


def _fake_export(*_args, **_kwargs):
    return SimpleNamespace(
        trade_date=pd.Timestamp("2026-05-08"),
        candidates=pd.DataFrame({"stock_id": ["1001", "1002"]}),
        risk_pass_candidates=pd.DataFrame({"stock_id": ["1001"]}),
        data_fetch_status=pd.DataFrame(),
        warning="",
    )


def _fake_paper(*_args, **_kwargs):
    return SimpleNamespace(
        positions=pd.DataFrame({"stock_id": ["1001"]}),
        pending_orders=pd.DataFrame({"stock_id": ["1001"], "status": ["PENDING"]}),
        rejected_orders=pd.DataFrame(),
        market_regime_score=80,
        new_entries_allowed=True,
        guardrail_status="OK",
        pause_new_entries_reason="",
        warning="",
    )


def _fake_execute(*_args, **_kwargs):
    return SimpleNamespace(
        pending_orders=pd.DataFrame({"stock_id": ["1001"], "status": ["PENDING"]}),
        executed_orders=pd.DataFrame(),
        skipped_orders=pd.DataFrame(),
        rejected_orders=pd.DataFrame(),
        warnings=[],
    )


def _fake_update(*_args, **_kwargs):
    return SimpleNamespace(
        summary=pd.DataFrame(
            [
                {
                    "open_positions": 1,
                    "closed_positions": 0,
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "total_equity": 1_000_000.0,
                }
            ]
        ),
        warning="",
    )


def _fake_validation(*_args, **_kwargs):
    return SimpleNamespace(validation=pd.DataFrame(), warning="")


def _fake_decisions(*_args, **_kwargs):
    return SimpleNamespace(decisions=pd.DataFrame({"decision": ["WATCH_ONLY"], "candidate_grade": ["B"]}), warning="")
