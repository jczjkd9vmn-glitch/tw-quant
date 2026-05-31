from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from scripts.generate_html_report import generate_html_report
from tw_quant.reporting.anysearch_industry_research import generate_anysearch_industry_research_report
from tw_quant.workflow.daily import run_all_daily


def test_anysearch_disabled_workflow_skips_without_error(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ANYSEARCH_ENABLED", raising=False)
    monkeypatch.delenv("ANYSEARCH_API_KEY", raising=False)

    result = run_all_daily(
        config_path=_config(tmp_path),
        trade_date="20260508",
        reports_dir=tmp_path / "reports",
        run_daily_func=_fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        execute_func=_fake_execute,
        update_func=_fake_update,
        validation_func=_fake_validation,
        decision_func=_fake_decisions,
    )

    assert result.summary.status == "OK"
    assert any("anysearch_industry_research SKIPPED disabled" in message for message in result.messages)


def test_missing_api_key_skips_without_calling_client(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ANYSEARCH_ENABLED", "true")
    monkeypatch.delenv("ANYSEARCH_API_KEY", raising=False)
    _write_anysearch_config(tmp_path, enabled=False)
    _write_priority(tmp_path / "reports")
    client = _FakeAnySearchClient(raise_on_call=True)

    result = generate_anysearch_industry_research_report(
        reports_dir=tmp_path / "reports",
        config_path=tmp_path / "config" / "anysearch.yml",
        cache_dir=tmp_path / "data" / "cache" / "anysearch",
        client=client,
    )

    assert result.status == "SKIPPED"
    assert result.warning == "missing ANYSEARCH_API_KEY"
    assert client.calls == []
    assert result.output_path.exists()


def test_anysearch_enabled_env_true_overrides_config_false(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ANYSEARCH_ENABLED", "true")
    monkeypatch.setenv("ANYSEARCH_API_KEY", "present")
    _write_anysearch_config(tmp_path, enabled=False)
    _write_priority(tmp_path / "reports")
    client = _FakeAnySearchClient()

    result = generate_anysearch_industry_research_report(
        reports_dir=tmp_path / "reports",
        config_path=tmp_path / "config" / "anysearch.yml",
        cache_dir=tmp_path / "data" / "cache" / "anysearch",
        client=client,
    )

    assert result.status == "OK"
    assert result.api_calls == 1
    assert len(client.calls) == 1


def test_anysearch_enabled_env_false_overrides_config_true(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ANYSEARCH_ENABLED", "false")
    monkeypatch.setenv("ANYSEARCH_API_KEY", "present")
    _write_anysearch_config(tmp_path, enabled=True)
    _write_priority(tmp_path / "reports")
    client = _FakeAnySearchClient(raise_on_call=True)

    result = generate_anysearch_industry_research_report(
        reports_dir=tmp_path / "reports",
        config_path=tmp_path / "config" / "anysearch.yml",
        cache_dir=tmp_path / "data" / "cache" / "anysearch",
        client=client,
    )

    assert result.status == "SKIPPED"
    assert result.warning == "disabled"
    assert client.calls == []


def test_only_high_priority_is_processed_and_candidates_csv_is_written(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ANYSEARCH_ENABLED", raising=False)
    monkeypatch.setenv("ANYSEARCH_API_KEY", "present")
    _write_anysearch_config(tmp_path, enabled=True)
    _write_priority(tmp_path / "reports")
    client = _FakeAnySearchClient()

    result = generate_anysearch_industry_research_report(
        reports_dir=tmp_path / "reports",
        config_path=tmp_path / "config" / "anysearch.yml",
        cache_dir=tmp_path / "data" / "cache" / "anysearch",
        client=client,
    )

    exported = pd.read_csv(result.output_path, dtype={"stock_id": str})
    assert result.status == "OK"
    assert result.api_calls == 1
    assert client.calls == ["00988A 主動統一全球創新 ETF ETN 追蹤指數 基金 產業分類 官方"]
    assert list(exported["stock_id"]) == ["00988A"]
    assert set(exported["status"]) <= {"PENDING_REVIEW", "NEEDS_MANUAL_CHECK"}


def test_anysearch_research_does_not_modify_stock_industry_map(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ANYSEARCH_ENABLED", raising=False)
    monkeypatch.setenv("ANYSEARCH_API_KEY", "present")
    _write_anysearch_config(tmp_path, enabled=True)
    _write_priority(tmp_path / "reports")
    map_path = tmp_path / "data" / "reference" / "stock_industry_map.csv"
    map_path.parent.mkdir(parents=True)
    original = "stock_id,stock_name,industry,sub_industry,market_type,source,updated_at\n2330,台積電,半導體,晶圓代工,TWSE,manual,2026-05-01\n"
    map_path.write_text(original, encoding="utf-8")

    generate_anysearch_industry_research_report(
        reports_dir=tmp_path / "reports",
        config_path=tmp_path / "config" / "anysearch.yml",
        cache_dir=tmp_path / "data" / "cache" / "anysearch",
        client=_FakeAnySearchClient(),
    )

    assert map_path.read_text(encoding="utf-8") == original


def test_anysearch_cache_prevents_repeated_api_calls(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ANYSEARCH_ENABLED", raising=False)
    monkeypatch.setenv("ANYSEARCH_API_KEY", "present")
    _write_anysearch_config(tmp_path, enabled=True)
    _write_priority(tmp_path / "reports")
    client = _FakeAnySearchClient()
    cache_dir = tmp_path / "data" / "cache" / "anysearch"

    first = generate_anysearch_industry_research_report(
        reports_dir=tmp_path / "reports",
        config_path=tmp_path / "config" / "anysearch.yml",
        cache_dir=cache_dir,
        client=client,
    )
    second = generate_anysearch_industry_research_report(
        reports_dir=tmp_path / "reports",
        config_path=tmp_path / "config" / "anysearch.yml",
        cache_dir=cache_dir,
        client=client,
    )

    assert first.api_calls == 1
    assert first.cache_hits == 0
    assert second.api_calls == 0
    assert second.cache_hits == 1
    assert len(client.calls) == 1


def test_html_contains_anysearch_candidate_section(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("scripts.generate_html_report.ROOT", tmp_path)
    (tmp_path / "config.yaml").write_text("database:\n  url: sqlite:///:memory:\n", encoding="utf-8")
    reports = tmp_path / "reports"
    reports.mkdir()
    pd.DataFrame(
        [
            {
                "stock_id": "00988A",
                "stock_name": "主動統一全球創新",
                "query": "00988A 主動統一全球創新 ETF ETN 追蹤指數 基金 產業分類 官方",
                "proposed_market_type": "ETF",
                "proposed_industry": "ETF",
                "proposed_sub_industry": "全球創新 ETF",
                "source_title": "00988A 主動統一全球創新 ETF",
                "source_url": "https://www.twse.com.tw/",
                "source_type": "twse",
                "confidence": 0.8,
                "reason": "來源顯示為 ETF；需人工確認後才可寫入正式產業分類。",
                "status": "PENDING_REVIEW",
                "checked_at": "2026-05-31T00:00:00+00:00",
            }
        ]
    ).to_csv(reports / "anysearch_industry_candidates.csv", index=False, encoding="utf-8-sig")

    html = generate_html_report(reports, docs_dir=None).read_text(encoding="utf-8")

    assert "AnySearch 產業分類候選資料" in html
    assert "此為候選資料，需人工確認後才可寫入正式產業分類。" in html


class _FakeAnySearchClient:
    def __init__(self, *, raise_on_call: bool = False) -> None:
        self.raise_on_call = raise_on_call
        self.calls: list[str] = []

    def search(self, query: str, max_results: int = 5):
        if self.raise_on_call:
            raise AssertionError("AnySearch API should not be called")
        self.calls.append(query)
        return [
            {
                "title": "00988A 主動統一全球創新 ETF",
                "url": "https://www.twse.com.tw/zh/ETF",
                "snippet": "ETF 基金 追蹤 全球創新主題。",
            }
        ]


def _write_anysearch_config(base: Path, *, enabled: bool) -> None:
    path = base / "config" / "anysearch.yml"
    path.parent.mkdir(parents=True)
    path.write_text(
        "anysearch:\n"
        f"  enabled: {'true' if enabled else 'false'}\n"
        "  max_requests_per_run: 30\n"
        "  cache_days: 14\n"
        "  timeout_seconds: 15\n"
        "  retry_count: 2\n"
        "  only_priority_levels:\n"
        "    - HIGH\n"
        "  write_mode: proposal_only\n",
        encoding="utf-8",
    )


def _write_priority(reports_dir: Path) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "stock_id": "00988A",
                "stock_name": "主動統一全球創新",
                "priority_level": "HIGH",
                "priority_score": 17,
            },
            {
                "stock_id": "00400A",
                "stock_name": "主動國泰動能高息",
                "priority_level": "LOW",
                "priority_score": 3,
            },
        ]
    ).to_csv(reports_dir / "missing_industry_priority.csv", index=False, encoding="utf-8-sig")


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
