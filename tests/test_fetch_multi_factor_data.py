from __future__ import annotations

from pathlib import Path

import pandas as pd

import scripts.fetch_multi_factor_data as module
from tw_quant.data_sources.base import ProviderResult
from tw_quant.data_sources.mops_provider import MATERIAL_EVENT_COLUMNS, MONTHLY_REVENUE_COLUMNS
from tw_quant.data_sources.twse_provider import ATTENTION_DISPOSITION_COLUMNS, INSTITUTIONAL_COLUMNS, MARGIN_SHORT_COLUMNS


class EmptyMOPSProvider:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir

    def fetch_monthly_revenue(self, trade_date: str) -> ProviderResult:
        return ProviderResult("monthly_revenue", pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS), "EMPTY", "provider returned no rows")

    def fetch_material_events(self, trade_date: str) -> ProviderResult:
        return ProviderResult("material_events", pd.DataFrame(columns=MATERIAL_EVENT_COLUMNS), "EMPTY", "provider returned no rows")


class FailedMOPSProvider(EmptyMOPSProvider):
    def fetch_monthly_revenue(self, trade_date: str) -> ProviderResult:
        return ProviderResult("monthly_revenue", pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS), "FAILED", "provider failed", "boom")


class EmptyTWSEProvider:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir

    def fetch_institutional(self, trade_date: str) -> ProviderResult:
        return ProviderResult("institutional", pd.DataFrame(columns=INSTITUTIONAL_COLUMNS), "EMPTY", "provider returned no rows")

    def fetch_margin_short(self, trade_date: str) -> ProviderResult:
        return ProviderResult("margin_short", pd.DataFrame(columns=MARGIN_SHORT_COLUMNS), "EMPTY", "provider returned no rows")

    def fetch_attention_disposition(self, trade_date: str) -> ProviderResult:
        return ProviderResult(
            "attention_disposition",
            pd.DataFrame(columns=ATTENTION_DISPOSITION_COLUMNS),
            "EMPTY",
            "provider returned no rows",
        )


def _patch_dirs_and_providers(tmp_path: Path, monkeypatch, mops_provider=EmptyMOPSProvider) -> tuple[Path, Path]:
    data_dir = tmp_path / "data"
    reports_dir = tmp_path / "reports"
    monkeypatch.setattr(module, "DATA_DIR", data_dir)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "MOPSProvider", mops_provider)
    monkeypatch.setattr(module, "TWSEProvider", EmptyTWSEProvider)
    return data_dir, reports_dir


def test_generate_required_csv_and_status_when_sources_missing(tmp_path: Path, monkeypatch) -> None:
    data_dir, reports_dir = _patch_dirs_and_providers(tmp_path, monkeypatch)

    status = module.run_fetch_multi_factor_data(as_of="20260515")

    assert set(status["source_name"]) == {
        "monthly_revenue",
        "valuation",
        "financials",
        "material_events",
        "institutional",
        "margin_short",
        "attention_disposition",
        "sector_strength",
        "liquidity",
    }
    assert "provider_maturity" in status.columns
    assert all((data_dir / name).exists() for name in [
        "monthly_revenue.csv",
        "valuation.csv",
        "financials.csv",
        "material_events.csv",
        "institutional.csv",
        "margin_short.csv",
        "attention_disposition.csv",
        "sector_strength.csv",
        "liquidity.csv",
    ])
    report_files = list(reports_dir.glob("data_fetch_status_*.csv"))
    assert len(report_files) == 1


def test_provider_failed_keeps_existing_csv(tmp_path: Path, monkeypatch) -> None:
    data_dir, _reports_dir = _patch_dirs_and_providers(tmp_path, monkeypatch, FailedMOPSProvider)
    data_dir.mkdir(parents=True)
    existing = pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "TSMC",
                "year_month": "202605",
                "revenue": 1,
                "revenue_yoy": 1,
                "revenue_mom": 1,
                "accumulated_revenue": 1,
                "accumulated_revenue_yoy": 1,
            }
        ]
    )
    existing_path = data_dir / "monthly_revenue.csv"
    existing.to_csv(existing_path, index=False)
    before = existing_path.read_text(encoding="utf-8")

    status = module.run_fetch_multi_factor_data(as_of="20260515")

    row = status[status["source_name"] == "monthly_revenue"].iloc[0]
    after = existing_path.read_text(encoding="utf-8")
    assert row["status"] == "OK_WITH_FALLBACK"
    assert row["rows"] == 1
    assert "provider failed, kept existing csv" in row["warning"]
    assert "保留既有 CSV" in row["warning"]
    assert after == before


def test_provider_empty_keeps_existing_csv(tmp_path: Path, monkeypatch) -> None:
    data_dir, _reports_dir = _patch_dirs_and_providers(tmp_path, monkeypatch)
    data_dir.mkdir(parents=True)
    existing = pd.DataFrame(
        [
            {
                "event_date": "2026-05-15",
                "stock_id": "2330",
                "stock_name": "TSMC",
                "event_type": "material_event",
                "event_title": "local event",
                "event_risk_level": "LOW",
                "event_keywords": "",
                "event_warning": "",
            }
        ]
    )
    existing_path = data_dir / "material_events.csv"
    existing.to_csv(existing_path, index=False)
    before = existing_path.read_text(encoding="utf-8")

    status = module.run_fetch_multi_factor_data(as_of="20260515")

    row = status[status["source_name"] == "material_events"].iloc[0]
    after = existing_path.read_text(encoding="utf-8")
    assert row["status"] == "OK_WITH_FALLBACK"
    assert row["rows"] == 1
    assert "provider empty, kept existing csv" in row["warning"]
    assert "保留既有 CSV" in row["warning"]
    assert after == before


def test_provider_success_with_new_rows_is_ok(tmp_path: Path, monkeypatch) -> None:
    class SuccessMOPSProvider(EmptyMOPSProvider):
        def fetch_monthly_revenue(self, trade_date: str) -> ProviderResult:
            return ProviderResult(
                "monthly_revenue",
                pd.DataFrame(
                    [
                        {
                            "stock_id": "2330",
                            "stock_name": "TSMC",
                            "year_month": "202605",
                            "revenue": 1,
                            "revenue_yoy": 1,
                            "revenue_mom": 1,
                            "accumulated_revenue": 1,
                            "accumulated_revenue_yoy": 1,
                        }
                    ]
                ),
                "OK",
            )

    data_dir, _reports_dir = _patch_dirs_and_providers(tmp_path, monkeypatch, SuccessMOPSProvider)

    status = module.run_fetch_multi_factor_data(as_of="20260515")

    row = status[status["source_name"] == "monthly_revenue"].iloc[0]
    assert row["status"] == "OK"
    assert row["fallback_action"] == "wrote_new_data"
    assert row["rows"] == 1
    assert len(pd.read_csv(data_dir / "monthly_revenue.csv")) == 1


def test_missing_existing_csv_writes_empty_schema_only(tmp_path: Path, monkeypatch) -> None:
    data_dir, _reports_dir = _patch_dirs_and_providers(tmp_path, monkeypatch)

    status = module.run_fetch_multi_factor_data(as_of="20260515")

    row = status[status["source_name"] == "monthly_revenue"].iloc[0]
    output = pd.read_csv(data_dir / "monthly_revenue.csv")
    assert row["status"] == "EMPTY"
    assert row["rows"] == 0
    assert "no existing csv, wrote empty schema" in row["warning"]
    assert output.empty
    assert list(output.columns) == MONTHLY_REVENUE_COLUMNS


def test_empty_existing_data_is_not_fake_data(tmp_path: Path, monkeypatch) -> None:
    data_dir, _reports_dir = _patch_dirs_and_providers(tmp_path, monkeypatch)
    data_dir.mkdir(parents=True)
    pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS).to_csv(data_dir / "monthly_revenue.csv", index=False)

    status = module.run_fetch_multi_factor_data(as_of="20260515")

    row = status[status["source_name"] == "monthly_revenue"].iloc[0]
    assert row["status"] == "EMPTY"
    assert row["rows"] == 0


def test_status_contains_expected_states(tmp_path: Path, monkeypatch) -> None:
    _patch_dirs_and_providers(tmp_path, monkeypatch)

    status = module.run_fetch_multi_factor_data(as_of="20260515")

    assert set(status["status"]).issubset({"OK", "EMPTY", "MISSING", "FAILED"})
