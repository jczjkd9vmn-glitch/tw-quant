from __future__ import annotations

from pathlib import Path

import pandas as pd

import scripts.fetch_multi_factor_data as module


def test_generate_required_csv_and_status_when_sources_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(module, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    status = module.run_fetch_multi_factor_data()

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
    assert all((tmp_path / "data" / name).exists() for name in [
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
    report_files = list((tmp_path / "reports").glob("data_fetch_status_*.csv"))
    assert len(report_files) == 1


def test_fetch_failed_but_existing_data_is_ok_status(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    reports_dir = tmp_path / "reports"
    data_dir.mkdir(parents=True)
    pd.DataFrame([{"stock_id": "2330", "stock_name": "台積電", "year_month": "202605", "revenue": 1, "revenue_yoy": 1, "revenue_mom": 1, "accumulated_revenue": 1, "accumulated_revenue_yoy": 1}]).to_csv(data_dir / "monthly_revenue.csv", index=False)

    monkeypatch.setattr(module, "DATA_DIR", data_dir)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    status = module.run_fetch_multi_factor_data()
    row = status[status["source_name"] == "monthly_revenue"].iloc[0]
    assert row["status"] == "OK"
    assert row["rows"] == 1
    assert "fallback" in row["warning"]


def test_empty_existing_data_is_not_fake_data(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    reports_dir = tmp_path / "reports"
    data_dir.mkdir(parents=True)
    pd.DataFrame(columns=[
        "stock_id",
        "stock_name",
        "year_month",
        "revenue",
        "revenue_yoy",
        "revenue_mom",
        "accumulated_revenue",
        "accumulated_revenue_yoy",
    ]).to_csv(data_dir / "monthly_revenue.csv", index=False)

    monkeypatch.setattr(module, "DATA_DIR", data_dir)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    status = module.run_fetch_multi_factor_data()
    row = status[status["source_name"] == "monthly_revenue"].iloc[0]
    assert row["status"] == "EMPTY"
    assert row["rows"] == 0


def test_status_contains_missing_ok_or_empty(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(module, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    status = module.run_fetch_multi_factor_data()

    assert set(status["status"]).issubset({"OK", "EMPTY", "MISSING", "FAILED"})
