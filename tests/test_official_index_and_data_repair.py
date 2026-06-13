from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report
from scripts.repair_price_data import repair_price_data
from tw_quant.reporting.benchmark import select_benchmark_snapshot
from tw_quant.reporting.data_quality import build_data_quality_health


def test_benchmark_snapshot_marks_official_index_can_judge_alpha(tmp_path: Path) -> None:
    _write_market_indices(tmp_path)

    snapshot = select_benchmark_snapshot(tmp_path, "2026-06-05")

    assert snapshot["source_label"] == "正式加權報酬指數"
    assert snapshot["benchmark_is_official"] is True
    assert snapshot["fallback_reason"] == ""
    assert snapshot["can_judge_alpha"] is True
    assert snapshot["returns"]["5d"] == 0.02


def test_benchmark_snapshot_marks_0050_fallback_cannot_judge_alpha(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.01,
                "stock_return_20d": 0.02,
            }
        ]
    ).to_csv(tmp_path / "sector_strength.csv", index=False, encoding="utf-8")

    snapshot = select_benchmark_snapshot(tmp_path, "2026-06-05")

    assert snapshot["source_label"] == "0050 fallback"
    assert snapshot["benchmark_is_official"] is False
    assert snapshot["fallback_reason"] == "missing_official_market_index"
    assert snapshot["can_judge_alpha"] is False


def test_html_fallback_benchmark_does_not_claim_beat_market(tmp_path: Path) -> None:
    _write_minimal_html_inputs(tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.01,
                "stock_return_20d": 0.02,
            }
        ]
    ).to_csv(tmp_path / "sector_strength.csv", index=False, encoding="utf-8")

    output_path = generate_html_report(tmp_path, docs_dir=tmp_path / "docs")
    html = output_path.read_text(encoding="utf-8")

    assert "benchmark_source=0050 fallback" in html
    assert "benchmark_is_official=false" in html
    assert "fallback_reason=missing_official_market_index" in html
    assert "can_judge_alpha=false" in html
    assert "DATA_INSUFFICIENT" in html


def test_repair_price_data_dry_run_keeps_rows_and_creates_backup(tmp_path: Path) -> None:
    db_path = tmp_path / "tw_quant.sqlite"
    _write_price_db(db_path)

    result = repair_price_data(db_path, mode="dry-run", backup_dir=tmp_path / "backups")

    assert result["applied"] is False
    assert result["contaminated_rows"] >= 1
    assert Path(str(result["backup_path"])).exists()
    with sqlite3.connect(db_path) as conn:
        active_count = conn.execute("select count(*) from daily_prices").fetchone()[0]
        quarantine_exists = conn.execute(
            "select count(*) from sqlite_master where type='table' and name='daily_prices_quarantine'"
        ).fetchone()[0]
    assert active_count == 4
    assert quarantine_exists == 0


def test_repair_price_data_apply_moves_contaminated_rows_to_quarantine(tmp_path: Path) -> None:
    db_path = tmp_path / "tw_quant.sqlite"
    _write_price_db(db_path)

    result = repair_price_data(db_path, mode="apply", backup_dir=tmp_path / "backups")

    assert result["applied"] is True
    assert result["contaminated_rows"] >= 1
    with sqlite3.connect(db_path) as conn:
        active_count = conn.execute("select count(*) from daily_prices").fetchone()[0]
        quarantine = pd.read_sql_query("select * from daily_prices_quarantine", conn)
    assert active_count < 4
    assert len(quarantine) == result["contaminated_rows"]
    assert quarantine["quarantine_reason"].str.contains("weekend|price_jump", regex=True).any()


def test_data_quality_health_distinguishes_legacy_active_and_quarantined(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    report_dir = tmp_path / "reports"
    data_dir.mkdir()
    report_dir.mkdir()
    db_path = data_dir / "tw_quant.sqlite"
    _write_price_db(db_path)

    before = build_data_quality_health(pd.DataFrame([{"stock_id": "2330"}]), pd.DataFrame(), report_dir=report_dir)
    before_issue_types = set(before["issue_type"].dropna().astype(str))
    assert "legacy_contamination" in before_issue_types or "active_pipeline_error" in before_issue_types

    repair_price_data(db_path, mode="apply", backup_dir=tmp_path / "backups")

    after = build_data_quality_health(pd.DataFrame([{"stock_id": "2330"}]), pd.DataFrame(), report_dir=report_dir)

    after_issue_types = set(after["issue_type"].dropna().astype(str))
    assert "repaired_or_quarantined" in after_issue_types


def _write_market_indices(path: Path) -> None:
    pd.DataFrame(
        [
            {"trade_date": "2026-05-29", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "open": 100, "high": 100, "low": 100, "close": 100.0, "source": "twse", "is_official": True},
            {"trade_date": "2026-06-01", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "open": 100, "high": 101, "low": 100, "close": 100.5, "source": "twse", "is_official": True},
            {"trade_date": "2026-06-02", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "open": 100, "high": 101, "low": 100, "close": 101.0, "source": "twse", "is_official": True},
            {"trade_date": "2026-06-03", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "open": 101, "high": 102, "low": 101, "close": 101.5, "source": "twse", "is_official": True},
            {"trade_date": "2026-06-04", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "open": 101, "high": 102, "low": 101, "close": 101.8, "source": "twse", "is_official": True},
            {"trade_date": "2026-06-05", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "open": 102, "high": 102, "low": 101, "close": 102.0, "source": "twse", "is_official": True},
        ]
    ).to_csv(path / "market_indices.csv", index=False, encoding="utf-8")


def _write_minimal_html_inputs(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "requested_date": "2026-06-05",
                "trade_date": "2026-06-05",
                "status": "OK",
                "total_capital": 1_000_000.0,
                "total_equity_after_cost": 1_020_000.0,
                "total_equity": 1_020_000.0,
                "market_regime_score": 65.0,
            }
        ]
    ).to_csv(path / "daily_summary_20260605.csv", index=False, encoding="utf-8")


def _write_price_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            create table daily_prices (
                id integer primary key autoincrement,
                trade_date text not null,
                symbol text not null,
                name text not null,
                open real not null,
                high real not null,
                low real not null,
                close real not null,
                volume real not null,
                turnover real,
                market text not null,
                source text not null,
                fetched_at text not null
            )
            """
        )
        rows = [
            ("2026-06-01", "2330", "台積電", 100, 101, 99, 100, 1000, 100000, "TSE", "test", "2026-06-01T00:00:00"),
            ("2026-06-02", "2330", "台積電", 100, 102, 99, 101, 1000, 101000, "TSE", "test", "2026-06-02T00:00:00"),
            ("2026-06-03", "2330", "台積電", 101, 1000, 100, 1000, 1000, 1000000, "TSE", "test", "2026-06-03T00:00:00"),
            ("2026-06-06", "2317", "鴻海", 50, 51, 49, 50, 1000, 50000, "TSE", "test", "2026-06-06T00:00:00"),
        ]
        conn.executemany(
            """
            insert into daily_prices
            (trade_date, symbol, name, open, high, low, close, volume, turnover, market, source, fetched_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
