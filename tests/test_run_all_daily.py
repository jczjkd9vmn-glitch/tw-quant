from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd

from tw_quant.workflow.daily import run_all_daily


def test_run_all_daily_success_writes_summary(tmp_path) -> None:
    config_path = _config(tmp_path)
    reports_dir = tmp_path / "reports"

    result = run_all_daily(
        config_path=config_path,
        trade_date="20260508",
        capital=1_000_000,
        reports_dir=reports_dir,
        run_daily_func=_fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        update_func=_fake_update,
    )

    assert result.summary.status == "OK"
    assert result.summary.trade_date == "2026-05-08"
    assert result.summary.scored_rows == 1328
    assert result.summary.candidate_rows == 20
    assert result.summary.risk_pass_rows == 6
    assert result.summary.new_positions == 6
    assert result.summary.open_positions == 6
    assert result.summary.closed_positions == 0
    assert result.summary.unrealized_pnl == 1234.5
    assert result.summary.realized_pnl == 0.0
    assert result.summary.total_equity == 1_001_234.5
    assert result.summary_path.exists()
    exported = pd.read_csv(result.summary_path)
    assert exported.iloc[0]["candidate_rows"] == 20
    assert any("run_daily OK" in message for message in result.messages)
    assert any("export_candidates OK" in message for message in result.messages)
    assert any("paper_trade OK" in message for message in result.messages)
    assert any("update_paper_positions OK" in message for message in result.messages)


def test_run_all_daily_skip_paper_trade_and_update(tmp_path) -> None:
    result = run_all_daily(
        config_path=_config(tmp_path),
        trade_date="20260508",
        capital=1_000_000,
        reports_dir=tmp_path / "reports",
        skip_paper_trade=True,
        skip_update=True,
        run_daily_func=_fake_run_daily,
        export_func=_fake_export,
        paper_func=_must_not_run,
        update_func=_must_not_run,
    )

    assert result.summary.status == "OK"
    assert result.summary.risk_pass_rows == 6
    assert result.summary.new_positions == 0
    assert result.summary.total_equity == 1_000_000
    assert "paper_trade SKIP" in result.messages
    assert "update_paper_positions SKIP" in result.messages


def test_run_all_daily_step_failure_is_summarized_without_traceback(tmp_path) -> None:
    def broken_export(*_args, **_kwargs):
        raise RuntimeError("export failed")

    result = run_all_daily(
        config_path=_config(tmp_path),
        trade_date="20260508",
        capital=1_000_000,
        reports_dir=tmp_path / "reports",
        run_daily_func=_fake_run_daily,
        export_func=broken_export,
        paper_func=_fake_paper,
        update_func=_fake_update,
    )

    assert result.summary.status == "FAILED"
    assert result.summary.error_step == "export_candidates"
    assert result.summary.error_message == "RuntimeError: export failed"
    assert result.summary_path.exists()
    assert not any("Traceback" in message for message in result.messages)
    exported = pd.read_csv(result.summary_path)
    assert exported.iloc[0]["status"] == "FAILED"


def _config(tmp_path) -> str:
    path = tmp_path / "config.yaml"
    path.write_text("database:\n  url: sqlite:///:memory:\n", encoding="utf-8")
    return str(path)


def _fake_run_daily(**_kwargs):
    return SimpleNamespace(
        trade_date=date(2026, 5, 8),
        fetched_rows=1345,
        scored_rows=1328,
        candidate_rows=20,
        message="",
    )


def _fake_export(*_args, **_kwargs):
    return SimpleNamespace(
        trade_date=pd.Timestamp("2026-05-08"),
        candidates=pd.DataFrame({"stock_id": range(20)}),
        risk_pass_candidates=pd.DataFrame({"stock_id": range(6)}),
        candidates_path="reports/candidates_20260508.csv",
        risk_pass_path="reports/risk_pass_candidates_20260508.csv",
        warning="",
    )


def _fake_paper(*_args, **_kwargs):
    return SimpleNamespace(
        trade_date=pd.Timestamp("2026-05-08"),
        new_positions=pd.DataFrame({"stock_id": range(6)}),
        positions=pd.DataFrame({"stock_id": range(6)}),
        skipped_existing=[],
        warning="",
    )


def _fake_update(*_args, **_kwargs):
    return SimpleNamespace(
        trade_date=pd.Timestamp("2026-05-08"),
        summary=pd.DataFrame(
            [
                {
                    "open_positions": 6,
                    "closed_positions": 0,
                    "unrealized_pnl": 1234.5,
                    "realized_pnl": 0.0,
                    "total_equity": 1_001_234.5,
                }
            ]
        ),
        warning="",
    )


def _must_not_run(*_args, **_kwargs):
    raise AssertionError("this step should have been skipped")
