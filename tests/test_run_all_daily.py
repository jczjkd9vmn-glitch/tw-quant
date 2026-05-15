from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
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
        execute_func=_fake_execute,
        update_func=_fake_update,
    )

    assert result.summary.status == "OK"
    assert result.summary.trade_date == "2026-05-08"
    assert result.summary.scored_rows == 1328
    assert result.summary.candidate_rows == 20
    assert result.summary.risk_pass_rows == 6
    assert result.summary.pending_orders == 4
    assert result.summary.executed_orders == 2
    assert result.summary.skipped_orders == 1
    assert result.summary.new_positions == 2
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
    assert any("execute_pending_orders OK" in message for message in result.messages)
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
        execute_func=_must_not_run,
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
        execute_func=_fake_execute,
        update_func=_fake_update,
    )

    assert result.summary.status == "FAILED"
    assert result.summary.error_step == "export_candidates"
    assert result.summary.error_message == "RuntimeError: export failed"
    assert result.summary_path.exists()
    assert not any("Traceback" in message for message in result.messages)
    exported = pd.read_csv(result.summary_path)
    assert exported.iloc[0]["status"] == "FAILED"


def test_run_all_daily_without_date_falls_back_to_latest_sqlite_date(tmp_path) -> None:
    db_url = _sqlite_url(tmp_path)
    config_path = _config(tmp_path, database_url=db_url)
    engine = create_db_engine(db_url)
    init_db(engine)
    save_daily_prices(engine, _price_frame("20260508"))
    calls = {}

    def fake_run_daily(**kwargs):
        calls.update(kwargs)
        return SimpleNamespace(
            trade_date=kwargs["trade_date"],
            fetched_rows=0,
            scored_rows=1328,
            candidate_rows=20,
            message="",
        )

    result = run_all_daily(
        config_path=config_path,
        trade_date=None,
        capital=1_000_000,
        reports_dir=tmp_path / "reports",
        run_daily_func=fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        execute_func=_fake_execute,
        update_func=_fake_update,
    )

    assert result.summary.status in {"OK", "OK_WITH_FALLBACK"}
    assert calls["trade_date"] == date(2026, 5, 8)
    assert calls["fetch"] is False
    assert "fallback_date=2026-05-08 reason=no trading data" in result.messages


def test_run_all_daily_fallback_overwrites_requested_date_failed_summary(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    failed_summary = reports_dir / "daily_summary_20260510.csv"
    pd.DataFrame([{"status": "FAILED", "trade_date": "2026-05-10"}]).to_csv(
        failed_summary,
        index=False,
    )

    def fake_run_daily(**_kwargs):
        return SimpleNamespace(
            trade_date=date(2026, 5, 8),
            fetched_rows=0,
            scored_rows=1328,
            candidate_rows=20,
            message="",
            fallback_date=date(2026, 5, 8),
            fallback_reason="no trading data",
        )

    result = run_all_daily(
        config_path=_config(tmp_path),
        trade_date="20260510",
        capital=1_000_000,
        reports_dir=reports_dir,
        run_daily_func=fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        execute_func=_fake_execute,
        update_func=_fake_flat_update,
    )

    requested_summary = pd.read_csv(failed_summary)
    actual_summary = reports_dir / "daily_summary_20260508.csv"

    assert result.summary.status == "OK_WITH_FALLBACK"
    assert result.summary_path == failed_summary
    assert actual_summary.exists()
    assert requested_summary.iloc[0]["status"] == "OK_WITH_FALLBACK"
    assert requested_summary.iloc[0]["requested_date"] == "2026-05-10"
    assert requested_summary.iloc[0]["trade_date"] == "2026-05-08"
    assert requested_summary.iloc[0]["fallback_date"] == "2026-05-08"
    assert requested_summary.iloc[0]["fallback_reason"] == "no trading data"
    assert requested_summary.iloc[0]["scored_rows"] == 1328
    assert requested_summary.iloc[0]["candidate_rows"] == 20
    assert requested_summary.iloc[0]["risk_pass_rows"] == 6
    assert requested_summary.iloc[0]["open_positions"] == 6
    assert requested_summary.iloc[0]["total_equity"] == 1_000_000.0


def test_run_all_daily_without_sqlite_data_fails_when_fallback_is_enabled(tmp_path) -> None:
    config_path = _config(tmp_path, database_url=_sqlite_url(tmp_path))

    result = run_all_daily(
        config_path=config_path,
        trade_date=None,
        capital=1_000_000,
        reports_dir=tmp_path / "reports",
        run_daily_func=_fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        execute_func=_fake_execute,
        update_func=_fake_update,
    )

    assert result.summary.status == "FAILED"
    assert result.summary.error_step == "run_daily"
    assert "no price history available for fallback" in result.summary.error_message


def test_run_all_daily_with_explicit_date_runs_requested_date_normally(tmp_path) -> None:
    calls = {}

    def fake_run_daily(**kwargs):
        calls.update(kwargs)
        return _fake_run_daily(**kwargs)

    result = run_all_daily(
        config_path=_config(tmp_path),
        trade_date="20260508",
        capital=1_000_000,
        reports_dir=tmp_path / "reports",
        run_daily_func=fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        execute_func=_fake_execute,
        update_func=_fake_update,
    )

    assert result.summary.status == "OK"
    assert calls["trade_date"] == "20260508"
    assert calls["fetch"] is True
    assert not any(message.startswith("fallback_date=") for message in result.messages)


def test_run_all_daily_passes_exit_strategy_config_to_update(tmp_path) -> None:
    captured = {}

    def fake_update(**kwargs):
        captured.update(kwargs)
        return _fake_update(**kwargs)

    result = run_all_daily(
        config_path=_config(
            tmp_path,
            extra=(
                "exit_strategy:\n"
                "  take_profit_1_pct: 0.08\n"
                "  take_profit_2_pct: 0.15\n"
            ),
        ),
        trade_date="20260508",
        capital=1_000_000,
        reports_dir=tmp_path / "reports",
        run_daily_func=_fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        execute_func=_fake_execute,
        update_func=fake_update,
    )

    assert result.summary.status == "OK"
    assert captured["exit_strategy"]["take_profit_1_pct"] == 0.08
    assert captured["exit_strategy"]["take_profit_2_pct"] == 0.15


def _config(tmp_path, database_url: str = "sqlite:///:memory:", extra: str = "") -> str:
    path = tmp_path / "config.yaml"
    path.write_text(f"database:\n  url: {database_url}\n{extra}", encoding="utf-8")
    return str(path)


def _sqlite_url(tmp_path) -> str:
    return f"sqlite:///{(tmp_path / 'tw_quant.sqlite').as_posix()}"


def _price_frame(trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "symbol": "2330",
                "name": "TSMC",
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 104.0,
                "volume": 2_000_000,
                "turnover": 208_000_000,
                "market": "TSE",
                "source": "TEST",
            }
        ]
    )


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
        pending_orders=pd.DataFrame(
            {"stock_id": range(6), "status": ["PENDING", "PENDING", "PENDING", "PENDING", "EXECUTED", "SKIPPED_EXISTING_POSITION"]}
        ),
        skipped_existing=[],
        warning="",
    )


def _fake_execute(*_args, **_kwargs):
    return SimpleNamespace(
        pending_orders=pd.DataFrame(
            {
                "stock_id": range(7),
                "status": [
                    "PENDING",
                    "PENDING",
                    "PENDING",
                    "PENDING",
                    "EXECUTED",
                    "EXECUTED",
                    "SKIPPED_EXISTING_POSITION",
                ],
                "entry_price_source": ["", "", "", "", "OPEN", "CLOSE_FALLBACK", ""],
            }
        ),
        executed_orders=pd.DataFrame(
            {"stock_id": [1, 2], "entry_price_source": ["OPEN", "CLOSE_FALLBACK"]}
        ),
        skipped_orders=pd.DataFrame({"stock_id": [3]}),
        warnings=["2330: 開盤價缺失或無效，改用收盤價成交"],
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


def _fake_flat_update(*_args, **_kwargs):
    return SimpleNamespace(
        trade_date=pd.Timestamp("2026-05-08"),
        summary=pd.DataFrame(
            [
                {
                    "open_positions": 6,
                    "closed_positions": 0,
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "total_equity": 1_000_000.0,
                }
            ]
        ),
        warning="",
    )


def _must_not_run(*_args, **_kwargs):
    raise AssertionError("this step should have been skipped")
