"""One-command daily workflow orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from tw_quant.config import load_config
from tw_quant.data.database import create_db_engine, init_db
from tw_quant.data.pipeline import run_daily_pipeline
from tw_quant.reporting.export import export_latest_candidates
from tw_quant.trading.paper import run_paper_trade
from tw_quant.trading.paper_update import update_paper_positions


@dataclass(frozen=True)
class DailyWorkflowSummary:
    trade_date: str
    scored_rows: int
    candidate_rows: int
    risk_pass_rows: int
    new_positions: int
    open_positions: int
    closed_positions: int
    unrealized_pnl: float
    realized_pnl: float
    total_equity: float
    status: str = "OK"
    error_step: str = ""
    error_message: str = ""


@dataclass(frozen=True)
class DailyWorkflowResult:
    summary: DailyWorkflowSummary
    summary_path: Path
    messages: list[str]
    daily_result: Any | None = None
    export_result: Any | None = None
    paper_result: Any | None = None
    update_result: Any | None = None


def run_all_daily(
    config_path: str | Path = "config.yaml",
    trade_date: str | date | None = None,
    capital: float = 1_000_000,
    reports_dir: str | Path = "reports",
    skip_paper_trade: bool = False,
    skip_update: bool = False,
    run_daily_func: Callable[..., Any] = run_daily_pipeline,
    export_func: Callable[..., Any] = export_latest_candidates,
    paper_func: Callable[..., Any] = run_paper_trade,
    update_func: Callable[..., Any] = update_paper_positions,
) -> DailyWorkflowResult:
    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    summary_values = _empty_summary(trade_date, capital)
    messages: list[str] = []
    daily_result = export_result = paper_result = update_result = None

    try:
        daily_result = run_daily_func(config_path=config_path, trade_date=trade_date, fetch=True)
        summary_values["trade_date"] = _date_text(daily_result.trade_date)
        summary_values["scored_rows"] = int(daily_result.scored_rows)
        summary_values["candidate_rows"] = int(daily_result.candidate_rows)
        messages.append(
            "run_daily OK "
            f"date={daily_result.trade_date} "
            f"fetched_rows={daily_result.fetched_rows} "
            f"scored_rows={daily_result.scored_rows} "
            f"candidate_rows={daily_result.candidate_rows}"
        )
        if getattr(daily_result, "message", ""):
            messages.append(f"run_daily warning {daily_result.message}")
    except Exception as exc:
        return _failed_result(
            report_dir,
            summary_values,
            messages,
            "run_daily",
            exc,
            daily_result=daily_result,
        )

    try:
        config = load_config(config_path)
        engine = create_db_engine(config["database"]["url"])
        init_db(engine)
        export_result = export_func(engine, output_dir=report_dir)
        if getattr(export_result, "warning", ""):
            messages.append(f"export_candidates warning {export_result.warning}")
        else:
            summary_values["trade_date"] = _date_text(export_result.trade_date)
            summary_values["candidate_rows"] = len(export_result.candidates)
            summary_values["risk_pass_rows"] = len(export_result.risk_pass_candidates)
            messages.append(
                "export_candidates OK "
                f"candidate_rows={len(export_result.candidates)} "
                f"risk_pass_rows={len(export_result.risk_pass_candidates)}"
            )
    except Exception as exc:
        return _failed_result(
            report_dir,
            summary_values,
            messages,
            "export_candidates",
            exc,
            daily_result=daily_result,
            export_result=export_result,
        )

    if skip_paper_trade:
        messages.append("paper_trade SKIP")
    else:
        try:
            paper_result = paper_func(reports_dir=report_dir, capital=capital)
            if getattr(paper_result, "warning", ""):
                messages.append(f"paper_trade warning {paper_result.warning}")
            else:
                summary_values["new_positions"] = len(paper_result.new_positions)
                summary_values["open_positions"] = len(paper_result.positions)
                messages.append(
                    "paper_trade OK "
                    f"new_positions={len(paper_result.new_positions)} "
                    f"open_positions={len(paper_result.positions)} "
                    f"skipped_existing={len(paper_result.skipped_existing)}"
                )
        except Exception as exc:
            return _failed_result(
                report_dir,
                summary_values,
                messages,
                "paper_trade",
                exc,
                daily_result=daily_result,
                export_result=export_result,
                paper_result=paper_result,
            )

    if skip_update:
        messages.append("update_paper_positions SKIP")
    else:
        try:
            update_result = update_func(
                engine=engine,
                reports_dir=report_dir,
                trade_date=trade_date,
                capital=capital,
            )
            if getattr(update_result, "warning", ""):
                messages.append(f"update_paper_positions warning {update_result.warning}")
            else:
                _merge_update_summary(summary_values, update_result.summary)
                messages.append(
                    "update_paper_positions OK "
                    f"open_positions={summary_values['open_positions']} "
                    f"closed_positions={summary_values['closed_positions']} "
                    f"total_equity={summary_values['total_equity']}"
                )
        except Exception as exc:
            return _failed_result(
                report_dir,
                summary_values,
                messages,
                "update_paper_positions",
                exc,
                daily_result=daily_result,
                export_result=export_result,
                paper_result=paper_result,
                update_result=update_result,
            )

    summary = DailyWorkflowSummary(**summary_values)
    summary_path = _write_summary(report_dir, summary)
    messages.append(f"daily_summary_csv={summary_path}")
    return DailyWorkflowResult(
        summary=summary,
        summary_path=summary_path,
        messages=messages,
        daily_result=daily_result,
        export_result=export_result,
        paper_result=paper_result,
        update_result=update_result,
    )


def _empty_summary(trade_date: str | date | None, capital: float) -> dict[str, Any]:
    return {
        "trade_date": _date_text(trade_date),
        "scored_rows": 0,
        "candidate_rows": 0,
        "risk_pass_rows": 0,
        "new_positions": 0,
        "open_positions": 0,
        "closed_positions": 0,
        "unrealized_pnl": 0.0,
        "realized_pnl": 0.0,
        "total_equity": float(capital),
        "status": "OK",
        "error_step": "",
        "error_message": "",
    }


def _failed_result(
    report_dir: Path,
    summary_values: dict[str, Any],
    messages: list[str],
    step: str,
    exc: Exception,
    **results,
) -> DailyWorkflowResult:
    summary_values["status"] = "FAILED"
    summary_values["error_step"] = step
    summary_values["error_message"] = f"{type(exc).__name__}: {exc}"
    messages.append(f"{step} FAILED {summary_values['error_message']}")
    summary = DailyWorkflowSummary(**summary_values)
    summary_path = _write_summary(report_dir, summary)
    messages.append(f"daily_summary_csv={summary_path}")
    return DailyWorkflowResult(summary=summary, summary_path=summary_path, messages=messages, **results)


def _merge_update_summary(summary_values: dict[str, Any], update_summary: pd.DataFrame) -> None:
    if update_summary.empty:
        return
    row = update_summary.iloc[0]
    summary_values["open_positions"] = int(row.get("open_positions", 0))
    summary_values["closed_positions"] = int(row.get("closed_positions", 0))
    summary_values["unrealized_pnl"] = float(row.get("unrealized_pnl", 0.0))
    summary_values["realized_pnl"] = float(row.get("realized_pnl", 0.0))
    summary_values["total_equity"] = float(row.get("total_equity", summary_values["total_equity"]))


def _write_summary(report_dir: Path, summary: DailyWorkflowSummary) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    date_label = _date_label(summary.trade_date)
    path = report_dir / f"daily_summary_{date_label}.csv"
    pd.DataFrame([summary.__dict__]).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _date_text(value: str | date | pd.Timestamp | None) -> str:
    if value is None:
        return pd.Timestamp.today().strftime("%Y-%m-%d")
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def _date_label(value: str) -> str:
    return pd.to_datetime(value).strftime("%Y%m%d")
