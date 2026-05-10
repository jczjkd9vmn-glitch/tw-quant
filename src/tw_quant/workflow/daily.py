"""One-command daily workflow orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from tw_quant.config import load_config
from tw_quant.data.database import create_db_engine, init_db, load_latest_price_date
from tw_quant.data.exceptions import TradingHalted
from tw_quant.data.pipeline import run_daily_pipeline
from tw_quant.reporting.export import export_latest_candidates
from tw_quant.trading.paper import run_paper_trade
from tw_quant.trading.paper_update import update_paper_positions
from tw_quant.trading.pending import execute_pending_orders


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
    total_cost: float = 0.0
    realized_pnl_after_cost: float = 0.0
    total_equity_after_cost: float = 0.0
    pending_orders: int = 0
    executed_orders: int = 0
    skipped_orders: int = 0
    entry_price_source_warnings: int = 0
    requested_date: str = ""
    fallback_date: str = ""
    fallback_reason: str = ""
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
    execute_result: Any | None = None
    update_result: Any | None = None


def run_all_daily(
    config_path: str | Path = "config.yaml",
    trade_date: str | date | None = None,
    capital: float = 1_000_000,
    reports_dir: str | Path = "reports",
    skip_paper_trade: bool = False,
    skip_update: bool = False,
    allow_fallback_latest: bool = True,
    run_daily_func: Callable[..., Any] = run_daily_pipeline,
    export_func: Callable[..., Any] = export_latest_candidates,
    paper_func: Callable[..., Any] = run_paper_trade,
    execute_func: Callable[..., Any] = execute_pending_orders,
    update_func: Callable[..., Any] = update_paper_positions,
) -> DailyWorkflowResult:
    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    summary_values = _empty_summary(trade_date, capital)
    messages: list[str] = []
    daily_result = export_result = paper_result = execute_result = update_result = None

    try:
        (
            effective_trade_date,
            fallback_message,
            resolved_fallback_date,
            resolved_fallback_reason,
        ) = _resolve_trade_date(
            config_path=config_path,
            trade_date=trade_date,
            allow_fallback_latest=allow_fallback_latest,
        )
        if fallback_message:
            messages.append(fallback_message)
            _apply_fallback(
                summary_values,
                fallback_date=resolved_fallback_date,
                fallback_reason=resolved_fallback_reason,
            )
        daily_result = run_daily_func(
            config_path=config_path,
            trade_date=effective_trade_date,
            fetch=fallback_message == "",
            allow_fallback_latest=allow_fallback_latest,
        )
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
        fallback_date = getattr(daily_result, "fallback_date", None)
        if fallback_date is not None and not fallback_message:
            reason = getattr(daily_result, "fallback_reason", "") or "no trading data"
            _apply_fallback(summary_values, fallback_date=fallback_date, fallback_reason=reason)
            messages.append(f"fallback_date={_date_text(fallback_date)} reason={reason}")
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
                pending_count = _count_status(getattr(paper_result, "pending_orders", pd.DataFrame()), "PENDING")
                summary_values["pending_orders"] = pending_count
                summary_values["open_positions"] = len(paper_result.positions)
                messages.append(
                    "paper_trade OK "
                    f"pending_orders={pending_count} "
                    f"open_positions={len(paper_result.positions)}"
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

        try:
            try:
                execute_result = execute_func(
                    engine=engine,
                    reports_dir=report_dir,
                    capital=capital,
                    trading_cost=config.get("trading_cost", {}),
                )
            except TypeError:
                execute_result = execute_func(engine=engine, reports_dir=report_dir, capital=capital)
            pending_count = _count_status(execute_result.pending_orders, "PENDING")
            warning_count = _count_entry_price_warnings(execute_result)
            summary_values["pending_orders"] = pending_count
            summary_values["executed_orders"] = len(execute_result.executed_orders)
            summary_values["skipped_orders"] = len(execute_result.skipped_orders)
            summary_values["entry_price_source_warnings"] = warning_count
            summary_values["new_positions"] = len(execute_result.executed_orders)
            messages.append(
                "execute_pending_orders OK "
                f"pending_orders={pending_count} "
                f"executed_orders={len(execute_result.executed_orders)} "
                f"skipped_orders={len(execute_result.skipped_orders)} "
                f"entry_price_source_warnings={warning_count}"
            )
            for warning in getattr(execute_result, "warnings", []):
                messages.append(f"execute_pending_orders warning {warning}")
        except Exception as exc:
            return _failed_result(
                report_dir,
                summary_values,
                messages,
                "execute_pending_orders",
                exc,
                daily_result=daily_result,
                export_result=export_result,
                paper_result=paper_result,
                execute_result=execute_result,
            )

    if skip_update:
        messages.append("update_paper_positions SKIP")
    else:
        try:
            try:
                update_result = update_func(
                    engine=engine,
                    reports_dir=report_dir,
                    trade_date=summary_values["trade_date"],
                    capital=capital,
                    trading_cost=config.get("trading_cost", {}),
                )
            except TypeError:
                update_result = update_func(
                    engine=engine,
                    reports_dir=report_dir,
                    trade_date=summary_values["trade_date"],
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
                execute_result=execute_result,
                update_result=update_result,
            )

    _refresh_fallback_status(summary_values)
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
        execute_result=execute_result,
        update_result=update_result,
    )


def _resolve_trade_date(
    config_path: str | Path,
    trade_date: str | date | None,
    allow_fallback_latest: bool,
) -> tuple[str | date | None, str, date | None, str]:
    if trade_date is not None or not allow_fallback_latest:
        return trade_date, "", None, ""

    config = load_config(config_path)
    engine = create_db_engine(config["database"]["url"])
    init_db(engine)
    latest_date = load_latest_price_date(engine)
    if latest_date is None:
        raise TradingHalted("no price history available for fallback")
    fallback_reason = "no trading data"
    return (
        latest_date,
        f"fallback_date={latest_date} reason={fallback_reason}",
        latest_date,
        fallback_reason,
    )


def _empty_summary(trade_date: str | date | None, capital: float) -> dict[str, Any]:
    requested_date = _date_text(trade_date)
    return {
        "trade_date": requested_date,
        "scored_rows": 0,
        "candidate_rows": 0,
        "risk_pass_rows": 0,
        "new_positions": 0,
        "open_positions": 0,
        "closed_positions": 0,
        "unrealized_pnl": 0.0,
        "realized_pnl": 0.0,
        "total_equity": float(capital),
        "total_cost": 0.0,
        "realized_pnl_after_cost": 0.0,
        "total_equity_after_cost": float(capital),
        "pending_orders": 0,
        "executed_orders": 0,
        "skipped_orders": 0,
        "entry_price_source_warnings": 0,
        "requested_date": requested_date,
        "fallback_date": "",
        "fallback_reason": "",
        "status": "OK",
        "error_step": "",
        "error_message": "",
    }


def _apply_fallback(
    summary_values: dict[str, Any],
    fallback_date: str | date | pd.Timestamp | None,
    fallback_reason: str,
) -> None:
    if fallback_date is None:
        return
    summary_values["fallback_date"] = _date_text(fallback_date)
    summary_values["fallback_reason"] = fallback_reason
    _refresh_fallback_status(summary_values)


def _refresh_fallback_status(summary_values: dict[str, Any]) -> None:
    if (
        summary_values.get("fallback_date")
        and summary_values["requested_date"] != summary_values["trade_date"]
        and summary_values.get("status") == "OK"
    ):
        summary_values["status"] = "OK_WITH_FALLBACK"


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
    summary_values["total_cost"] = float(row.get("total_cost", 0.0))
    summary_values["realized_pnl_after_cost"] = float(
        row.get("realized_pnl_after_cost", summary_values["realized_pnl"])
    )
    summary_values["total_equity_after_cost"] = float(
        row.get("total_equity_after_cost", summary_values["total_equity"])
    )


def _count_status(frame: pd.DataFrame, status: str) -> int:
    if frame.empty or "status" not in frame.columns:
        return 0
    return int((frame["status"].fillna("").astype(str) == status).sum())


def _count_entry_price_warnings(execute_result: Any) -> int:
    executed = getattr(execute_result, "executed_orders", pd.DataFrame())
    if executed.empty or "entry_price_source" not in executed.columns:
        return 0
    return int((executed["entry_price_source"].fillna("").astype(str) == "CLOSE_FALLBACK").sum())


def _write_summary(report_dir: Path, summary: DailyWorkflowSummary) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([summary.__dict__])
    paths = [report_dir / f"daily_summary_{_date_label(summary.trade_date)}.csv"]
    if summary.requested_date and summary.requested_date != summary.trade_date:
        paths.append(report_dir / f"daily_summary_{_date_label(summary.requested_date)}.csv")
    for path in paths:
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    return paths[-1]


def _date_text(value: str | date | pd.Timestamp | None) -> str:
    if value is None:
        return pd.Timestamp.today().strftime("%Y-%m-%d")
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def _date_label(value: str) -> str:
    return pd.to_datetime(value).strftime("%Y%m%d")
