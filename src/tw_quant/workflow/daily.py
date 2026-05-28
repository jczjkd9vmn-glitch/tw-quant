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
from tw_quant.data_sources.local_derived_provider import LocalDerivedProvider
from tw_quant.decision.engine import decision_counts, generate_trading_decisions
from tw_quant.enrichment.industry import update_industry_map
from tw_quant.enrichment.report import generate_ai_enrichment
from tw_quant.reporting.dashboard_data import generate_market_recap, generate_pnl_chart_data
from tw_quant.reporting.candidate_coverage import generate_candidate_coverage_report
from tw_quant.reporting.position_review import generate_position_review_summary
from tw_quant.reporting.export import export_latest_candidates
from tw_quant.trading.paper import run_paper_trade
from tw_quant.trading.paper_update import update_paper_positions
from tw_quant.trading.pending import execute_pending_orders
from tw_quant.validation.loss_attribution import generate_loss_attribution
from tw_quant.validation.strategy_validation import generate_strategy_validation


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
    take_profit_exits: int = 0
    stop_loss_exits: int = 0
    trailing_stop_exits: int = 0
    trend_exit_exits: int = 0
    time_exit_exits: int = 0
    realized_pnl_after_cost_today: float = 0.0
    fundamental_positive_candidates: int = 0
    fundamental_warning_candidates: int = 0
    high_risk_event_candidates: int = 0
    valuation_warning_candidates: int = 0
    financial_warning_candidates: int = 0
    institutional_positive_candidates: int = 0
    multi_factor_data_status: str = ""
    market_intel_status: str = ""
    market_intel_warning_count: int = 0
    market_intel_top_score: float = 0.0
    pending_orders: int = 0
    executed_orders: int = 0
    skipped_orders: int = 0
    pending_orders_active_count: int = 0
    pending_orders_executed_count: int = 0
    pending_orders_expired_count: int = 0
    pending_orders_cancelled_count: int = 0
    rejected_orders_signal_count: int = 0
    rejected_orders_execution_count: int = 0
    rejected_orders_total_count: int = 0
    guardrail_blocked_execution_count: int = 0
    expired_pending_orders_count: int = 0
    cancelled_by_market_regime_count: int = 0
    cancelled_by_low_grade_count: int = 0
    cancelled_by_event_risk_count: int = 0
    cancelled_by_max_position_count: int = 0
    entry_price_source_warnings: int = 0
    requested_date: str = ""
    fallback_date: str = ""
    fallback_reason: str = ""
    status: str = "OK"
    error_step: str = ""
    error_message: str = ""
    strategy_validation_status: str = ""
    trading_decisions_status: str = ""
    buy_candidate_count: int = 0
    watch_only_count: int = 0
    no_trade_count: int = 0
    hold_count: int = 0
    reduce_count: int = 0
    exit_review_count: int = 0
    grade_a_count: int = 0
    grade_b_count: int = 0
    grade_c_count: int = 0
    grade_d_count: int = 0
    market_regime_score: float = 0.0
    new_entries_allowed: bool = True
    guardrail_status: str = ""
    pause_new_entries_reason: str = ""
    rejected_orders: int = 0
    loss_attribution_status: str = ""
    loss_attribution_loss_count: int = 0
    loss_attribution_top_reason: str = ""
    ai_enrichment_status: str = ""
    ai_used_count: int = 0
    rule_based_enrichment_count: int = 0
    enrichment_insufficient_data_count: int = 0
    industry_map_status: str = ""
    pnl_chart_status: str = ""
    market_recap_status: str = ""
    decision_dashboard_status: str = ""
    config_summary_status: str = ""
    enrichment_evidence_status: str = ""


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
    validation_result: Any | None = None
    decision_result: Any | None = None
    loss_attribution_result: Any | None = None
    enrichment_result: Any | None = None
    pnl_chart_result: Any | None = None
    market_recap_result: Any | None = None


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
    validation_func: Callable[..., Any] = generate_strategy_validation,
    decision_func: Callable[..., Any] = generate_trading_decisions,
    loss_attribution_func: Callable[..., Any] = generate_loss_attribution,
    industry_map_func: Callable[..., Any] = update_industry_map,
    enrichment_func: Callable[..., Any] = generate_ai_enrichment,
    pnl_chart_func: Callable[..., Any] = generate_pnl_chart_data,
    market_recap_func: Callable[..., Any] = generate_market_recap,
    candidate_coverage_func: Callable[..., Any] = generate_candidate_coverage_report,
    position_review_func: Callable[..., Any] = generate_position_review_summary,
) -> DailyWorkflowResult:
    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    summary_values = _empty_summary(trade_date, capital)
    messages: list[str] = []
    daily_result = export_result = paper_result = execute_result = update_result = None
    validation_result = decision_result = loss_attribution_result = enrichment_result = None
    pnl_chart_result = market_recap_result = None

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
        data_dir = Path(config_path).resolve().parent / "data"
        try:
            try:
                _industry_path, industry_status, industry_rows = industry_map_func(
                    data_dir=data_dir,
                    config_path=config_path,
                )
            except TypeError:
                _industry_path, industry_status, industry_rows = industry_map_func()
            summary_values["industry_map_status"] = str(industry_status)
            messages.append(f"industry_map {industry_status} rows={industry_rows}")
            _refresh_local_sector_strength(
                data_dir=data_dir,
                config_path=config_path,
                trade_date=summary_values["trade_date"],
                messages=messages,
            )
        except Exception as exc:
            summary_values["industry_map_status"] = "FAILED"
            messages.append(f"industry_map warning {type(exc).__name__}: {exc}")
        try:
            export_result = export_func(
                engine,
                output_dir=report_dir,
                config=config,
                requested_date=summary_values.get("requested_date"),
                fallback_date=summary_values.get("fallback_date"),
                fallback_reason=summary_values.get("fallback_reason", ""),
            )
        except TypeError:
            try:
                export_result = export_func(engine, output_dir=report_dir, config=config)
            except TypeError:
                export_result = export_func(engine, output_dir=report_dir)
        if getattr(export_result, "warning", ""):
            messages.append(f"export_candidates warning {export_result.warning}")
        else:
            summary_values["trade_date"] = _date_text(export_result.trade_date)
            summary_values["candidate_rows"] = len(export_result.candidates)
            summary_values["risk_pass_rows"] = len(export_result.risk_pass_candidates)
            summary_values["fundamental_positive_candidates"] = _count_fundamental_positive(
                export_result.candidates
            )
            summary_values["fundamental_warning_candidates"] = _count_fundamental_warning(
                export_result.candidates
            )
            summary_values["high_risk_event_candidates"] = _count_high_risk_events(
                export_result.candidates
            )
            summary_values["valuation_warning_candidates"] = _count_non_empty(
                export_result.candidates, "valuation_warning"
            )
            summary_values["financial_warning_candidates"] = _count_non_empty(
                export_result.candidates, "financial_warning"
            )
            summary_values["institutional_positive_candidates"] = _count_institutional_positive(
                export_result.candidates
            )
            summary_values["multi_factor_data_status"] = _data_status_text(
                getattr(export_result, "data_fetch_status", pd.DataFrame())
            )
            summary_values["market_intel_status"] = _market_intel_status(
                getattr(export_result, "data_fetch_status", pd.DataFrame())
            )
            summary_values["market_intel_warning_count"] = _count_non_empty(
                export_result.candidates, "market_intel_warning"
            )
            summary_values["market_intel_top_score"] = _max_numeric(
                export_result.candidates, "final_market_score"
            )
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
            try:
                paper_result = paper_func(
                    reports_dir=report_dir,
                    capital=capital,
                    config=load_config(config_path),
                    config_path=config_path,
                    engine=engine,
                )
            except TypeError:
                paper_result = paper_func(reports_dir=report_dir, capital=capital)
            if getattr(paper_result, "warning", ""):
                messages.append(f"paper_trade warning {paper_result.warning}")
            else:
                pending_count = _count_status(getattr(paper_result, "pending_orders", pd.DataFrame()), "PENDING")
                rejected_count = len(getattr(paper_result, "rejected_orders", pd.DataFrame()))
                summary_values["pending_orders"] = pending_count
                summary_values["pending_orders_active_count"] = pending_count
                summary_values["open_positions"] = len(paper_result.positions)
                summary_values["rejected_orders"] = rejected_count
                summary_values["rejected_orders_signal_count"] = rejected_count
                summary_values["rejected_orders_total_count"] = rejected_count
                summary_values["market_regime_score"] = float(getattr(paper_result, "market_regime_score", 0.0) or 0.0)
                summary_values["new_entries_allowed"] = bool(getattr(paper_result, "new_entries_allowed", True))
                summary_values["guardrail_status"] = str(getattr(paper_result, "guardrail_status", ""))
                summary_values["pause_new_entries_reason"] = str(getattr(paper_result, "pause_new_entries_reason", ""))
                messages.append(
                    "paper_trade OK "
                    f"pending_orders={pending_count} "
                    f"rejected_orders={rejected_count} "
                    f"open_positions={len(paper_result.positions)} "
                    f"guardrail_status={summary_values['guardrail_status']} "
                    f"market_regime_score={summary_values['market_regime_score']}"
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
                    config=config,
                    config_path=config_path,
                )
            except TypeError:
                execute_result = execute_func(engine=engine, reports_dir=report_dir, capital=capital)
            pending_count = _count_status(execute_result.pending_orders, "PENDING")
            warning_count = _count_entry_price_warnings(execute_result)
            pending_counts = _pending_status_counts(execute_result.pending_orders)
            rejected_counts = _rejected_status_counts(getattr(execute_result, "rejected_orders", pd.DataFrame()))
            summary_values["pending_orders"] = pending_count
            summary_values["pending_orders_active_count"] = pending_count
            summary_values["executed_orders"] = len(execute_result.executed_orders)
            summary_values["pending_orders_executed_count"] = pending_counts.get("EXECUTED", len(execute_result.executed_orders))
            summary_values["pending_orders_expired_count"] = pending_counts.get("EXPIRED", 0)
            summary_values["expired_pending_orders_count"] = pending_counts.get("EXPIRED", 0)
            summary_values["pending_orders_cancelled_count"] = _cancelled_pending_count(execute_result.pending_orders)
            summary_values["skipped_orders"] = len(execute_result.skipped_orders)
            summary_values["entry_price_source_warnings"] = warning_count
            summary_values["new_positions"] = len(execute_result.executed_orders)
            summary_values["rejected_orders_execution_count"] = len(getattr(execute_result, "rejected_orders", pd.DataFrame()))
            summary_values["rejected_orders_total_count"] = (
                int(summary_values.get("rejected_orders_signal_count", 0))
                + int(summary_values["rejected_orders_execution_count"])
            )
            summary_values["rejected_orders"] = summary_values["rejected_orders_total_count"]
            summary_values["guardrail_blocked_execution_count"] = rejected_counts.get("CANCELLED_BY_GUARDRAIL", 0)
            summary_values["cancelled_by_market_regime_count"] = rejected_counts.get("CANCELLED_BY_MARKET_REGIME", 0)
            summary_values["cancelled_by_low_grade_count"] = rejected_counts.get("CANCELLED_BY_LOW_GRADE", 0)
            summary_values["cancelled_by_event_risk_count"] = rejected_counts.get("CANCELLED_BY_EVENT_RISK", 0)
            summary_values["cancelled_by_max_position_count"] = rejected_counts.get("CANCELLED_BY_MAX_POSITION", 0)
            messages.append(
                "execute_pending_orders OK "
                f"pending_orders={pending_count} "
                f"executed_orders={len(execute_result.executed_orders)} "
                f"skipped_orders={len(execute_result.skipped_orders)} "
                f"expired_orders={summary_values['pending_orders_expired_count']} "
                f"cancelled_orders={summary_values['pending_orders_cancelled_count']} "
                f"rejected_execution={summary_values['rejected_orders_execution_count']} "
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
                    exit_strategy=config.get("exit_strategy", {}),
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

    try:
        try:
            loss_attribution_result = loss_attribution_func(
                reports_dir=report_dir,
                trade_date=summary_values["trade_date"],
            )
        except TypeError:
            loss_attribution_result = loss_attribution_func(reports_dir=report_dir)
        attribution = getattr(loss_attribution_result, "attribution", pd.DataFrame())
        summary_values["loss_attribution_status"] = (
            "WARNING" if getattr(loss_attribution_result, "warning", "") else "OK"
        )
        _merge_loss_attribution_summary(summary_values, attribution)
        messages.append(
            "loss_attribution OK "
            f"rows={len(attribution)} "
            f"loss_count={summary_values['loss_attribution_loss_count']}"
        )
        if getattr(loss_attribution_result, "warning", ""):
            messages.append(f"loss_attribution warning {loss_attribution_result.warning}")
    except Exception as exc:
        summary_values["loss_attribution_status"] = "FAILED"
        messages.append(f"loss_attribution warning {type(exc).__name__}: {exc}")

    validation_config = config.get("strategy_validation", {}) if "config" in locals() else {}
    if validation_config.get("enabled", True):
        try:
            try:
                validation_result = validation_func(
                    reports_dir=report_dir,
                    trade_date=summary_values["trade_date"],
                    min_trades_required=int(validation_config.get("min_trades_required", 10)),
                )
            except TypeError:
                validation_result = validation_func(
                    reports_dir=report_dir,
                    trade_date=summary_values["trade_date"],
                )
            summary_values["strategy_validation_status"] = (
                "WARNING" if getattr(validation_result, "warning", "") else "OK"
            )
            messages.append(
                "strategy_validation OK "
                f"rows={len(getattr(validation_result, 'validation', pd.DataFrame()))}"
            )
            if getattr(validation_result, "warning", ""):
                messages.append(f"strategy_validation warning {validation_result.warning}")
        except Exception as exc:
            summary_values["strategy_validation_status"] = "FAILED"
            messages.append(f"strategy_validation warning {type(exc).__name__}: {exc}")

    decision_config = config.get("decision_engine", {}) if "config" in locals() else {}
    if decision_config.get("enabled", True):
        try:
            try:
                decision_result = decision_func(
                    reports_dir=report_dir,
                    config_path=config_path,
                    trade_date=summary_values["trade_date"],
                )
            except TypeError:
                decision_result = decision_func(
                    reports_dir=report_dir,
                    trade_date=summary_values["trade_date"],
                )
            summary_values["trading_decisions_status"] = (
                "WARNING" if getattr(decision_result, "warning", "") else "OK"
            )
            for key, value in decision_counts(getattr(decision_result, "decisions", pd.DataFrame())).items():
                summary_values[key] = value
            messages.append(
                "trading_decisions OK "
                f"rows={len(getattr(decision_result, 'decisions', pd.DataFrame()))} "
                f"buy_candidate_count={summary_values['buy_candidate_count']} "
                f"watch_only_count={summary_values['watch_only_count']} "
                f"no_trade_count={summary_values['no_trade_count']}"
            )
            if getattr(decision_result, "warning", ""):
                messages.append(f"trading_decisions warning {decision_result.warning}")
        except Exception as exc:
            summary_values["trading_decisions_status"] = "FAILED"
            messages.append(f"trading_decisions warning {type(exc).__name__}: {exc}")

    try:
        candidate_coverage_result = candidate_coverage_func(
            reports_dir=report_dir,
            trade_date=summary_values["trade_date"],
        )
        if getattr(candidate_coverage_result, "output_path", None):
            messages.append(
                "candidate_coverage_report OK "
                f"rows={len(getattr(candidate_coverage_result, 'coverage', pd.DataFrame()))}"
            )
        if getattr(candidate_coverage_result, "warning", ""):
            messages.append(f"candidate_coverage_report warning {candidate_coverage_result.warning}")
    except Exception as exc:
        messages.append(f"candidate_coverage_report warning {type(exc).__name__}: {exc}")

    try:
        position_review_result = position_review_func(
            reports_dir=report_dir,
            config=config if "config" in locals() else {},
            trade_date=summary_values["trade_date"],
        )
        if getattr(position_review_result, "output_path", None):
            messages.append(
                "position_review_summary OK "
                f"rows={len(getattr(position_review_result, 'review', pd.DataFrame()))}"
            )
        if getattr(position_review_result, "warning", ""):
            messages.append(f"position_review_summary warning {position_review_result.warning}")
    except Exception as exc:
        messages.append(f"position_review_summary warning {type(exc).__name__}: {exc}")

    enrichment_config = config.get("ai_enrichment", {}) if "config" in locals() else {}
    if enrichment_config.get("enabled", True):
        try:
            data_dir = Path(config_path).resolve().parent / "data"
            try:
                enrichment_result = enrichment_func(
                    reports_dir=report_dir,
                    data_dir=data_dir,
                    config_path=config_path,
                    trade_date=summary_values["trade_date"],
                )
            except TypeError:
                enrichment_result = enrichment_func(
                    reports_dir=report_dir,
                    trade_date=summary_values["trade_date"],
                )
            enrichment = getattr(enrichment_result, "enrichment", pd.DataFrame())
            summary_values["ai_enrichment_status"] = (
                "WARNING" if getattr(enrichment_result, "warning", "") else "OK"
            )
            summary_values["ai_used_count"] = _count_true(enrichment, "ai_used")
            summary_values["rule_based_enrichment_count"] = _count_provider(enrichment, "rule_based")
            summary_values["enrichment_insufficient_data_count"] = _count_status_value(
                enrichment, "enrichment_status", {"PARTIAL", "INSUFFICIENT_DATA"}
            )
            messages.append(
                "ai_enrichment OK "
                f"rows={len(enrichment)} "
                f"ai_used={summary_values['ai_used_count']} "
                f"rule_based={summary_values['rule_based_enrichment_count']} "
                f"insufficient_data={summary_values['enrichment_insufficient_data_count']}"
            )
            if getattr(enrichment_result, "warning", ""):
                messages.append(f"ai_enrichment warning {enrichment_result.warning}")
        except Exception as exc:
            summary_values["ai_enrichment_status"] = "FAILED"
            messages.append(f"ai_enrichment warning {type(exc).__name__}: {exc}")

    try:
        try:
            pnl_chart_result = pnl_chart_func(
                reports_dir=report_dir,
                trade_date=summary_values["trade_date"],
                current_summary=summary_values,
            )
        except TypeError:
            pnl_chart_result = pnl_chart_func()
        summary_values["pnl_chart_status"] = str(getattr(pnl_chart_result, "status", "OK"))
        messages.append(
            "pnl_chart_data "
            f"{summary_values['pnl_chart_status']} "
            f"rows={len(getattr(pnl_chart_result, 'frame', pd.DataFrame()))}"
        )
        if getattr(pnl_chart_result, "warning", ""):
            messages.append(f"pnl_chart_data warning {pnl_chart_result.warning}")
    except Exception as exc:
        summary_values["pnl_chart_status"] = "FAILED"
        messages.append(f"pnl_chart_data warning {type(exc).__name__}: {exc}")

    try:
        try:
            market_recap_result = market_recap_func(
                reports_dir=report_dir,
                config_path=config_path,
                trade_date=summary_values["trade_date"],
            )
        except TypeError:
            market_recap_result = market_recap_func()
        summary_values["market_recap_status"] = str(getattr(market_recap_result, "status", "OK"))
        messages.append(
            "market_recap "
            f"{summary_values['market_recap_status']} "
            f"rows={len(getattr(market_recap_result, 'frame', pd.DataFrame()))}"
        )
        if getattr(market_recap_result, "warning", ""):
            messages.append(f"market_recap warning {market_recap_result.warning}")
    except Exception as exc:
        summary_values["market_recap_status"] = "FAILED"
        messages.append(f"market_recap warning {type(exc).__name__}: {exc}")

    summary_values["decision_dashboard_status"] = "OK" if summary_values.get("trading_decisions_status") != "FAILED" else "WARNING"
    summary_values["config_summary_status"] = "OK"
    summary_values["enrichment_evidence_status"] = (
        "OK" if summary_values.get("ai_enrichment_status") in {"OK", "WARNING"} else summary_values.get("ai_enrichment_status", "")
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
        validation_result=validation_result,
        decision_result=decision_result,
        loss_attribution_result=loss_attribution_result,
        enrichment_result=enrichment_result,
        pnl_chart_result=pnl_chart_result,
        market_recap_result=market_recap_result,
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
        "take_profit_exits": 0,
        "stop_loss_exits": 0,
        "trailing_stop_exits": 0,
        "trend_exit_exits": 0,
        "time_exit_exits": 0,
        "realized_pnl_after_cost_today": 0.0,
        "fundamental_positive_candidates": 0,
        "fundamental_warning_candidates": 0,
        "high_risk_event_candidates": 0,
        "valuation_warning_candidates": 0,
        "financial_warning_candidates": 0,
        "institutional_positive_candidates": 0,
        "multi_factor_data_status": "",
        "market_intel_status": "",
        "market_intel_warning_count": 0,
        "market_intel_top_score": 0.0,
        "pending_orders": 0,
        "executed_orders": 0,
        "skipped_orders": 0,
        "pending_orders_active_count": 0,
        "pending_orders_executed_count": 0,
        "pending_orders_expired_count": 0,
        "pending_orders_cancelled_count": 0,
        "rejected_orders_signal_count": 0,
        "rejected_orders_execution_count": 0,
        "rejected_orders_total_count": 0,
        "guardrail_blocked_execution_count": 0,
        "expired_pending_orders_count": 0,
        "cancelled_by_market_regime_count": 0,
        "cancelled_by_low_grade_count": 0,
        "cancelled_by_event_risk_count": 0,
        "cancelled_by_max_position_count": 0,
        "entry_price_source_warnings": 0,
        "requested_date": requested_date,
        "fallback_date": "",
        "fallback_reason": "",
        "status": "OK",
        "error_step": "",
        "error_message": "",
        "strategy_validation_status": "",
        "trading_decisions_status": "",
        "buy_candidate_count": 0,
        "watch_only_count": 0,
        "no_trade_count": 0,
        "hold_count": 0,
        "reduce_count": 0,
        "exit_review_count": 0,
        "grade_a_count": 0,
        "grade_b_count": 0,
        "grade_c_count": 0,
        "grade_d_count": 0,
        "market_regime_score": 0.0,
        "new_entries_allowed": True,
        "guardrail_status": "",
        "pause_new_entries_reason": "",
        "rejected_orders": 0,
        "loss_attribution_status": "",
        "loss_attribution_loss_count": 0,
        "loss_attribution_top_reason": "",
        "ai_enrichment_status": "",
        "ai_used_count": 0,
        "rule_based_enrichment_count": 0,
        "enrichment_insufficient_data_count": 0,
        "industry_map_status": "",
        "pnl_chart_status": "",
        "market_recap_status": "",
        "decision_dashboard_status": "",
        "config_summary_status": "",
        "enrichment_evidence_status": "",
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


def _refresh_local_sector_strength(
    data_dir: Path,
    config_path: str | Path,
    trade_date: str,
    messages: list[str],
) -> None:
    try:
        result = LocalDerivedProvider(config_path=config_path).fetch_sector_strength(trade_date)
        data = getattr(result, "data", pd.DataFrame())
        if data.empty:
            warning = getattr(result, "warning", "") or "no local sector strength rows"
            messages.append(f"sector_strength refresh warning {warning}")
            return
        data_dir.mkdir(parents=True, exist_ok=True)
        data.to_csv(data_dir / "sector_strength.csv", index=False, encoding="utf-8-sig")
        messages.append(f"sector_strength refresh {getattr(result, 'status', 'OK')} rows={len(data)}")
        if getattr(result, "warning", ""):
            messages.append(f"sector_strength refresh warning {result.warning}")
    except Exception as exc:  # noqa: BLE001
        messages.append(f"sector_strength refresh warning {type(exc).__name__}: {exc}")


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
    for column in [
        "take_profit_exits",
        "stop_loss_exits",
        "trailing_stop_exits",
        "trend_exit_exits",
        "time_exit_exits",
    ]:
        summary_values[column] = int(row.get(column, 0))
    summary_values["realized_pnl_after_cost_today"] = float(
        row.get("realized_pnl_after_cost_today", 0.0)
    )


def _merge_loss_attribution_summary(summary_values: dict[str, Any], attribution: pd.DataFrame) -> None:
    if attribution.empty:
        return
    realized = pd.to_numeric(
        attribution.get("realized_pnl_pct", pd.Series([None] * len(attribution))),
        errors="coerce",
    )
    unrealized = pd.to_numeric(
        attribution.get("unrealized_pnl_pct", pd.Series([None] * len(attribution))),
        errors="coerce",
    )
    returns = realized.where(realized.notna(), unrealized).fillna(0.0)
    summary_values["loss_attribution_loss_count"] = int((returns < 0).sum())
    if "likely_loss_reason" in attribution.columns:
        reasons = attribution.loc[returns < 0, "likely_loss_reason"].fillna("").astype(str)
        reasons = reasons[reasons.str.strip() != ""]
        if not reasons.empty:
            summary_values["loss_attribution_top_reason"] = reasons.value_counts().index[0]


def _count_status(frame: pd.DataFrame, status: str) -> int:
    if frame.empty or "status" not in frame.columns:
        return 0
    return int((frame["status"].fillna("").astype(str) == status).sum())


def _pending_status_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or "status" not in frame.columns:
        return {}
    return frame["status"].fillna("").astype(str).str.upper().value_counts().to_dict()


def _rejected_status_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty:
        return {}
    column = "final_order_status" if "final_order_status" in frame.columns else "rejected_status"
    if column not in frame.columns:
        return {}
    return frame[column].fillna("").astype(str).str.upper().value_counts().to_dict()


def _cancelled_pending_count(frame: pd.DataFrame) -> int:
    if frame.empty or "status" not in frame.columns:
        return 0
    statuses = frame["status"].fillna("").astype(str).str.upper()
    return int(statuses.str.startswith("CANCELLED_").sum() + (statuses == "SKIPPED_EXISTING_POSITION").sum())


def _count_true(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].apply(_to_bool).sum())


def _count_provider(frame: pd.DataFrame, provider: str) -> int:
    if frame.empty or "enrichment_provider" not in frame.columns:
        return 0
    return int((frame["enrichment_provider"].fillna("").astype(str) == provider).sum())


def _count_status_value(frame: pd.DataFrame, column: str, values: set[str]) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].fillna("").astype(str).str.upper().isin(values).sum())


def _count_entry_price_warnings(execute_result: Any) -> int:
    executed = getattr(execute_result, "executed_orders", pd.DataFrame())
    if executed.empty or "entry_price_source" not in executed.columns:
        return 0
    return int((executed["entry_price_source"].fillna("").astype(str) == "CLOSE_FALLBACK").sum())


def _count_fundamental_positive(frame: pd.DataFrame) -> int:
    score_column = "revenue_score" if "revenue_score" in frame.columns else "fundamental_score"
    if frame.empty or score_column not in frame.columns:
        return 0
    return int((pd.to_numeric(frame[score_column], errors="coerce").fillna(50) > 50).sum())


def _count_fundamental_warning(frame: pd.DataFrame) -> int:
    score_column = "revenue_score" if "revenue_score" in frame.columns else "fundamental_score"
    if frame.empty or score_column not in frame.columns:
        return 0
    return int((pd.to_numeric(frame[score_column], errors="coerce").fillna(50) < 50).sum())


def _count_high_risk_events(frame: pd.DataFrame) -> int:
    if frame.empty or "event_blocked" not in frame.columns:
        return 0
    return int(frame["event_blocked"].apply(_to_bool).sum())


def _count_non_empty(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    values = frame[column].fillna("").astype(str).str.strip()
    return int((values != "").sum())


def _count_institutional_positive(frame: pd.DataFrame) -> int:
    if frame.empty or "institutional_score" not in frame.columns:
        return 0
    return int((pd.to_numeric(frame["institutional_score"], errors="coerce").fillna(50) > 50).sum())


def _data_status_text(status: pd.DataFrame) -> str:
    if status is None or status.empty or "status" not in status.columns:
        return ""
    counts = status["status"].fillna("").astype(str).value_counts().to_dict()
    return "；".join(f"{key}:{value}" for key, value in sorted(counts.items()) if key)


def _market_intel_status(status: pd.DataFrame) -> str:
    if status is None or status.empty or "source_name" not in status.columns:
        return ""
    frame = status[status["source_name"].fillna("").astype(str) == "market_intel"].copy()
    if frame.empty:
        return ""
    return str(frame.iloc[-1].get("status", "")).strip()


def _max_numeric(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return 0.0
    return float(round(values.max(), 2))


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


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
