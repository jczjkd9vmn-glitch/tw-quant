from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.workflow.daily import run_all_daily
from tw_quant.data_sources.official_market_indices import (
    DEFAULT_HISTORY_DAYS,
    fetch_official_market_indices,
    update_market_indices_csv,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run full daily Taiwan stock quant workflow.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--date", default=None, help="Trade date, e.g. 20260508.")
    parser.add_argument("--capital", type=float, default=1_000_000)
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--skip-paper-trade", action="store_true")
    parser.add_argument("--skip-update", action="store_true")
    parser.add_argument(
        "--skip-market-indices",
        action="store_true",
        help="Skip best-effort TWSE/TPEx official market index refresh.",
    )
    parser.add_argument(
        "--allow-fallback-latest",
        action="store_true",
        default=True,
        help="Use latest SQLite trading date when no requested date has valid TWSE data.",
    )
    parser.add_argument(
        "--no-allow-fallback-latest",
        dest="allow_fallback_latest",
        action="store_false",
        help="Disable fallback to latest SQLite trading date.",
    )
    args = parser.parse_args()

    if not args.skip_market_indices:
        _refresh_official_market_indices()

    result = run_all_daily(
        config_path=args.config,
        trade_date=args.date,
        capital=args.capital,
        reports_dir=args.reports_dir,
        skip_paper_trade=args.skip_paper_trade,
        skip_update=args.skip_update,
        allow_fallback_latest=args.allow_fallback_latest,
    )

    for message in result.messages:
        print(message)

    summary = result.summary
    print(
        "summary "
        f"requested_date={summary.requested_date} "
        f"trade_date={summary.trade_date} "
        f"fallback_date={summary.fallback_date} "
        f"fallback_reason={summary.fallback_reason} "
        f"scored_rows={summary.scored_rows} "
        f"candidate_rows={summary.candidate_rows} "
        f"risk_pass_rows={summary.risk_pass_rows} "
        f"pending_orders={summary.pending_orders} "
        f"executed_orders={summary.executed_orders} "
        f"expired_orders={summary.pending_orders_expired_count} "
        f"cancelled_orders={summary.pending_orders_cancelled_count} "
        f"rejected_signal={summary.rejected_orders_signal_count} "
        f"rejected_execution={summary.rejected_orders_execution_count} "
        f"skipped_orders={summary.skipped_orders} "
        f"new_positions={summary.new_positions} "
        f"open_positions={summary.open_positions} "
        f"closed_positions={summary.closed_positions} "
        f"unrealized_pnl={summary.unrealized_pnl} "
        f"realized_pnl={summary.realized_pnl} "
        f"total_equity={summary.total_equity} "
        f"total_cost={summary.total_cost} "
        f"realized_pnl_after_cost={summary.realized_pnl_after_cost} "
        f"total_equity_after_cost={summary.total_equity_after_cost} "
        f"market_intel_status={summary.market_intel_status} "
        f"market_intel_warning_count={summary.market_intel_warning_count} "
        f"actual_data_date={summary.actual_data_date} "
        f"cache_age_days={summary.cache_age_days} "
        f"trading_day_lag={summary.trading_day_lag} "
        f"market_closed={summary.market_closed} "
        f"used_latest_available={summary.used_latest_available} "
        f"is_stale_data={summary.is_stale_data} "
        f"data_freshness_level={summary.data_freshness_level} "
        f"take_profit_exits={summary.take_profit_exits} "
        f"stop_loss_exits={summary.stop_loss_exits} "
        f"trailing_stop_exits={summary.trailing_stop_exits} "
        f"trend_exit_exits={summary.trend_exit_exits} "
        f"strategy_validation_status={summary.strategy_validation_status} "
        f"trading_decisions_status={summary.trading_decisions_status} "
        f"loss_attribution_status={summary.loss_attribution_status} "
        f"market_regime_score={summary.market_regime_score} "
        f"new_entries_allowed={summary.new_entries_allowed} "
        f"guardrail_status={summary.guardrail_status} "
        f"rejected_orders={summary.rejected_orders} "
        f"industry_map_status={summary.industry_map_status} "
        f"ai_enrichment_status={summary.ai_enrichment_status} "
        f"pnl_chart_status={summary.pnl_chart_status} "
        f"market_recap_status={summary.market_recap_status} "
        f"decision_dashboard_status={summary.decision_dashboard_status} "
        f"config_summary_status={summary.config_summary_status} "
        f"enrichment_evidence_status={summary.enrichment_evidence_status} "
        f"ai_used_count={summary.ai_used_count} "
        f"rule_based_enrichment_count={summary.rule_based_enrichment_count} "
        f"enrichment_insufficient_data_count={summary.enrichment_insufficient_data_count} "
        f"buy_candidate_count={summary.buy_candidate_count} "
        f"watch_only_count={summary.watch_only_count} "
        f"no_trade_count={summary.no_trade_count} "
        f"hold_count={summary.hold_count} "
        f"reduce_count={summary.reduce_count} "
        f"exit_review_count={summary.exit_review_count}"
    )

    if summary.status == "FAILED":
        print(f"error: step={summary.error_step} message={summary.error_message}")
        raise SystemExit(1)


def _refresh_official_market_indices() -> None:
    output_path = ROOT / "data" / "market_indices.csv"
    try:
        fetched = fetch_official_market_indices(timeout_seconds=15, history_days=DEFAULT_HISTORY_DAYS)
        merged = update_market_indices_csv(output_path, fetched)
        official_rows = int(merged["is_official"].astype(bool).sum()) if "is_official" in merged.columns else 0
        index_ids = ",".join(sorted(set(merged["index_id"].astype(str)))) if not merged.empty else ""
        print(
            "market_indices_ingestion OK "
            f"rows={len(merged)} "
            f"official_rows={official_rows} "
            f"history_days={DEFAULT_HISTORY_DAYS} "
            f"index_ids={index_ids}"
        )
    except Exception as exc:  # noqa: BLE001
        print(f"market_indices_ingestion WARNING {type(exc).__name__}: {exc}; kept existing data/market_indices.csv")


if __name__ == "__main__":
    main()
