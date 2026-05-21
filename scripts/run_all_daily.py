from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.workflow.daily import run_all_daily


def main() -> None:
    parser = argparse.ArgumentParser(description="Run full daily Taiwan stock quant workflow.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--date", default=None, help="Trade date, e.g. 20260508.")
    parser.add_argument("--capital", type=float, default=1_000_000)
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--skip-paper-trade", action="store_true")
    parser.add_argument("--skip-update", action="store_true")
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
        f"trade_date={summary.trade_date} "
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


if __name__ == "__main__":
    main()
