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
        f"skipped_orders={summary.skipped_orders} "
        f"new_positions={summary.new_positions} "
        f"open_positions={summary.open_positions} "
        f"closed_positions={summary.closed_positions} "
        f"unrealized_pnl={summary.unrealized_pnl} "
        f"realized_pnl={summary.realized_pnl} "
        f"total_equity={summary.total_equity} "
        f"total_cost={summary.total_cost} "
        f"realized_pnl_after_cost={summary.realized_pnl_after_cost} "
        f"total_equity_after_cost={summary.total_equity_after_cost}"
    )

    if summary.status == "FAILED":
        print(f"error: step={summary.error_step} message={summary.error_message}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
