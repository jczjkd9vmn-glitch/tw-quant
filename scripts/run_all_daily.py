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
    args = parser.parse_args()

    result = run_all_daily(
        config_path=args.config,
        trade_date=args.date,
        capital=args.capital,
        reports_dir=args.reports_dir,
        skip_paper_trade=args.skip_paper_trade,
        skip_update=args.skip_update,
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
        f"new_positions={summary.new_positions} "
        f"open_positions={summary.open_positions} "
        f"closed_positions={summary.closed_positions} "
        f"unrealized_pnl={summary.unrealized_pnl} "
        f"realized_pnl={summary.realized_pnl} "
        f"total_equity={summary.total_equity}"
    )

    if summary.status == "FAILED":
        print(f"error: step={summary.error_step} message={summary.error_message}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
