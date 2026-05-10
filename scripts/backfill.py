from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.data.backfill import run_backfill


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Taiwan stock daily prices.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--start", default=None, help="Start date, e.g. 20250101")
    parser.add_argument("--end", default=None, help="End date, e.g. 20260508")
    parser.add_argument("--days", type=int, default=None, help="Calendar days ending at --end or today.")
    parser.add_argument("--retries", type=int, default=3, help="Fetch retries per date.")
    parser.add_argument("--sleep", type=float, default=0.5, help="Sleep seconds after each date.")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP request timeout seconds.")
    parser.add_argument("--verbose", action="store_true", help="Print detailed TWSE payload debug.")
    args = parser.parse_args()

    result = run_backfill(
        config_path=args.config,
        start=args.start,
        end=args.end,
        days=args.days,
        retries=args.retries,
        sleep_seconds=args.sleep,
        timeout_seconds=args.timeout,
        verbose=args.verbose,
    )

    for day in result.days:
        if day.status == "success":
            print(f"{day.trade_date} OK rows={day.saved_rows}")
        elif day.status == "skipped":
            print(f"{day.trade_date} SKIP {day.skipped_reason}")
        else:
            print(f"{day.trade_date} FAILED {day.error}")

    summary = result.summary
    print(
        "summary "
        f"attempted_days={summary.attempted_days} "
        f"success_days={summary.success_days} "
        f"skipped_days={summary.skipped_days} "
        f"failed_days={summary.failed_days} "
        f"total_rows={summary.total_rows} "
        f"scoring_date={summary.scoring_date} "
        f"scored_rows={summary.scored_rows} "
        f"candidate_rows={summary.candidate_rows}"
    )
    if summary.warning:
        print(f"warning: {summary.warning}")


if __name__ == "__main__":
    main()
