from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.data.pipeline import run_daily_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily Taiwan stock quant pipeline.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--date", default=None, help="Trade date, e.g. 2026-05-08")
    parser.add_argument("--no-fetch", action="store_true", help="Use existing SQLite data only.")
    parser.add_argument(
        "--allow-fallback-latest",
        action="store_true",
        help="Use latest SQLite trading date if the requested date has no TWSE stock data.",
    )
    args = parser.parse_args()

    result = run_daily_pipeline(
        config_path=args.config,
        trade_date=args.date,
        fetch=not args.no_fetch,
        allow_fallback_latest=args.allow_fallback_latest,
    )
    print(
        "daily pipeline completed: "
        f"date={result.trade_date} "
        f"fetched_rows={result.fetched_rows} "
        f"scored_rows={result.scored_rows} "
        f"candidate_rows={result.candidate_rows}"
    )
    if result.fallback_date:
        reason = result.fallback_reason or "no trading data"
        print(f"fallback_date={result.fallback_date} reason={reason}")
    if result.message:
        print(f"warning: {result.message}")


if __name__ == "__main__":
    main()
