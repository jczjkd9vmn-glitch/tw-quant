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
    args = parser.parse_args()

    result = run_daily_pipeline(
        config_path=args.config,
        trade_date=args.date,
        fetch=not args.no_fetch,
    )
    print(
        "daily pipeline completed: "
        f"date={result.trade_date} "
        f"fetched_rows={result.fetched_rows} "
        f"scored_rows={result.scored_rows} "
        f"candidate_rows={result.candidate_rows}"
    )
    if result.message:
        print(f"warning: {result.message}")


if __name__ == "__main__":
    main()
