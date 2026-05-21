from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.reporting.dashboard_data import generate_market_recap


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Taiwan market recap report.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    result = generate_market_recap(
        reports_dir=args.reports_dir,
        config_path=args.config,
        trade_date=args.date,
    )
    if result.warning:
        print(f"warning: {result.warning}")
    print(
        "summary "
        f"trade_date={result.trade_date} "
        f"status={result.status} "
        f"rows={len(result.frame)} "
        f"output={result.output_path}"
    )


if __name__ == "__main__":
    main()
