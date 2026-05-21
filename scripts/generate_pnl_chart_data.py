from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.reporting.dashboard_data import generate_pnl_chart_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate PnL chart data for the static report.")
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--date", default=None)
    parser.add_argument("--lookback", type=int, default=20)
    args = parser.parse_args()

    result = generate_pnl_chart_data(
        reports_dir=args.reports_dir,
        trade_date=args.date,
        lookback=args.lookback,
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
