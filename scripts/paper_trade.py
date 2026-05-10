from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.trading.paper import run_paper_trade


def main() -> None:
    parser = argparse.ArgumentParser(description="Create paper trading positions from risk-passed candidates.")
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--capital", type=float, default=1_000_000)
    args = parser.parse_args()

    result = run_paper_trade(reports_dir=args.reports_dir, capital=args.capital)
    if result.warning:
        print(f"warning: {result.warning}")
        return

    if result.new_positions.empty:
        print("warning: no new paper positions created")
    else:
        print(result.new_positions.to_string(index=False))

    print(f"source_report={result.source_report}")
    print(f"positions_csv={result.positions_path}")
    print(f"paper_trades_csv={result.trades_path}")
    print(
        "summary "
        f"trade_date={result.trade_date.date()} "
        f"new_positions={len(result.new_positions)} "
        f"open_positions={len(result.positions)} "
        f"skipped_existing={len(result.skipped_existing)}"
    )


if __name__ == "__main__":
    main()
