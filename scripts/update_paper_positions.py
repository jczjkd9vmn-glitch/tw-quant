from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.config import load_config
from tw_quant.data.database import create_db_engine, init_db
from tw_quant.trading.paper_update import update_paper_positions


def main() -> None:
    parser = argparse.ArgumentParser(description="Update paper trading positions with daily close prices.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--date", default=None, help="Valuation date, e.g. 20260508. Defaults to latest price date.")
    parser.add_argument("--capital", type=float, default=1_000_000)
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    args = parser.parse_args()

    config = load_config(args.config)
    engine = create_db_engine(config["database"]["url"])
    init_db(engine)
    result = update_paper_positions(
        engine=engine,
        reports_dir=args.reports_dir,
        trade_date=args.date,
        capital=args.capital,
    )

    if result.warning:
        print(f"warning: {result.warning}")
        return

    print(result.portfolio.to_string(index=False))
    print(f"portfolio_csv={result.portfolio_path}")
    print(f"summary_csv={result.summary_path}")
    print(f"paper_trades_csv={result.trades_path}")
    print(result.summary.to_string(index=False))


if __name__ == "__main__":
    main()
