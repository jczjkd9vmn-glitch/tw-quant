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
from tw_quant.trading.paper import run_paper_trade
from tw_quant.trading.pending import execute_pending_orders


def main() -> None:
    parser = argparse.ArgumentParser(description="Create paper trading positions from risk-passed candidates.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--capital", type=float, default=1_000_000)
    parser.add_argument("--mode", choices=["signal", "execute"], default="signal")
    args = parser.parse_args()

    if args.mode == "execute":
        config = load_config(args.config)
        engine = create_db_engine(config["database"]["url"])
        init_db(engine)
        result = execute_pending_orders(
            engine=engine,
            reports_dir=args.reports_dir,
            capital=args.capital,
            trading_cost=config.get("trading_cost", {}),
        )
        for warning in result.warnings:
            print(f"warning: {warning}")
        print(
            "summary "
            f"pending_orders={len(result.pending_orders[result.pending_orders['status'] == 'PENDING'])} "
            f"executed_orders={len(result.executed_orders)} "
            f"skipped_orders={len(result.skipped_orders)}"
        )
        print(f"paper_trades_csv={result.trades_path}")
        return

    result = run_paper_trade(reports_dir=args.reports_dir, capital=args.capital)
    if result.warning:
        print(f"warning: {result.warning}")
        return

    if result.pending_orders.empty:
        print("warning: no pending orders created")
    else:
        print(result.pending_orders.to_string(index=False))

    print(f"source_report={result.source_report}")
    print(f"pending_orders_csv={result.pending_orders_path}")
    print(f"paper_trades_csv={result.trades_path}")
    print(
        "summary "
        f"trade_date={result.trade_date.date()} "
        f"pending_orders={len(result.pending_orders[result.pending_orders['status'] == 'PENDING'])} "
        f"open_positions={len(result.positions)} "
    )


if __name__ == "__main__":
    main()
