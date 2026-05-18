from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.decision.engine import decision_counts, generate_trading_decisions


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate advisory trading decisions.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    result = generate_trading_decisions(
        reports_dir=args.reports_dir,
        config_path=args.config,
        trade_date=args.date,
    )
    if result.warning:
        print(f"trading_decisions warning {result.warning}")
    if result.output_path:
        print(f"trading_decisions_csv={result.output_path}")
    counts = decision_counts(result.decisions)
    print(
        "trading_decisions "
        f"rows={len(result.decisions)} "
        f"buy_candidate_count={counts['buy_candidate_count']} "
        f"watch_only_count={counts['watch_only_count']} "
        f"no_trade_count={counts['no_trade_count']} "
        f"hold_count={counts['hold_count']} "
        f"reduce_count={counts['reduce_count']} "
        f"exit_review_count={counts['exit_review_count']}"
    )


if __name__ == "__main__":
    main()
