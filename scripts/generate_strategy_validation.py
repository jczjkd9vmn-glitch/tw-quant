from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.config import load_config
from tw_quant.validation.strategy_validation import generate_strategy_validation


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate advisory strategy validation report.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    validation_config = config.get("strategy_validation", {})
    result = generate_strategy_validation(
        reports_dir=args.reports_dir,
        trade_date=args.date,
        min_trades_required=int(validation_config.get("min_trades_required", 10)),
    )
    if result.warning:
        print(f"strategy_validation warning {result.warning}")
    if result.output_path:
        print(f"strategy_validation_csv={result.output_path}")
    print(f"strategy_validation_rows={len(result.validation)}")


if __name__ == "__main__":
    main()

