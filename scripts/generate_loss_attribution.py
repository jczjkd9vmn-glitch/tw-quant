from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.validation.loss_attribution import generate_loss_attribution


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate paper trading loss attribution report.")
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    result = generate_loss_attribution(reports_dir=args.reports_dir, trade_date=args.date)
    if result.warning:
        print(f"loss_attribution warning {result.warning}")
    print(f"loss_attribution_csv={result.output_path}")
    print(f"loss_attribution_rows={len(result.attribution)}")


if __name__ == "__main__":
    main()
