from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.enrichment.report import generate_ai_enrichment


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate rule-based/AI enrichment report.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    result = generate_ai_enrichment(
        reports_dir=args.reports_dir,
        data_dir=args.data_dir,
        config_path=args.config,
        trade_date=args.date,
    )
    if result.warning:
        print(f"warning: {result.warning}")
    print(
        "summary "
        f"trade_date={result.trade_date} "
        f"rows={len(result.enrichment)} "
        f"output={result.output_path} "
        f"cache={result.cache_path}"
    )


if __name__ == "__main__":
    main()
