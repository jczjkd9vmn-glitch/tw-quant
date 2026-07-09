from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tw_quant.data_sources.official_market_indices import (  # noqa: E402
    DEFAULT_HISTORY_DAYS,
    fetch_official_market_indices,
    update_market_indices_csv,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch official TWSE/TPEx market indices into data/market_indices.csv."
    )
    parser.add_argument("--output", default=str(ROOT / "data" / "market_indices.csv"))
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--source", choices=["all", "twse", "tpex"], default="all")
    parser.add_argument(
        "--history-days",
        type=int,
        default=DEFAULT_HISTORY_DAYS,
        help="Target official closing rows per index. 253 rows are required for a 252-trading-day alpha.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print summary without writing the CSV.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    sources = ["twse", "tpex"] if args.source == "all" else [args.source]
    fetched = fetch_official_market_indices(
        timeout_seconds=args.timeout, sources=sources, history_days=args.history_days
    )
    if args.dry_run:
        print(
            "market_indices DRY_RUN "
            f"rows={len(fetched)} "
            f"history_days={args.history_days} "
            f"sources={','.join(sorted(set(fetched['index_id'].astype(str))))}"
        )
        return 0

    output_path = Path(args.output)
    merged = update_market_indices_csv(output_path, fetched)
    official_rows = int(merged["is_official"].astype(bool).sum()) if "is_official" in merged.columns else 0
    index_ids = ",".join(sorted(set(merged["index_id"].astype(str)))) if not merged.empty else ""
    print(
        "market_indices OK "
        f"rows={len(merged)} "
        f"official_rows={official_rows} "
        f"history_days={args.history_days} "
        f"index_ids={index_ids} "
        f"output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
