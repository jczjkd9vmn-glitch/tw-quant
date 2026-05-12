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
from tw_quant.reporting.export import export_latest_candidates


def main() -> None:
    parser = argparse.ArgumentParser(description="Export latest Taiwan stock candidates.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--output-dir", default=str(ROOT / "reports"))
    args = parser.parse_args()

    config = load_config(args.config)
    engine = create_db_engine(config["database"]["url"])
    init_db(engine)
    result = export_latest_candidates(engine, output_dir=args.output_dir, config=config)

    if result.warning:
        print(f"warning: {result.warning}")
        return

    print(result.candidates.to_string(index=False))
    print(f"candidates_csv={result.candidates_path}")
    print(f"risk_pass_candidates_csv={result.risk_pass_path}")
    print(f"data_fetch_status_csv={result.data_fetch_status_path}")
    print(
        "summary "
        f"trade_date={result.trade_date.date()} "
        f"candidate_rows={len(result.candidates)} "
        f"risk_pass_rows={len(result.risk_pass_candidates)}"
    )


if __name__ == "__main__":
    main()
