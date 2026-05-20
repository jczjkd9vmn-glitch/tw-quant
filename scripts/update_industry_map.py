from __future__ import annotations

import argparse
from pathlib import Path
import sys



ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.config import load_config
from tw_quant.enrichment.industry import update_industry_map


def main() -> None:
    parser = argparse.ArgumentParser(description="Update local industry_map.csv.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    args = parser.parse_args()
    path, status, rows = update_industry_map(data_dir=args.data_dir, config_path=args.config)
    print(f"industry_map_status={status} rows={rows} path={path}")


if __name__ == "__main__":
    main()
