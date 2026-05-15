"""Small JSON cache for official data provider outputs."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def cache_path(cache_dir: str | Path, source_name: str, date_label: str) -> Path:
    safe_source = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in source_name)
    return Path(cache_dir) / f"{safe_source}_{date_label}.json"


def read_cache(
    cache_dir: str | Path,
    source_name: str,
    date_label: str,
    columns: list[str],
) -> tuple[pd.DataFrame | None, str]:
    path = cache_path(cache_dir, source_name, date_label)
    if not path.exists():
        return None, ""
    try:
        records = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"cache damaged: {type(exc).__name__}"
    frame = pd.DataFrame(records)
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    return frame[columns].copy(), ""


def write_cache(cache_dir: str | Path, source_name: str, date_label: str, frame: pd.DataFrame) -> Path:
    path = cache_path(cache_dir, source_name, date_label)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(frame.to_dict(orient="records"), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return path
