"""Local industry map updater."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.config import load_config


INDUSTRY_COLUMNS = [
    "stock_id",
    "stock_name",
    "market",
    "industry_main",
    "industry_sub",
    "source",
    "updated_at",
    "confidence",
    "fallback_used",
]
INDUSTRY_LOAD_COLUMNS = INDUSTRY_COLUMNS + ["industry", "sub_industry"]


def load_industry_map(
    data_dir: str | Path = "data",
    config_path: str | Path = "config.yaml",
) -> pd.DataFrame:
    """Load manually maintained industry mappings without inventing missing values."""

    config = load_config(config_path)
    industry_config = config.get("industry_enrichment", {})
    data_path = Path(data_dir)
    paths: list[Path] = []
    if bool(industry_config.get("fallback_to_local_csv", True)):
        paths.append(
            _resolve_map_path(
                industry_config.get("industry_map_path", data_path / "industry_map.csv"),
                data_path,
                config_path,
            )
        )
    reference_path = industry_config.get("reference_map_path", "data/reference/stock_industry_map.csv")
    paths.append(_resolve_map_path(reference_path, data_path, config_path))

    frames = [_read_industry_map(path) for path in paths]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=INDUSTRY_LOAD_COLUMNS)

    merged = pd.concat(frames, ignore_index=True)
    merged = _with_industry_aliases(merged)
    merged["stock_id"] = merged["stock_id"].astype(str).str.strip()
    merged["industry"] = merged["industry"].fillna("").astype(str).str.strip()
    merged = merged[(merged["stock_id"] != "") & (merged["industry"] != "")]
    return merged.drop_duplicates("stock_id", keep="last")[INDUSTRY_LOAD_COLUMNS].reset_index(drop=True)


def update_industry_map(
    data_dir: str | Path = "data",
    config_path: str | Path = "config.yaml",
) -> tuple[Path, str, int]:
    config = load_config(config_path)
    industry_config = config.get("industry_enrichment", {})
    target = _resolve_map_path(
        industry_config.get("industry_map_path", Path(data_dir) / "industry_map.csv"),
        Path(data_dir),
        config_path,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    existing = _read_industry_map(target)
    derived = _derive_from_local_files(Path(data_dir))
    if not derived.empty:
        merged = _merge(existing, derived)
        merged.to_csv(target, index=False, encoding="utf-8-sig")
        return target, "OK", len(merged)
    if not existing.empty:
        existing.to_csv(target, index=False, encoding="utf-8-sig")
        return target, "OK_WITH_FALLBACK", len(existing)
    pd.DataFrame(columns=INDUSTRY_COLUMNS).to_csv(target, index=False, encoding="utf-8-sig")
    return target, "EMPTY", 0


def _derive_from_local_files(data_dir: Path) -> pd.DataFrame:
    rows = []
    for filename in ["valuation.csv", "financials.csv", "monthly_revenue.csv", "institutional.csv"]:
        frame = _read_csv(data_dir / filename)
        if frame.empty or "stock_id" not in frame.columns or "industry" not in frame.columns:
            continue
        for _, row in frame.iterrows():
            industry = str(row.get("industry", "") or "").strip()
            stock_id = str(row.get("stock_id", "") or "").strip()
            if not stock_id or not industry:
                continue
            rows.append(
                {
                    "stock_id": stock_id,
                    "stock_name": str(row.get("stock_name", "")),
                    "market": str(row.get("market", row.get("market_type", ""))),
                    "industry_main": industry,
                    "industry_sub": str(row.get("industry_sub", row.get("sub_industry", ""))),
                    "source": f"local:{filename}",
                    "updated_at": pd.Timestamp.today().strftime("%Y-%m-%d"),
                    "confidence": 0.7,
                    "fallback_used": True,
                }
            )
    if not rows:
        return pd.DataFrame(columns=INDUSTRY_COLUMNS)
    return pd.DataFrame(rows, columns=INDUSTRY_COLUMNS).drop_duplicates("stock_id", keep="last")


def _read_industry_map(path: Path) -> pd.DataFrame:
    frame = _read_csv(path)
    if "industry_main" not in frame.columns and "industry" in frame.columns:
        frame["industry_main"] = frame["industry"]
    if "industry_sub" not in frame.columns and "sub_industry" in frame.columns:
        frame["industry_sub"] = frame["sub_industry"]
    if "market" not in frame.columns and "market_type" in frame.columns:
        frame["market"] = frame["market_type"]
    for column in INDUSTRY_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    return frame[INDUSTRY_COLUMNS].copy()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=INDUSTRY_COLUMNS)
    try:
        return pd.read_csv(path, dtype={"stock_id": str}, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=INDUSTRY_COLUMNS)


def _with_industry_aliases(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "industry" not in result.columns and "industry_main" in result.columns:
        result["industry"] = result["industry_main"]
    if "industry_main" not in result.columns and "industry" in result.columns:
        result["industry_main"] = result["industry"]
    if "sub_industry" not in result.columns and "industry_sub" in result.columns:
        result["sub_industry"] = result["industry_sub"]
    if "industry_sub" not in result.columns and "sub_industry" in result.columns:
        result["industry_sub"] = result["sub_industry"]
    for column in INDUSTRY_LOAD_COLUMNS:
        if column not in result.columns:
            result[column] = ""
    return result


def _merge(existing: pd.DataFrame, derived: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return derived[INDUSTRY_COLUMNS].copy()
    merged = pd.concat([existing, derived], ignore_index=True)
    merged["stock_id"] = merged["stock_id"].astype(str).str.strip()
    return merged.drop_duplicates("stock_id", keep="last")[INDUSTRY_COLUMNS].reset_index(drop=True)


def _resolve_map_path(path_value: object, data_dir: Path, config_path: str | Path) -> Path:
    path = Path(str(path_value))
    if path.is_absolute():
        return path
    config_root = Path(config_path).resolve().parent
    if path.parts and path.parts[0] == data_dir.name:
        return config_root / path
    return data_dir / path
