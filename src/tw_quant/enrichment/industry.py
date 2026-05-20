"""Local industry map updater."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.config import load_config


INDUSTRY_COLUMNS = ["stock_id", "stock_name", "industry", "sub_industry", "market_type", "source", "updated_at"]


def update_industry_map(
    data_dir: str | Path = "data",
    config_path: str | Path = "config.yaml",
) -> tuple[Path, str, int]:
    config = load_config(config_path)
    industry_config = config.get("industry_enrichment", {})
    target = Path(industry_config.get("industry_map_path", Path(data_dir) / "industry_map.csv"))
    if not target.is_absolute():
        target = Path(data_dir) / target.name
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
                    "industry": industry,
                    "sub_industry": str(row.get("sub_industry", "")),
                    "market_type": str(row.get("market_type", "")),
                    "source": f"local:{filename}",
                    "updated_at": pd.Timestamp.today().strftime("%Y-%m-%d"),
                }
            )
    if not rows:
        return pd.DataFrame(columns=INDUSTRY_COLUMNS)
    return pd.DataFrame(rows, columns=INDUSTRY_COLUMNS).drop_duplicates("stock_id", keep="last")


def _read_industry_map(path: Path) -> pd.DataFrame:
    frame = _read_csv(path)
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


def _merge(existing: pd.DataFrame, derived: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return derived[INDUSTRY_COLUMNS].copy()
    merged = pd.concat([existing, derived], ignore_index=True)
    merged["stock_id"] = merged["stock_id"].astype(str).str.strip()
    return merged.drop_duplicates("stock_id", keep="last")[INDUSTRY_COLUMNS].reset_index(drop=True)
