"""Small trading-day helpers for local data-quality gates."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def is_weekend(value: object) -> bool:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return False
    return int(parsed.dayofweek) >= 5


def is_trading_day(value: object, calendar_path: str | Path | None = None) -> bool:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return False
    target = parsed.strftime("%Y-%m-%d")
    if calendar_path is not None:
        override = _calendar_override(Path(calendar_path), target)
        if override is not None:
            return override
    return not is_weekend(parsed)


def filter_trading_days(
    frame: pd.DataFrame,
    date_column: str = "trade_date",
    calendar_path: str | Path | None = None,
) -> pd.DataFrame:
    if frame.empty or date_column not in frame.columns:
        return frame.copy()
    output = frame.copy()
    dates = pd.to_datetime(output[date_column], errors="coerce")
    mask = dates.apply(lambda value: is_trading_day(value, calendar_path=calendar_path))
    output[date_column] = dates
    return output[mask.fillna(False)].copy()


def latest_trading_day(values: object, end_date: object | None = None, calendar_path: str | Path | None = None):
    dates = pd.to_datetime(pd.Series(list(values)), errors="coerce").dropna()
    if end_date is not None:
        parsed_end = pd.to_datetime(end_date, errors="coerce")
        if not pd.isna(parsed_end):
            dates = dates[dates <= parsed_end]
    if dates.empty:
        return None
    dates = dates.drop_duplicates().sort_values(ascending=False)
    for value in dates:
        if is_trading_day(value, calendar_path=calendar_path):
            return value.date()
    return None


def _calendar_override(path: Path, target: str) -> bool | None:
    if not path.exists():
        return None
    try:
        frame = pd.read_csv(path, dtype={"date": str})
    except Exception:
        return None
    if frame.empty or not {"date", "is_trading_day"}.issubset(frame.columns):
        return None
    rows = frame[frame["date"].astype(str).str.strip() == target]
    if rows.empty:
        return None
    return _truthy(rows.iloc[-1].get("is_trading_day"))


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}
