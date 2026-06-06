"""Shared benchmark source selection for reporting diagnostics.

The helper intentionally accepts only explicit official index data from
``data/market_indices.csv``. It does not infer official TAIEX/TPEx index rows
from ``daily_prices`` symbol or name keywords because ETF/ETN names can contain
the same words.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from tw_quant.data.trading_calendar import filter_trading_days


ACCEPTED_INDEX_IDS = {"TAIEX_TR", "TAIEX", "TPEx", "TPEX"}
OFFICIAL_INDEX_PRIORITY = ["TAIEX_TR", "TAIEX", "TPEx", "TPEX"]
NO_OFFICIAL_INDEX_WARNING = "缺少正式加權 / 櫃買指數資料，未使用正式加權指數資料，也未使用正式大盤指數。"


def select_benchmark_snapshot(
    report_dir: str | Path,
    selected_date: str | pd.Timestamp | None = None,
) -> dict[str, object]:
    """Select a benchmark with one consistent priority order for all reports."""

    report_path = Path(report_dir)
    target = pd.to_datetime(selected_date, errors="coerce") if selected_date is not None else pd.NaT
    target_date = None if pd.isna(target) else target

    official = _official_index_snapshot(report_path, target_date)
    if official is not None:
        return official

    sector_strength = _read_sector_strength(report_path)
    fallback_warning = NO_OFFICIAL_INDEX_WARNING
    if not sector_strength.empty and "stock_id" in sector_strength.columns:
        frame = sector_strength.copy()
        frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
        etf_0050 = frame[frame["stock_id"] == "0050"]
        if not etf_0050.empty:
            returns = {
                "1d": None,
                "5d": _normalized_return(etf_0050.iloc[0].get("stock_return_5d")),
                "20d": _normalized_return(etf_0050.iloc[0].get("stock_return_20d")),
                "total": None,
            }
            quality_warning = _fallback_quality_warning(returns, "0050 fallback")
            if not quality_warning:
                return {
                    "source_label": "0050 fallback",
                    "warning": f"{fallback_warning} benchmark fallback 使用 0050。",
                    "returns": returns,
                    "status": "OK_WITH_WARNING",
                }
            fallback_warning = f"{fallback_warning} {quality_warning}"

        if {"market_return_5d", "market_return_20d"}.issubset(frame.columns):
            market_rows = frame.dropna(subset=["market_return_5d", "market_return_20d"], how="all")
            if not market_rows.empty:
                returns = {
                    "1d": None,
                    "5d": _normalized_return(market_rows.iloc[0].get("market_return_5d")),
                    "20d": _normalized_return(market_rows.iloc[0].get("market_return_20d")),
                    "total": None,
                }
                quality_warning = _fallback_quality_warning(returns, "全市場等權 fallback")
                if not quality_warning:
                    return {
                        "source_label": "全市場等權 fallback",
                        "warning": f"{fallback_warning} benchmark fallback 使用全市場等權報酬。",
                        "returns": returns,
                        "status": "OK_WITH_WARNING",
                    }
                fallback_warning = f"{fallback_warning} {quality_warning}"

    return {
        "source_label": "benchmark 資料不足",
        "warning": f"{fallback_warning} 缺少可信 0050 或全市場等權 fallback，無法計算 benchmark alpha。",
        "returns": {"1d": None, "5d": None, "20d": None, "total": None},
        "status": "DATA_INSUFFICIENT",
    }


def benchmark_return_for_window(
    report_dir: str | Path,
    selected_date: str | pd.Timestamp | None,
    return_days: int,
) -> dict[str, object]:
    snapshot = select_benchmark_snapshot(report_dir, selected_date)
    window = "20d" if return_days >= 20 else ("5d" if return_days >= 5 else "1d")
    returns = snapshot.get("returns", {}) if isinstance(snapshot.get("returns"), dict) else {}
    return {
        "return": _benchmark_value_for_window(returns, window),
        "window": window,
        "source": snapshot.get("source_label", "benchmark 資料不足"),
        "warning": snapshot.get("warning", ""),
        "status": snapshot.get("status", "DATA_INSUFFICIENT"),
    }


def _official_index_snapshot(report_dir: Path, selected_date: pd.Timestamp | None) -> dict[str, object] | None:
    frame = _read_market_indices(report_dir)
    if frame.empty:
        return None
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.dropna(subset=["trade_date"])
    if selected_date is not None:
        frame = frame[frame["trade_date"] <= selected_date]
    if frame.empty:
        return None
    frame["index_id"] = frame["index_id"].astype(str).str.strip()
    frame = frame[frame["index_id"].isin(ACCEPTED_INDEX_IDS)].copy()
    if "is_official" in frame.columns:
        frame = frame[frame["is_official"].apply(_truthy)].copy()
    if frame.empty:
        return None
    frame = filter_trading_days(frame)
    if frame.empty:
        return None

    for index_id in OFFICIAL_INDEX_PRIORITY:
        subset = frame[frame["index_id"] == index_id].copy()
        if subset.empty:
            continue
        subset["close"] = pd.to_numeric(subset["close"], errors="coerce")
        subset = subset.dropna(subset=["close"]).sort_values("trade_date")
        if subset.empty:
            continue
        close = subset["close"].reset_index(drop=True)
        returns = {
            "1d": _period_return(close, 1),
            "5d": _period_return(close, 5),
            "20d": _period_return(close, 20),
            "total": None,
        }
        warning = ""
        source_label = "正式加權報酬指數" if index_id == "TAIEX_TR" else "正式加權指數"
        if index_id != "TAIEX_TR":
            warning = "使用正式 TAIEX 價格指數，非 total return；alpha 需保守解讀。"
        return {
            "source_label": source_label,
            "warning": warning,
            "returns": returns,
            "status": "OK_WITH_WARNING" if warning else "OK",
        }
    return None


def _read_market_indices(report_dir: Path) -> pd.DataFrame:
    for path in _candidate_market_index_paths(report_dir):
        frame = _read_csv(path)
        if not frame.empty and {"trade_date", "index_id", "close"}.issubset(frame.columns):
            return frame
    return pd.DataFrame()


def _candidate_market_index_paths(report_dir: Path) -> Iterable[Path]:
    yield report_dir / "market_indices.csv"
    yield report_dir.parent / "data" / "market_indices.csv"
    yield Path("data") / "market_indices.csv"


def _read_sector_strength(report_dir: Path) -> pd.DataFrame:
    for path in [report_dir / "sector_strength.csv", report_dir.parent / "data" / "sector_strength.csv"]:
        frame = _read_csv(path)
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig", dtype={"stock_id": str, "index_id": str})
    except Exception:
        return pd.DataFrame()


def _fallback_quality_warning(returns: dict[str, float | None], label: str) -> str:
    issues = []
    for window, limit in {"5d": 0.20, "20d": 0.50}.items():
        value = returns.get(window)
        if value is not None and abs(float(value)) > limit:
            issues.append(f"{label} {window} 報酬 {value:.4f} 超過資料品質門檻 {limit:.2f}")
    return "；".join(issues)


def _benchmark_value_for_window(returns: dict[str, object], window: str) -> float | None:
    candidates = [window]
    if window == "1d":
        candidates.extend(["5d", "20d"])
    elif window == "5d":
        candidates.append("20d")
    for candidate in candidates:
        value = _normalized_return(returns.get(candidate))
        if value is not None:
            return value
    return None


def _period_return(close: pd.Series, days: int) -> float | None:
    clean = pd.to_numeric(close, errors="coerce").dropna().reset_index(drop=True)
    if len(clean) <= days:
        return None
    base = clean.iloc[-days - 1]
    if abs(float(base)) < 0.000001:
        return None
    return round(float(clean.iloc[-1] / base - 1.0), 6)


def _normalized_return(value: object) -> float | None:
    number = _num(value)
    if number is None:
        return None
    if abs(number) > 0.5 and abs(number) <= 100:
        return number / 100.0
    if abs(number) > 100:
        return None
    return number


def _num(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}
