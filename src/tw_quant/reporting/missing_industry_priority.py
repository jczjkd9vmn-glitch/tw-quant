"""Prioritize stocks still using market-relative industry fallback."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd


MISSING_INDUSTRY_PRIORITY_COLUMNS = [
    "stock_id",
    "stock_name",
    "market_type",
    "latest_relative_mode",
    "fallback_reason",
    "appear_in_candidates_count",
    "appear_in_trading_decisions_count",
    "appear_in_risk_pass_count",
    "appear_in_position_review_count",
    "appear_in_ai_enrichment_count",
    "recent_appearance_count",
    "liquidity_score",
    "avg_volume",
    "turnover_value",
    "last_seen_date",
    "priority_score",
    "priority_level",
    "suggested_action",
]

REPORT_SPECS = {
    "appear_in_candidates_count": "candidates_*.csv",
    "appear_in_trading_decisions_count": "trading_decisions_*.csv",
    "appear_in_risk_pass_count": "risk_pass_candidates_*.csv",
    "appear_in_position_review_count": "position_review_summary_*.csv",
    "appear_in_ai_enrichment_count": "ai_enrichment_*.csv",
}


@dataclass(frozen=True)
class MissingIndustryPriorityResult:
    priority: pd.DataFrame
    output_path: Path | None
    warning: str = ""


def generate_missing_industry_priority_report(
    data_dir: str | Path = "data",
    reports_dir: str | Path = "reports",
    trade_date: str | pd.Timestamp | None = None,
    recent_days: int = 7,
) -> MissingIndustryPriorityResult:
    data_path = Path(data_dir)
    report_dir = Path(reports_dir)
    sector_strength = _read_csv(data_path / "sector_strength.csv")
    liquidity = _read_csv(data_path / "liquidity.csv")
    report_frames = _read_recent_report_frames(report_dir, trade_date, recent_days)
    priority = build_missing_industry_priority_report(
        sector_strength=sector_strength,
        report_frames=report_frames,
        liquidity=liquidity,
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    output_path = report_dir / "missing_industry_priority.csv"
    priority.to_csv(output_path, index=False, encoding="utf-8")
    warning = "no market_relative_fallback rows" if priority.empty else ""
    return MissingIndustryPriorityResult(priority=priority, output_path=output_path, warning=warning)


def build_missing_industry_priority_report(
    sector_strength: pd.DataFrame,
    report_frames: dict[str, pd.DataFrame] | None = None,
    liquidity: pd.DataFrame | None = None,
) -> pd.DataFrame:
    fallback = _latest_fallback_rows(sector_strength)
    if fallback.empty:
        return pd.DataFrame(columns=MISSING_INDUSTRY_PRIORITY_COLUMNS)

    report_frames = report_frames or {}
    liquidity_lookup = _latest_liquidity_lookup(liquidity if liquidity is not None else pd.DataFrame())
    counts = {column: _count_stock_ids(report_frames.get(column, pd.DataFrame())) for column in REPORT_SPECS}
    last_seen = _last_seen_dates(fallback, report_frames)
    report_market_types = _report_market_types(report_frames)

    rows: list[dict[str, object]] = []
    for _, row in fallback.iterrows():
        stock_id = str(row.get("stock_id", "") or "").strip()
        if not stock_id:
            continue
        appearance_counts = {column: int(counts[column].get(stock_id, 0)) for column in REPORT_SPECS}
        recent_appearance_count = sum(appearance_counts.values())
        liquidity_row = liquidity_lookup.get(stock_id, {})
        liquidity_score = _first_number(liquidity_row, ["liquidity_score"])
        priority_score = calculate_priority_score(
            appear_in_candidates_count=appearance_counts["appear_in_candidates_count"],
            appear_in_trading_decisions_count=appearance_counts["appear_in_trading_decisions_count"],
            appear_in_risk_pass_count=appearance_counts["appear_in_risk_pass_count"],
            appear_in_position_review_count=appearance_counts["appear_in_position_review_count"],
            appear_in_ai_enrichment_count=appearance_counts["appear_in_ai_enrichment_count"],
            recent_appearance_count=recent_appearance_count,
            liquidity_score=liquidity_score,
        )
        priority_level = priority_level_for_score(priority_score)
        fallback_reason = str(row.get("sector_strength_warning", "") or "").strip()
        if not fallback_reason:
            fallback_reason = "缺少產業分類，使用全市場相對強弱"
        rows.append(
            {
                "stock_id": stock_id,
                "stock_name": str(row.get("stock_name", "") or ""),
                "market_type": report_market_types.get(stock_id) or _derived_market_type(stock_id),
                "latest_relative_mode": str(row.get("sector_strength_mode", "") or ""),
                "fallback_reason": fallback_reason,
                **appearance_counts,
                "recent_appearance_count": recent_appearance_count,
                "liquidity_score": liquidity_score,
                "avg_volume": _first_number(liquidity_row, ["avg_volume_20d", "latest_volume"]),
                "turnover_value": _first_number(liquidity_row, ["latest_turnover", "avg_turnover_20d"]),
                "last_seen_date": last_seen.get(stock_id, _date_text(row.get("trade_date"))),
                "priority_score": priority_score,
                "priority_level": priority_level,
                "suggested_action": suggested_action_for_level(priority_level),
            }
        )

    result = pd.DataFrame(rows, columns=MISSING_INDUSTRY_PRIORITY_COLUMNS)
    if result.empty:
        return pd.DataFrame(columns=MISSING_INDUSTRY_PRIORITY_COLUMNS)
    result["_liquidity_sort"] = pd.to_numeric(result["liquidity_score"], errors="coerce").fillna(-1)
    result = result.sort_values(
        ["priority_score", "recent_appearance_count", "_liquidity_sort", "stock_id"],
        ascending=[False, False, False, True],
    )
    return result.drop(columns=["_liquidity_sort"]).reset_index(drop=True)


def calculate_priority_score(
    *,
    appear_in_candidates_count: int = 0,
    appear_in_trading_decisions_count: int = 0,
    appear_in_risk_pass_count: int = 0,
    appear_in_position_review_count: int = 0,
    appear_in_ai_enrichment_count: int = 0,
    recent_appearance_count: int = 0,
    liquidity_score: float | None = None,
) -> int:
    score = 0
    if appear_in_candidates_count > 0:
        score += 5
    if appear_in_trading_decisions_count > 0:
        score += 4
    if appear_in_risk_pass_count > 0:
        score += 4
    if appear_in_position_review_count > 0:
        score += 5
    if appear_in_ai_enrichment_count > 0:
        score += 2
    score += min(max(int(recent_appearance_count), 0), 7)
    score += _liquidity_points(liquidity_score)
    return score


def priority_level_for_score(score: int | float) -> str:
    if score >= 8:
        return "HIGH"
    if score >= 4:
        return "MEDIUM"
    return "LOW"


def suggested_action_for_level(level: str) -> str:
    return {
        "HIGH": "優先查證並補產業分類",
        "MEDIUM": "可排入下一批補資料",
        "LOW": "暫緩補資料",
    }.get(level, "暫緩補資料")


def _read_recent_report_frames(
    report_dir: Path,
    trade_date: str | pd.Timestamp | None,
    recent_days: int,
) -> dict[str, pd.DataFrame]:
    dated_paths: list[tuple[str, Path, pd.Timestamp]] = []
    for count_column, pattern in REPORT_SPECS.items():
        for path in report_dir.glob(pattern):
            parsed = _date_from_path(path)
            if parsed is not None:
                dated_paths.append((count_column, path, parsed))
    if not dated_paths:
        return {column: pd.DataFrame() for column in REPORT_SPECS}

    latest_date = _parse_date(trade_date) or max(parsed for _, _, parsed in dated_paths)
    cutoff = latest_date - pd.Timedelta(days=max(int(recent_days), 1) - 1)
    frames: dict[str, list[pd.DataFrame]] = {column: [] for column in REPORT_SPECS}
    for count_column, path, parsed in dated_paths:
        if parsed < cutoff or parsed > latest_date:
            continue
        frame = _read_csv(path)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["_report_date"] = parsed.strftime("%Y-%m-%d")
        frames[count_column].append(frame)
    return {
        column: pd.concat(items, ignore_index=True) if items else pd.DataFrame() for column, items in frames.items()
    }


def _latest_fallback_rows(sector_strength: pd.DataFrame) -> pd.DataFrame:
    if sector_strength.empty or "stock_id" not in sector_strength.columns:
        return pd.DataFrame(columns=sector_strength.columns)
    frame = sector_strength.copy()
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    mode = frame.get("sector_strength_mode", pd.Series([""] * len(frame))).fillna("").astype(str)
    warning = frame.get("sector_strength_warning", pd.Series([""] * len(frame))).fillna("").astype(str)
    frame = frame[
        mode.str.lower().eq("market_relative_fallback") | warning.str.contains("缺少產業分類", na=False)
    ].copy()
    if frame.empty:
        return frame
    frame["_trade_date"] = pd.to_datetime(frame.get("trade_date"), errors="coerce")
    frame = frame.sort_values(["stock_id", "_trade_date"]).drop_duplicates("stock_id", keep="last")
    return frame.drop(columns=["_trade_date"]).reset_index(drop=True)


def _latest_liquidity_lookup(liquidity: pd.DataFrame) -> dict[str, dict[str, object]]:
    if liquidity.empty or "stock_id" not in liquidity.columns:
        return {}
    frame = liquidity.copy()
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["_trade_date"] = pd.to_datetime(frame.get("trade_date"), errors="coerce")
    frame = frame.sort_values(["stock_id", "_trade_date"]).drop_duplicates("stock_id", keep="last")
    return frame.drop(columns=["_trade_date"]).set_index("stock_id").to_dict("index")


def _count_stock_ids(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or "stock_id" not in frame.columns:
        return {}
    stock_ids = frame["stock_id"].fillna("").astype(str).str.strip()
    stock_ids = stock_ids[stock_ids != ""]
    return stock_ids.value_counts().to_dict()


def _last_seen_dates(fallback: pd.DataFrame, report_frames: dict[str, pd.DataFrame]) -> dict[str, str]:
    seen: dict[str, pd.Timestamp] = {}
    if "stock_id" in fallback.columns:
        for _, row in fallback.iterrows():
            stock_id = str(row.get("stock_id", "") or "").strip()
            parsed = _parse_date(row.get("trade_date"))
            if stock_id and parsed is not None:
                seen[stock_id] = parsed
    for frame in report_frames.values():
        if frame.empty or "stock_id" not in frame.columns or "_report_date" not in frame.columns:
            continue
        for _, row in frame.iterrows():
            stock_id = str(row.get("stock_id", "") or "").strip()
            parsed = _parse_date(row.get("_report_date"))
            if not stock_id or parsed is None:
                continue
            if stock_id not in seen or parsed > seen[stock_id]:
                seen[stock_id] = parsed
    return {stock_id: parsed.strftime("%Y-%m-%d") for stock_id, parsed in seen.items()}


def _report_market_types(report_frames: dict[str, pd.DataFrame]) -> dict[str, str]:
    market_types: dict[str, str] = {}
    for frame in report_frames.values():
        if frame.empty or "stock_id" not in frame.columns or "market_type" not in frame.columns:
            continue
        for _, row in frame.iterrows():
            stock_id = str(row.get("stock_id", "") or "").strip()
            market_type = str(row.get("market_type", "") or "").strip().upper()
            if stock_id and market_type and market_type != "NAN":
                market_types[stock_id] = market_type
    return market_types


def _derived_market_type(stock_id: str) -> str:
    if stock_id.startswith("020"):
        return "ETN"
    if stock_id.startswith("00"):
        return "ETF"
    return "UNKNOWN"


def _liquidity_points(liquidity_score: float | None) -> int:
    if liquidity_score is None:
        return 0
    if liquidity_score >= 90:
        return 3
    if liquidity_score >= 75:
        return 2
    if liquidity_score >= 60:
        return 1
    return 0


def _first_number(row: dict[str, object], columns: list[str]) -> float | None:
    for column in columns:
        value = _to_float(row.get(column))
        if value is not None:
            return value
    return None


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return float(number)


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    return _parse_date(match.group(1))


def _parse_date(value: object) -> pd.Timestamp | None:
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        parsed = pd.to_datetime(text, errors="coerce")
    return None if pd.isna(parsed) else parsed


def _date_text(value: object) -> str:
    parsed = _parse_date(value)
    return "" if parsed is None else parsed.strftime("%Y-%m-%d")


def _read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()
