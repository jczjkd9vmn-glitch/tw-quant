"""Dashboard-side derived reports for PnL charts and Taiwan market recap."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from tw_quant.config import load_config
from tw_quant.data.database import create_db_engine, init_db, load_price_history


@dataclass(frozen=True)
class DashboardReportResult:
    trade_date: str
    output_path: Path | None
    frame: pd.DataFrame
    status: str = "OK"
    warning: str = ""


PNL_CHART_COLUMNS = [
    "trade_date",
    "total_equity",
    "unrealized_pnl",
    "realized_pnl_after_cost",
    "total_equity_after_cost",
    "today_total_pnl",
    "total_return_pct",
    "data_quality_note",
]


MARKET_RECAP_COLUMNS = [
    "trade_date",
    "market_regime_score",
    "regime_label",
    "twse_index",
    "tpex_index",
    "advancers",
    "decliners",
    "unchanged",
    "limit_up_count",
    "limit_down_count",
    "market_breadth_summary",
    "recap_summary",
    "fallback_used",
    "data_quality_note",
]


def generate_pnl_chart_data(
    reports_dir: str | Path = "reports",
    trade_date: str | None = None,
    lookback: int = 20,
    current_summary: dict[str, Any] | None = None,
) -> DashboardReportResult:
    """Create a compact PnL time series for the static HTML report."""

    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    summaries = _read_all_reports(report_dir, "daily_summary_*.csv")
    if current_summary:
        current_frame = pd.DataFrame([current_summary])
        summaries = pd.concat([summaries, current_frame], ignore_index=True, sort=False) if not summaries.empty else current_frame
    if summaries.empty:
        target = trade_date or pd.Timestamp.today().strftime("%Y-%m-%d")
        frame = pd.DataFrame(columns=PNL_CHART_COLUMNS)
        output = report_dir / f"pnl_chart_data_{_date_label(target)}.csv"
        frame.to_csv(output, index=False, encoding="utf-8-sig")
        return DashboardReportResult(target, output, frame, status="EMPTY", warning="損益資料不足")

    summaries = summaries.copy()
    summaries["trade_date"] = pd.to_datetime(summaries["trade_date"], errors="coerce")
    summaries = summaries.dropna(subset=["trade_date"]).sort_values("trade_date").tail(lookback)
    total_capital = _first_number(summaries.iloc[-1].to_dict(), "total_capital")
    if total_capital is None:
        total_capital = _first_number(summaries.iloc[-1].to_dict(), "initial_capital") or 1_000_000.0
    rows = []
    for _, row in summaries.iterrows():
        total_equity = _to_float(row.get("total_equity"))
        total_equity_after_cost = _to_float(row.get("total_equity_after_cost")) or total_equity
        unrealized = _to_float(row.get("unrealized_pnl")) or 0.0
        realized_after_cost = _to_float(row.get("realized_pnl_after_cost_today"))
        if realized_after_cost is None:
            realized_after_cost = _to_float(row.get("realized_pnl_after_cost")) or 0.0
        today_total = unrealized + realized_after_cost
        total_return_pct = (
            (total_equity_after_cost - total_capital) / total_capital
            if total_equity_after_cost is not None and total_capital
            else None
        )
        rows.append(
            {
                "trade_date": row["trade_date"].strftime("%Y-%m-%d"),
                "total_equity": total_equity,
                "unrealized_pnl": unrealized,
                "realized_pnl_after_cost": realized_after_cost,
                "total_equity_after_cost": total_equity_after_cost,
                "today_total_pnl": today_total,
                "total_return_pct": total_return_pct,
                "data_quality_note": "",
            }
        )
    frame = pd.DataFrame(rows, columns=PNL_CHART_COLUMNS)
    target_date = trade_date or frame.iloc[-1]["trade_date"]
    output = report_dir / f"pnl_chart_data_{_date_label(target_date)}.csv"
    frame.to_csv(output, index=False, encoding="utf-8-sig")
    return DashboardReportResult(str(target_date), output, frame)


def generate_market_recap(
    reports_dir: str | Path = "reports",
    config_path: str | Path = "config.yaml",
    trade_date: str | None = None,
) -> DashboardReportResult:
    """Create a Taiwan-only market recap from local market regime and SQLite OHLCV."""

    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    target = trade_date or _latest_trade_date(report_dir) or pd.Timestamp.today().strftime("%Y-%m-%d")
    regime = _read_latest_report(report_dir, "market_regime_*.csv")
    regime_row = regime.iloc[0].to_dict() if not regime.empty else {}
    market_score = _first_number(regime_row, "market_regime_score") or 50.0
    fallback_used = True
    note = "使用 market_regime / 全市場價量 fallback"
    breadth = _market_breadth_from_sqlite(config_path, target)
    if not breadth:
        breadth = {
            "advancers": 0,
            "decliners": 0,
            "unchanged": 0,
            "limit_up_count": 0,
            "limit_down_count": 0,
            "twse_index": "",
            "tpex_index": "",
            "fallback_used": True,
            "data_quality_note": "缺少可用指數或價量資料，使用市場環境分數描述",
        }
    else:
        fallback_used = bool(breadth.get("fallback_used", True))
        note = str(breadth.get("data_quality_note") or note)

    regime_label = _regime_label(market_score)
    breadth_summary = _breadth_summary(
        int(breadth.get("advancers", 0) or 0),
        int(breadth.get("decliners", 0) or 0),
        int(breadth.get("unchanged", 0) or 0),
    )
    allow_text = "目前紙上新增持倉需保守" if market_score < 60 else "市場環境未觸發新增持倉限制"
    recap = f"{regime_label}；{breadth_summary}；market_regime_score={market_score:.0f}，{allow_text}。"
    frame = pd.DataFrame(
        [
            {
                "trade_date": pd.to_datetime(target).strftime("%Y-%m-%d"),
                "market_regime_score": round(market_score, 2),
                "regime_label": regime_label,
                "twse_index": breadth.get("twse_index", ""),
                "tpex_index": breadth.get("tpex_index", ""),
                "advancers": breadth.get("advancers", 0),
                "decliners": breadth.get("decliners", 0),
                "unchanged": breadth.get("unchanged", 0),
                "limit_up_count": breadth.get("limit_up_count", 0),
                "limit_down_count": breadth.get("limit_down_count", 0),
                "market_breadth_summary": breadth_summary,
                "recap_summary": recap,
                "fallback_used": bool(fallback_used),
                "data_quality_note": note,
            }
        ],
        columns=MARKET_RECAP_COLUMNS,
    )
    output = report_dir / f"market_recap_{_date_label(target)}.csv"
    frame.to_csv(output, index=False, encoding="utf-8-sig")
    return DashboardReportResult(str(target), output, frame)


def _market_breadth_from_sqlite(config_path: str | Path, trade_date: str) -> dict[str, Any]:
    try:
        config = load_config(config_path)
        engine = create_db_engine(config["database"]["url"])
        init_db(engine)
        history = load_price_history(engine, end_date=trade_date)
    except Exception:
        return {}
    if history.empty:
        return {}
    frame = history.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.dropna(subset=["trade_date"]).sort_values(["symbol", "trade_date"])
    if frame.empty:
        return {}
    latest_date = frame["trade_date"].max()
    latest = frame[frame["trade_date"] == latest_date].copy()
    previous = (
        frame[frame["trade_date"] < latest_date]
        .sort_values(["symbol", "trade_date"])
        .drop_duplicates("symbol", keep="last")[["symbol", "close"]]
        .rename(columns={"close": "prev_close"})
    )
    merged = latest.merge(previous, on="symbol", how="left")
    merged["close"] = pd.to_numeric(merged["close"], errors="coerce")
    merged["prev_close"] = pd.to_numeric(merged["prev_close"], errors="coerce")
    merged["return_pct"] = merged["close"] / merged["prev_close"] - 1
    returns = merged["return_pct"].dropna()
    if returns.empty:
        return {}
    text = merged["symbol"].astype(str) + " " + merged["name"].astype(str)
    index_mask = text.str.contains("加權|櫃買|TAIEX|TWII|TPEX|OTC|發行量加權", case=False, na=False)
    stocks = merged[~index_mask].copy()
    if stocks.empty:
        stocks = merged.copy()
    return {
        "advancers": int((stocks["return_pct"] > 0).sum()),
        "decliners": int((stocks["return_pct"] < 0).sum()),
        "unchanged": int((stocks["return_pct"] == 0).sum()),
        "limit_up_count": int((stocks["return_pct"] >= 0.095).sum()),
        "limit_down_count": int((stocks["return_pct"] <= -0.095).sum()),
        "twse_index": _index_close(merged, ["加權", "TAIEX", "TWII", "發行量加權"]),
        "tpex_index": _index_close(merged, ["櫃買", "TPEX", "OTC"]),
        "fallback_used": True,
        "data_quality_note": "未接正式指數來源時，使用本地 SQLite 全市場價量 fallback",
    }


def _index_close(frame: pd.DataFrame, keywords: list[str]) -> str:
    text = frame["symbol"].astype(str) + " " + frame["name"].astype(str)
    mask = text.str.contains("|".join(keywords), case=False, na=False)
    if not mask.any():
        return ""
    value = _to_float(frame[mask].iloc[-1].get("close"))
    return "" if value is None else round(value, 2)


def _read_all_reports(report_dir: Path, pattern: str) -> pd.DataFrame:
    frames = [_read_csv(path) for path in sorted(report_dir.glob(pattern))]
    frames = [frame for frame in frames if not frame.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _read_latest_report(report_dir: Path, pattern: str) -> pd.DataFrame:
    files = sorted(report_dir.glob(pattern))
    return _read_csv(files[-1]) if files else pd.DataFrame()


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype={"stock_id": str}, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


def _latest_trade_date(report_dir: Path) -> str | None:
    summary = _read_latest_report(report_dir, "daily_summary_*.csv")
    if not summary.empty and "trade_date" in summary.columns:
        value = pd.to_datetime(summary.iloc[0]["trade_date"], errors="coerce")
        if not pd.isna(value):
            return value.strftime("%Y-%m-%d")
    return None


def _first_number(row: dict[str, object], column: str) -> float | None:
    return _to_float(row.get(column))


def _to_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(parsed) else parsed


def _regime_label(score: float) -> str:
    if score >= 70:
        return "大盤偏多"
    if score < 50:
        return "大盤偏空"
    return "大盤震盪"


def _breadth_summary(advancers: int, decliners: int, unchanged: int) -> str:
    total = advancers + decliners + unchanged
    if total <= 0:
        return "市場廣度資料不足"
    if advancers > decliners * 1.3:
        return f"上漲家數 {advancers} 高於下跌家數 {decliners}，市場廣度偏多"
    if decliners > advancers * 1.3:
        return f"下跌家數 {decliners} 高於上漲家數 {advancers}，市場廣度偏空"
    return f"上漲 {advancers}、下跌 {decliners}、平盤 {unchanged}，市場廣度偏震盪"


def _date_label(value: str) -> str:
    return pd.to_datetime(value).strftime("%Y%m%d")
