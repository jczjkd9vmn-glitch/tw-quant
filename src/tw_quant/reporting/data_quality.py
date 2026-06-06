"""Data quality health report helpers."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from tw_quant.risk.controls import detect_price_jumps


DATA_QUALITY_HEALTH_COLUMNS = [
    "run_date",
    "trade_date",
    "check_name",
    "category",
    "health_status",
    "severity",
    "review_level",
    "review_reason",
    "message",
    "recommendation",
    "data_issue",
    "investment_risk",
    "rows",
    "affected_rows",
    "affected_symbols_count",
    "affected_symbols",
    "source_name",
    "status",
    "fallback_action",
    "issue_type",
]


def build_data_quality_health(
    candidates: pd.DataFrame,
    data_fetch_status: pd.DataFrame,
    report_dir: str | Path | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    rows.extend(_candidate_health_rows(candidates))
    rows.extend(_source_health_rows(data_fetch_status))
    if report_dir is not None:
        rows.extend(_system_health_rows(Path(report_dir)))
    return pd.DataFrame(rows, columns=DATA_QUALITY_HEALTH_COLUMNS)


def write_data_quality_health(
    report_dir: str | Path,
    candidates: pd.DataFrame,
    data_fetch_status: pd.DataFrame,
) -> Path:
    path = Path(report_dir) / "data_quality_health.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    build_data_quality_health(candidates, data_fetch_status, report_dir=path.parent).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _candidate_health_rows(candidates: pd.DataFrame) -> list[dict[str, object]]:
    if candidates.empty:
        return [
            _row(
                "候選股資料",
                "candidate",
                "WARNING",
                "DATA_REVIEW",
                "找不到候選股資料，需確認每日流程是否成功產生 candidates CSV",
                data_issue=True,
                rows=0,
            )
        ]

    data_flags = _non_empty_count(candidates, "data_quality_flags")
    risk_flags = _non_empty_count(candidates, "investment_risk_flags")
    fallback_count = _industry_fallback_count(candidates)
    rows = [
        _row(
            "候選股資料完整度",
            "candidate",
            "ATTENTION" if data_flags else "OK",
            "DATA_REVIEW" if data_flags else "OK",
            f"{data_flags} 檔候選股有資料不足或 fallback 註記",
            data_issue=bool(data_flags),
            rows=len(candidates),
            affected_rows=data_flags,
            affected_symbols_count=data_flags,
        ),
        _row(
            "候選股投資風險",
            "candidate",
            "ATTENTION" if risk_flags else "OK",
            "RISK_REVIEW" if risk_flags else "OK",
            f"{risk_flags} 檔候選股有投資風險註記",
            investment_risk=bool(risk_flags),
            rows=len(candidates),
            affected_rows=risk_flags,
            affected_symbols_count=risk_flags,
        ),
        _row(
            "產業分類覆蓋率",
            "industry",
            "ATTENTION" if fallback_count else "OK",
            "DATA_REVIEW" if fallback_count else "OK",
            f"{fallback_count} 檔缺少產業分類，使用全市場相對強弱 fallback",
            data_issue=bool(fallback_count),
            rows=len(candidates),
            affected_rows=fallback_count,
            affected_symbols_count=fallback_count,
        ),
    ]
    return rows


def _source_health_rows(data_fetch_status: pd.DataFrame) -> list[dict[str, object]]:
    if data_fetch_status.empty:
        return [
            _row(
                "資料來源狀態",
                "source",
                "ATTENTION",
                "DATA_REVIEW",
                "找不到 data_fetch_status 紀錄",
                data_issue=True,
                rows=0,
            )
        ]

    rows: list[dict[str, object]] = []
    for _, source in data_fetch_status.iterrows():
        status = str(source.get("status", "") or "").strip().upper()
        fallback_action = str(source.get("fallback_action", "") or "").strip()
        row_count = int(_to_float(source.get("rows")) or 0)
        health_status = _source_health_status(source, status, fallback_action, row_count)
        review_level = "DATA_REVIEW" if health_status != "OK" else "OK"
        rows.append(
            _row(
                f"資料來源：{source.get('source_name', '')}",
                "source",
                health_status,
                review_level,
                _source_review_reason(source),
                data_issue=health_status != "OK",
                rows=row_count,
                affected_symbols_count=int(_to_float(source.get("affected_symbols_count")) or 0),
                source_name=str(source.get("source_name", "") or ""),
                status=status,
                fallback_action=fallback_action,
            )
        )
    return rows


def _row(
    check_name: str,
    category: str,
    health_status: str,
    review_level: str,
    review_reason: str,
    *,
    data_issue: bool = False,
    investment_risk: bool = False,
    rows: int = 0,
    affected_symbols_count: int = 0,
    source_name: str = "",
    status: str = "",
    fallback_action: str = "",
    trade_date: str = "",
    severity: str = "",
    affected_rows: int | None = None,
    affected_symbols: str = "",
    recommendation: str = "",
    issue_type: str = "",
) -> dict[str, object]:
    severity_value = severity or _severity_from_health_status(health_status)
    message = review_reason
    return {
        "run_date": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "trade_date": trade_date,
        "check_name": check_name,
        "category": category,
        "health_status": health_status,
        "severity": severity_value,
        "review_level": review_level,
        "review_reason": review_reason,
        "message": message,
        "recommendation": recommendation,
        "data_issue": data_issue,
        "investment_risk": investment_risk,
        "rows": rows,
        "affected_rows": rows if affected_rows is None else affected_rows,
        "affected_symbols_count": affected_symbols_count,
        "affected_symbols": affected_symbols,
        "source_name": source_name,
        "status": status,
        "fallback_action": fallback_action,
        "issue_type": issue_type,
    }


def _system_health_rows(report_dir: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    price_frame = _load_daily_prices(report_dir)
    quarantine_frame = _load_quarantined_prices(report_dir)
    rows.extend(_price_health_rows(price_frame))
    rows.extend(_quarantine_health_rows(quarantine_frame))
    rows.extend(_benchmark_health_rows(report_dir))
    rows.append(_official_index_health_row(report_dir))
    return rows


def _price_health_rows(frame: pd.DataFrame) -> list[dict[str, object]]:
    if frame.empty:
        return [
            _row(
                "SQLite 價量資料",
                "price_data",
                "ATTENTION",
                "DATA_REVIEW",
                "找不到 SQLite daily_prices，無法檢查週末資料與跳價。",
                data_issue=True,
                rows=0,
                status="DATA_INSUFFICIENT",
                recommendation="確認 daily workflow 是否已建立 data/tw_quant.sqlite。",
            )
        ]
    output: list[dict[str, object]] = []
    prices = frame.copy()
    prices["trade_date"] = pd.to_datetime(prices["trade_date"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        if column in prices.columns:
            prices[column] = pd.to_numeric(prices[column], errors="coerce")
    total_rows = len(prices)

    weekend = prices[prices["trade_date"].dt.weekday >= 5].copy()
    output.append(
        _row(
            "daily_prices 週末資料",
            "price_data",
            "WARNING" if not weekend.empty else "OK",
            "DATA_REVIEW" if not weekend.empty else "OK",
            f"{len(weekend)} 筆 daily_prices 落在週末，計算時已排除非交易日資料。",
            data_issue=not weekend.empty,
            rows=total_rows,
            affected_rows=len(weekend),
            affected_symbols_count=_symbol_count(weekend),
            affected_symbols=_symbol_preview(weekend),
            status="DATA_QUALITY_WARNING" if not weekend.empty else "OK",
            recommendation="後續 backfill/pipeline 應跳過非交易日；舊資料暫不刪除但不可用於 benchmark。",
            issue_type=_price_issue_type(weekend, prices),
        )
    )

    duplicates = prices[prices.duplicated(["trade_date", "symbol"], keep=False)].copy()
    output.append(
        _row(
            "daily_prices duplicate trade_date/symbol",
            "price_data",
            "WARNING" if not duplicates.empty else "OK",
            "DATA_REVIEW" if not duplicates.empty else "OK",
            f"{len(duplicates)} 筆 daily_prices 有重複 trade_date/symbol。",
            data_issue=not duplicates.empty,
            rows=total_rows,
            affected_rows=len(duplicates),
            affected_symbols_count=_symbol_count(duplicates),
            affected_symbols=_symbol_preview(duplicates),
            status="DATA_QUALITY_WARNING" if not duplicates.empty else "OK",
            recommendation="重複資料應人工確認來源，不可靜默覆蓋 benchmark。",
            issue_type=_price_issue_type(duplicates, prices),
        )
    )

    ohlc_columns = [column for column in ["open", "high", "low", "close"] if column in prices.columns]
    non_positive = prices[(prices[ohlc_columns] <= 0).any(axis=1)].copy() if ohlc_columns else pd.DataFrame()
    output.append(
        _row(
            "daily_prices 非正 OHLC",
            "price_data",
            "WARNING" if not non_positive.empty else "OK",
            "DATA_REVIEW" if not non_positive.empty else "OK",
            f"{len(non_positive)} 筆 daily_prices 有非正 OHLC。",
            data_issue=not non_positive.empty,
            rows=total_rows,
            affected_rows=len(non_positive),
            affected_symbols_count=_symbol_count(non_positive),
            affected_symbols=_symbol_preview(non_positive),
            status="DATA_QUALITY_WARNING" if not non_positive.empty else "OK",
            recommendation="非正 OHLC 不可進入候選評分、market regime 或 benchmark。",
            issue_type=_price_issue_type(non_positive, prices),
        )
    )

    invalid_high_low = pd.DataFrame()
    if {"open", "high", "low", "close"}.issubset(prices.columns):
        invalid_high_low = prices[
            (prices["high"] < prices[["open", "close", "low"]].max(axis=1))
            | (prices["low"] > prices[["open", "close", "high"]].min(axis=1))
        ].copy()
    output.append(
        _row(
            "daily_prices high/low 合理性",
            "price_data",
            "WARNING" if not invalid_high_low.empty else "OK",
            "DATA_REVIEW" if not invalid_high_low.empty else "OK",
            f"{len(invalid_high_low)} 筆 daily_prices high/low 不合理。",
            data_issue=not invalid_high_low.empty,
            rows=total_rows,
            affected_rows=len(invalid_high_low),
            affected_symbols_count=_symbol_count(invalid_high_low),
            affected_symbols=_symbol_preview(invalid_high_low),
            status="DATA_QUALITY_WARNING" if not invalid_high_low.empty else "OK",
            recommendation="high/low 異常資料不可用於正式診斷。",
            issue_type=_price_issue_type(invalid_high_low, prices),
        )
    )

    negative_volume = prices[prices["volume"] < 0].copy() if "volume" in prices.columns else pd.DataFrame()
    output.append(
        _row(
            "daily_prices 負成交量",
            "price_data",
            "WARNING" if not negative_volume.empty else "OK",
            "DATA_REVIEW" if not negative_volume.empty else "OK",
            f"{len(negative_volume)} 筆 daily_prices 成交量為負。",
            data_issue=not negative_volume.empty,
            rows=total_rows,
            affected_rows=len(negative_volume),
            affected_symbols_count=_symbol_count(negative_volume),
            affected_symbols=_symbol_preview(negative_volume),
            status="DATA_QUALITY_WARNING" if not negative_volume.empty else "OK",
            recommendation="負成交量應回查資料來源。",
            issue_type=_price_issue_type(negative_volume, prices),
        )
    )

    jumps = detect_price_jumps(prices[["trade_date", "symbol", "close"]], max_abs_daily_return=0.50)
    output.append(
        _row(
            "daily_prices 跨日價格跳動",
            "price_data",
            "WARNING" if not jumps.empty else "OK",
            "DATA_REVIEW" if not jumps.empty else "OK",
            f"{len(jumps)} 筆 daily_prices abs(return_1d) > 50%，benchmark 與 market regime 計算時已排除。",
            data_issue=not jumps.empty,
            rows=total_rows,
            affected_rows=len(jumps),
            affected_symbols_count=_symbol_count(jumps),
            affected_symbols=_symbol_preview(jumps),
            status="DATA_QUALITY_WARNING" if not jumps.empty else "OK",
            recommendation="跳價可能來自錯價、減資、分割或 ETF 反分割，需人工確認再作正式解讀。",
            issue_type=_price_issue_type(jumps, prices),
        )
    )
    return output


def _quarantine_health_rows(frame: pd.DataFrame) -> list[dict[str, object]]:
    if frame.empty:
        return []
    reason_column = "quarantine_reason" if "quarantine_reason" in frame.columns else ""
    reasons = ""
    if reason_column:
        values = frame[reason_column].fillna("").astype(str).str.strip()
        reasons = "；".join(value for value in dict.fromkeys(values.tolist()) if value)[:240]
    return [
        _row(
            "daily_prices quarantine",
            "price_data",
            "OK_WITH_WARNING",
            "DATA_REVIEW",
            f"{len(frame)} 筆歷史污染價量資料已移入 quarantine，不再用於 active benchmark / market regime。",
            data_issue=True,
            rows=len(frame),
            affected_rows=len(frame),
            affected_symbols_count=_symbol_count(frame),
            affected_symbols=_symbol_preview(frame),
            status="REPAIRED_OR_QUARANTINED",
            recommendation="保留 quarantine 與 SQLite 備份；確認無誤後才考慮後續資料重建。",
            issue_type="repaired_or_quarantined",
            fallback_action=reasons,
        )
    ]


def _benchmark_health_rows(report_dir: Path) -> list[dict[str, object]]:
    frame = _latest_report(report_dir, "benchmark_diagnostics_*.csv")
    if frame.empty:
        frame = _latest_report(report_dir, "performance_diagnostics_*.csv")
    if frame.empty:
        return [
            _row(
                "benchmark source",
                "benchmark",
                "ATTENTION",
                "DATA_REVIEW",
                "找不到 benchmark/performance diagnostics，無法判定是否打敗大盤。",
                data_issue=True,
                rows=0,
                status="DATA_INSUFFICIENT",
                recommendation="執行 daily workflow 或 generate_html_report 產生 benchmark diagnostics。",
            )
        ]
    row = frame.iloc[0]
    source = str(row.get("benchmark_source", "") or "").strip()
    warning = str(row.get("benchmark_warning", "") or row.get("data_quality_warning", "") or "").strip()
    can_judge_alpha = _truthy(row.get("can_judge_alpha", False))
    missing = source == "" or "資料不足" in source
    fallback = "fallback" in source.lower() or not can_judge_alpha
    return [
        _row(
            "benchmark source",
            "benchmark",
            "WARNING" if missing else "ATTENTION" if fallback or warning else "OK",
            "DATA_REVIEW" if missing or fallback or warning else "OK",
            warning or (f"benchmark 使用 {source}；can_judge_alpha={str(can_judge_alpha).lower()}" if source else "benchmark source 不足"),
            data_issue=missing or bool(fallback or warning),
            rows=len(frame),
            affected_rows=1 if missing or fallback or warning else 0,
            status="DATA_INSUFFICIENT" if missing else "OK_WITH_WARNING" if fallback or warning else "OK",
            recommendation="若使用 fallback，首頁不可把 alpha 當成正式大盤比較；需標示 fallback warning。",
        )
    ]


def _official_index_health_row(report_dir: Path) -> dict[str, object]:
    frame = _read_market_indices(report_dir)
    has_official = False
    if not frame.empty and {"index_id", "is_official"}.issubset(frame.columns):
        ids = {"TAIEX_TR", "TAIEX", "TPEx", "TPEX"}
        has_official = bool(frame[frame["index_id"].astype(str).str.strip().isin(ids)]["is_official"].apply(_truthy).any())
    elif not frame.empty and {"index_id", "close"}.issubset(frame.columns):
        has_official = bool(frame["index_id"].astype(str).str.strip().isin({"TAIEX_TR", "TAIEX", "TPEx", "TPEX"}).any())
    return _row(
        "official market index",
        "benchmark",
        "OK" if has_official else "ATTENTION",
        "OK" if has_official else "DATA_REVIEW",
        "已找到正式 market_indices 資料。" if has_official else "缺少正式加權 / 櫃買指數資料，market regime 與 benchmark 會使用 fallback。",
        data_issue=not has_official,
        rows=len(frame),
        affected_rows=0 if has_official else 1,
        status="OK" if has_official else "DATA_INSUFFICIENT",
        recommendation="若要正式比較大盤，請補 data/market_indices.csv 官方指數資料。",
    )


def _source_health_status(row: pd.Series, status: str, fallback_action: str, rows: int) -> str:
    source = str(row.get("source_name", "") or "").strip()
    fallback_reason = str(row.get("fallback_reason", "") or "").strip()
    freshness_level = str(row.get("data_freshness_level", "") or "").strip().upper()
    if source == "market_intel" and fallback_reason == "no trading data" and freshness_level in {"CURRENT", "RECENT"}:
        return "OK"
    if status in {"FAILED", "MISSING"} and fallback_action != "kept_existing_csv":
        return "WARNING"
    if status in {"EMPTY", "CACHE", "OK_WITH_FALLBACK", "OK_WITH_WARNING"} or fallback_action == "kept_existing_csv" or rows == 0:
        return "ATTENTION"
    return "OK"


def _load_daily_prices(report_dir: Path) -> pd.DataFrame:
    for path in [report_dir.parent / "data" / "tw_quant.sqlite", Path("data") / "tw_quant.sqlite"]:
        if not path.exists():
            continue
        try:
            with sqlite3.connect(path) as conn:
                return pd.read_sql_query(
                    "select trade_date, symbol, name, open, high, low, close, volume from daily_prices",
                    conn,
                )
        except Exception:
            continue
    return pd.DataFrame()


def _load_quarantined_prices(report_dir: Path) -> pd.DataFrame:
    for path in [report_dir.parent / "data" / "tw_quant.sqlite", Path("data") / "tw_quant.sqlite"]:
        if not path.exists():
            continue
        try:
            with sqlite3.connect(path) as conn:
                tables = pd.read_sql_query(
                    "select name from sqlite_master where type='table' and name='daily_prices_quarantine'",
                    conn,
                )
                if tables.empty:
                    continue
                return pd.read_sql_query("select * from daily_prices_quarantine", conn)
        except Exception:
            continue
    return pd.DataFrame()


def _price_issue_type(issue_frame: pd.DataFrame, all_prices: pd.DataFrame) -> str:
    if issue_frame.empty:
        return ""
    latest = _date_series(all_prices, "trade_date")
    issue_dates = _date_series(issue_frame, "trade_date")
    if latest.empty or issue_dates.empty:
        return "legacy_contamination"
    return "active_pipeline_error" if issue_dates.max() >= latest.max() else "legacy_contamination"


def _date_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame.empty or column not in frame.columns:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(frame[column], errors="coerce").dropna()


def _latest_report(report_dir: Path, pattern: str) -> pd.DataFrame:
    files = sorted(report_dir.glob(pattern), key=lambda path: path.stat().st_mtime)
    for path in reversed(files):
        try:
            return pd.read_csv(path, encoding="utf-8-sig", dtype={"stock_id": str})
        except Exception:
            continue
    return pd.DataFrame()


def _read_market_indices(report_dir: Path) -> pd.DataFrame:
    for path in [report_dir.parent / "data" / "market_indices.csv", Path("data") / "market_indices.csv"]:
        if not path.exists():
            continue
        try:
            return pd.read_csv(path, encoding="utf-8-sig", dtype={"index_id": str})
        except Exception:
            continue
    return pd.DataFrame()


def _symbol_count(frame: pd.DataFrame) -> int:
    if frame.empty or "symbol" not in frame.columns:
        return 0
    return int(frame["symbol"].astype(str).str.strip().nunique())


def _symbol_preview(frame: pd.DataFrame, limit: int = 20) -> str:
    if frame.empty or "symbol" not in frame.columns:
        return ""
    values = list(dict.fromkeys(frame["symbol"].astype(str).str.strip().tolist()))
    return "；".join(values[:limit])


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}


def _severity_from_health_status(health_status: str) -> str:
    status = str(health_status or "").strip().upper()
    if status in {"WARNING", "DATA_QUALITY_WARNING", "DATA_BLOCKED"}:
        return "HIGH"
    if status in {"ATTENTION", "OK_WITH_WARNING", "DATA_INSUFFICIENT"}:
        return "MEDIUM"
    return "LOW"


def _source_review_reason(row: pd.Series) -> str:
    source = str(row.get("source_name", "") or "").strip()
    status = str(row.get("status", "") or "").strip().upper()
    warning = str(row.get("warning", "") or "").strip()
    fallback_action = str(row.get("fallback_action", "") or "").strip()
    fallback_reason = str(row.get("fallback_reason", "") or "").strip()
    freshness_level = str(row.get("data_freshness_level", "") or "").strip().upper()
    if status == "OK":
        return "資料來源正常"
    if source == "market_intel" and fallback_reason == "no trading data" and freshness_level in {"CURRENT", "RECENT"}:
        return "非交易日，使用最近交易日資料。"
    if source == "market_intel" and freshness_level in {"STALE", "CACHE"}:
        return "市場資料過期或使用快取資料，不建議短線進場，需人工確認。"
    if fallback_action == "kept_existing_csv" or status == "OK_WITH_FALLBACK":
        return f"{source} 使用既有資料或 fallback，屬資料完整度問題，不等同投資風險"
    if status == "CACHE":
        return f"{source} 使用快取資料，需確認資料時效"
    if status in {"FAILED", "MISSING", "EMPTY"}:
        reason = f"{source} 尚未取得完整資料，採中性或既有資料；{warning}" if warning else f"{source} 尚未取得完整資料，採中性或既有資料"
        return _truncate(reason)
    return _truncate(warning or "需人工確認資料來源狀態")


def _non_empty_count(frame: pd.DataFrame, column: str) -> int:
    if column not in frame.columns:
        return 0
    values = frame[column].fillna("").astype(str).str.strip()
    return int(values.ne("").sum())


def _industry_fallback_count(frame: pd.DataFrame) -> int:
    mode_count = 0
    warning_count = 0
    if "sector_strength_mode" in frame.columns:
        mode_count = int(frame["sector_strength_mode"].fillna("").astype(str).eq("market_relative_fallback").sum())
    if "sector_strength_warning" in frame.columns:
        warning_count = int(frame["sector_strength_warning"].fillna("").astype(str).str.contains("缺少產業分類", na=False).sum())
    return max(mode_count, warning_count)


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _truncate(value: str, limit: int = 240) -> str:
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."
