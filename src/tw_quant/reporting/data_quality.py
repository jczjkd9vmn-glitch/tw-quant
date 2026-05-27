"""Data quality health report helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


DATA_QUALITY_HEALTH_COLUMNS = [
    "check_name",
    "category",
    "health_status",
    "review_level",
    "review_reason",
    "data_issue",
    "investment_risk",
    "rows",
    "affected_symbols_count",
    "source_name",
    "status",
    "fallback_action",
]


def build_data_quality_health(
    candidates: pd.DataFrame,
    data_fetch_status: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    rows.extend(_candidate_health_rows(candidates))
    rows.extend(_source_health_rows(data_fetch_status))
    return pd.DataFrame(rows, columns=DATA_QUALITY_HEALTH_COLUMNS)


def write_data_quality_health(
    report_dir: str | Path,
    candidates: pd.DataFrame,
    data_fetch_status: pd.DataFrame,
) -> Path:
    path = Path(report_dir) / "data_quality_health.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    build_data_quality_health(candidates, data_fetch_status).to_csv(path, index=False, encoding="utf-8-sig")
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
        health_status = _source_health_status(status, fallback_action, row_count)
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
) -> dict[str, object]:
    return {
        "check_name": check_name,
        "category": category,
        "health_status": health_status,
        "review_level": review_level,
        "review_reason": review_reason,
        "data_issue": data_issue,
        "investment_risk": investment_risk,
        "rows": rows,
        "affected_symbols_count": affected_symbols_count,
        "source_name": source_name,
        "status": status,
        "fallback_action": fallback_action,
    }


def _source_health_status(status: str, fallback_action: str, rows: int) -> str:
    if status in {"FAILED", "MISSING"} and fallback_action != "kept_existing_csv":
        return "WARNING"
    if status in {"EMPTY", "CACHE", "OK_WITH_FALLBACK", "OK_WITH_WARNING"} or fallback_action == "kept_existing_csv" or rows == 0:
        return "ATTENTION"
    return "OK"


def _source_review_reason(row: pd.Series) -> str:
    source = str(row.get("source_name", "") or "").strip()
    status = str(row.get("status", "") or "").strip().upper()
    warning = str(row.get("warning", "") or "").strip()
    fallback_action = str(row.get("fallback_action", "") or "").strip()
    if status == "OK":
        return "資料來源正常"
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
