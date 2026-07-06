"""Candidate data coverage reporting."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd


ETF_METADATA_FIELDS = [
    "liquidity_score",
    "tracking_index",
    "fund_size",
    "expense_ratio",
    "discount_premium",
    "top_holdings_available",
]

CANDIDATE_COVERAGE_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "is_etf",
    "market_type",
    "decision",
    "candidate_grade",
    "review_level",
    "has_industry",
    "has_valuation",
    "has_financials",
    "has_revenue",
    "has_institutional",
    "has_margin",
    "has_event_data",
    "has_etf_metadata",
    "missing_fields",
]


@dataclass(frozen=True)
class CandidateCoverageResult:
    trade_date: pd.Timestamp | None
    coverage: pd.DataFrame
    output_path: Path | None
    warning: str = ""


def generate_candidate_coverage_report(
    reports_dir: str | Path = "reports",
    trade_date: str | pd.Timestamp | None = None,
) -> CandidateCoverageResult:
    report_dir = Path(reports_dir)
    candidates_path = _latest_file(report_dir, "candidates_*.csv", trade_date)
    if candidates_path is None:
        return CandidateCoverageResult(
            None,
            pd.DataFrame(columns=CANDIDATE_COVERAGE_COLUMNS),
            None,
            "no candidates report found",
        )

    candidates = _read_csv(candidates_path)
    selected_date = _date_from_path(candidates_path)
    decisions_path = _latest_file(report_dir, "trading_decisions_*.csv", selected_date or trade_date)
    decisions = _read_csv(decisions_path) if decisions_path is not None else pd.DataFrame()
    coverage = build_candidate_coverage_report(candidates, decisions, selected_date)
    if selected_date is None and not coverage.empty:
        selected_date = pd.to_datetime(coverage["trade_date"].iloc[0], errors="coerce")
        if pd.isna(selected_date):
            selected_date = None

    if selected_date is None:
        return CandidateCoverageResult(None, coverage, None, "cannot resolve candidate coverage date")

    report_dir.mkdir(parents=True, exist_ok=True)
    output_path = report_dir / f"candidate_coverage_report_{selected_date.strftime('%Y%m%d')}.csv"
    coverage.to_csv(output_path, index=False, encoding="utf-8")
    return CandidateCoverageResult(selected_date, coverage, output_path)


def build_candidate_coverage_report(
    candidates: pd.DataFrame,
    decisions: pd.DataFrame | None = None,
    trade_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame(columns=CANDIDATE_COVERAGE_COLUMNS)

    decision_lookup = _decision_lookup(decisions)
    rows: list[dict[str, object]] = []
    for _, row in candidates.iterrows():
        symbol = str(row.get("stock_id", "")).strip()
        decision = decision_lookup.get(symbol, {})
        is_etf = _is_etf(row)

        has_industry = _has_industry(row)
        has_valuation = True if is_etf else _has_valuation(row)
        has_financials = True if is_etf else _has_financials(row)
        has_revenue = True if is_etf else _has_revenue(row)
        has_institutional = _has_institutional(row)
        has_margin = _has_margin(row)
        has_event_data = _has_event_data(row)
        has_etf_metadata = _has_etf_metadata(row) if is_etf else True

        missing = []
        if not has_industry:
            missing.append("INDUSTRY_MISSING")
        if not has_valuation:
            missing.append("VALUATION_MISSING")
        if not has_financials:
            missing.append("FINANCIAL_MISSING")
        if not has_revenue:
            missing.append("REVENUE_MISSING")
        if not has_institutional:
            missing.append("INSTITUTIONAL_MISSING")
        if not has_margin:
            missing.append("MARGIN_MISSING")
        if not has_event_data:
            missing.append("EVENT_DATA_MISSING")
        if is_etf and not has_etf_metadata:
            missing.append("ETF_METADATA_MISSING")

        rows.append(
            {
                "trade_date": _date_text(trade_date or row.get("trade_date")),
                "stock_id": symbol,
                "stock_name": str(row.get("stock_name", "") or decision.get("stock_name", "") or ""),
                "is_etf": bool(is_etf),
                "market_type": str(row.get("market_type", "") or ""),
                "decision": str(decision.get("decision", "") or ""),
                "candidate_grade": str(decision.get("candidate_grade", "") or row.get("candidate_grade", "") or ""),
                "review_level": str(decision.get("review_level", "") or row.get("review_level", "") or ""),
                "has_industry": bool(has_industry),
                "has_valuation": bool(has_valuation),
                "has_financials": bool(has_financials),
                "has_revenue": bool(has_revenue),
                "has_institutional": bool(has_institutional),
                "has_margin": bool(has_margin),
                "has_event_data": bool(has_event_data),
                "has_etf_metadata": bool(has_etf_metadata),
                "missing_fields": "；".join(missing),
            }
        )
    return pd.DataFrame(rows, columns=CANDIDATE_COVERAGE_COLUMNS)


def _decision_lookup(decisions: pd.DataFrame | None) -> dict[str, dict[str, object]]:
    if decisions is None or decisions.empty or "stock_id" not in decisions.columns:
        return {}
    frame = decisions.copy()
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    if "source" in frame.columns:
        candidate_rows = frame[frame["source"].fillna("").astype(str) == "candidate"].copy()
        if not candidate_rows.empty:
            frame = candidate_rows
    return {
        stock_id: row.to_dict() for stock_id, row in frame.drop_duplicates("stock_id").set_index("stock_id").iterrows()
    }


def _is_etf(row: pd.Series | dict[str, object]) -> bool:
    stock_id = str(row.get("stock_id", "") or "").strip()
    market_type = str(row.get("market_type", "") or "").strip().upper()
    return stock_id.startswith("00") or market_type == "ETF"


def _has_industry(row: pd.Series) -> bool:
    industry = str(row.get("industry", "") or row.get("industry_main", "") or "").strip()
    sub_industry = str(row.get("sub_industry", "") or "").strip()
    if not industry and not sub_industry:
        return False
    if industry == "全市場":
        return False
    mode = str(row.get("sector_strength_mode", "") or "").strip().lower()
    warning = str(row.get("sector_strength_warning", "") or "")
    if mode == "market_relative_fallback" or "缺少產業分類" in warning:
        return False
    return True


def _has_valuation(row: pd.Series) -> bool:
    if _status_ok(row.get("valuation_source_status")):
        return True
    if _has_any_number(row, ["pe_ratio", "pb_ratio", "dividend_yield"]):
        return True
    return _score_has_non_missing_reason(row, "valuation_score", "valuation_reason")


def _has_financials(row: pd.Series) -> bool:
    if _status_ok(row.get("financial_source_status")):
        return True
    if _has_any_number(row, ["eps", "roe", "gross_margin", "operating_margin", "debt_ratio"]):
        return True
    return _score_has_non_missing_reason(row, "financial_score", "financial_reason")


def _has_revenue(row: pd.Series) -> bool:
    if _status_ok(row.get("revenue_source_status")):
        return True
    if _has_any_number(row, ["monthly_revenue", "revenue_yoy", "revenue_mom", "accumulated_revenue_yoy"]):
        return True
    return _score_has_non_missing_reason(row, "revenue_score", "revenue_reason")


def _has_institutional(row: pd.Series) -> bool:
    if _has_any_number(
        row,
        [
            "foreign_net_buy",
            "investment_trust_net_buy",
            "dealer_net_buy",
            "total_institutional_net_buy",
            "institutional_buy_ratio",
        ],
    ):
        return True
    return _score_has_non_missing_reason(row, "institutional_score", "institutional_reason")


def _has_margin(row: pd.Series) -> bool:
    if _has_any_number(
        row,
        [
            "margin_balance",
            "margin_change",
            "short_balance",
            "short_change",
            "securities_lending_sell_volume",
            "securities_lending_balance",
        ],
    ):
        return True
    return _score_has_non_missing_reason(row, "credit_score", "credit_reason")


def _has_event_data(row: pd.Series) -> bool:
    if _truthy(row.get("is_attention_stock")) or _truthy(row.get("is_disposition_stock")):
        return True
    if str(row.get("event_risk_level", "") or "").strip().upper() in {"LOW", "MEDIUM", "HIGH"}:
        return True
    text = " ".join(
        str(row.get(column, "") or "")
        for column in ["event_reason", "event_keywords", "attention_reason", "disposition_reason"]
    )
    return bool(text.strip()) and "無重大事件資料" not in text


def _has_etf_metadata(row: pd.Series) -> bool:
    missing = _missing_etf_metadata_fields(row)
    return not missing


def _missing_etf_metadata_fields(row: pd.Series) -> list[str]:
    missing = []
    for field in ETF_METADATA_FIELDS:
        value = row.get(field)
        if field == "top_holdings_available":
            if not _truthy(value):
                missing.append(field)
        elif _is_blank(value):
            missing.append(field)
    return missing


def _has_any_text(row: pd.Series, columns: list[str]) -> bool:
    return any(not _is_blank(row.get(column)) for column in columns if column in row.index)


def _has_any_number(row: pd.Series, columns: list[str]) -> bool:
    return any(_to_float(row.get(column)) is not None for column in columns if column in row.index)


def _score_has_non_missing_reason(row: pd.Series, score_column: str, reason_column: str) -> bool:
    score = _to_float(row.get(score_column))
    reason = str(row.get(reason_column, "") or "")
    return score is not None and score != 50.0 and "資料不足" not in reason


def _status_ok(value: object) -> bool:
    return str(value or "").strip().upper() in {"OK", "OK_WITH_WARNING", "OK_WITH_FALLBACK"}


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _latest_file(report_dir: Path, pattern: str, trade_date: str | pd.Timestamp | None = None) -> Path | None:
    if trade_date is not None:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            target = report_dir / pattern.replace("*", parsed.strftime("%Y%m%d"))
            if target.exists():
                return target
    files = sorted(report_dir.glob(pattern), key=lambda path: _date_from_path(path) or pd.Timestamp.min)
    return files[-1] if files else None


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed


def _date_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()
