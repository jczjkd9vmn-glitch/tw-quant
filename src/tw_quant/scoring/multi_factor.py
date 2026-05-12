"""Auxiliary multi-factor scoring for candidate reports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from tw_quant.chips.institutional import load_institutional, score_institutional_for_symbols
from tw_quant.events.material_events import load_material_events, score_material_events_for_symbols
from tw_quant.fundamental.financials import load_financials, score_financials_for_symbols
from tw_quant.fundamental.revenue import _load_monthly_revenue, score_revenue_for_symbols
from tw_quant.fundamental.valuation import load_valuation, score_valuation_for_symbols


ROOT = Path(__file__).resolve().parents[3]

MULTI_FACTOR_COLUMNS = [
    "original_total_score",
    "multi_factor_score",
    "multi_factor_reason",
    "revenue_score",
    "revenue_reason",
    "valuation_score",
    "valuation_reason",
    "valuation_warning",
    "financial_score",
    "financial_reason",
    "financial_warning",
    "event_score",
    "event_reason",
    "event_risk_level",
    "event_blocked",
    "institutional_score",
    "institutional_reason",
    "pe_ratio",
    "pb_ratio",
    "dividend_yield",
    "eps",
    "roe",
    "gross_margin",
    "operating_margin",
    "debt_ratio",
    "foreign_net_buy",
    "investment_trust_net_buy",
    "dealer_net_buy",
]


@dataclass(frozen=True)
class DataFetchStatus:
    source_name: str
    status: str
    rows: int
    warning: str = ""
    error_message: str = ""


@dataclass(frozen=True)
class MultiFactorResult:
    candidates: pd.DataFrame
    data_fetch_status: pd.DataFrame


def apply_multi_factor_scores(
    candidates: pd.DataFrame,
    config: dict | None = None,
    data_dir: str | Path = ROOT / "data",
    revenue_path: str | Path | None = None,
    valuation_path: str | Path | None = None,
    financials_path: str | Path | None = None,
    events_path: str | Path | None = None,
    institutional_path: str | Path | None = None,
) -> MultiFactorResult:
    frame = candidates.copy()
    if frame.empty:
        return MultiFactorResult(frame, _status_frame([]))

    multi_config = config or {}
    if not bool(multi_config.get("enabled", True)):
        for column in MULTI_FACTOR_COLUMNS:
            if column not in frame.columns:
                frame[column] = None
        frame["original_total_score"] = frame["total_score"]
        frame["multi_factor_score"] = frame["total_score"]
        frame["multi_factor_reason"] = "multi_factor disabled"
        return MultiFactorResult(frame, _status_frame([]))

    data_path = Path(data_dir)
    source_paths = {
        "monthly_revenue": Path(revenue_path) if revenue_path else data_path / "monthly_revenue.csv",
        "valuation": Path(valuation_path) if valuation_path else data_path / "valuation.csv",
        "financials": Path(financials_path) if financials_path else data_path / "financials.csv",
        "material_events": Path(events_path) if events_path else data_path / "material_events.csv",
        "institutional": Path(institutional_path) if institutional_path else data_path / "institutional.csv",
    }

    symbols = frame["stock_id"].astype(str).str.strip().tolist()
    statuses: list[DataFetchStatus] = []

    revenue_scores, status = _safe_score(
        "monthly_revenue",
        source_paths["monthly_revenue"],
        lambda: score_revenue_for_symbols(symbols, source_paths["monthly_revenue"]),
        _neutral_revenue(symbols),
        lambda path: len(_load_monthly_revenue(path)),
    )
    statuses.append(status)

    valuation_scores, status = _safe_score(
        "valuation",
        source_paths["valuation"],
        lambda: score_valuation_for_symbols(symbols, source_paths["valuation"], revenue_scores),
        _neutral_valuation(symbols),
        lambda path: len(load_valuation(path)),
    )
    statuses.append(status)

    financial_scores, status = _safe_score(
        "financials",
        source_paths["financials"],
        lambda: score_financials_for_symbols(symbols, source_paths["financials"]),
        _neutral_financials(symbols),
        lambda path: len(load_financials(path)),
    )
    statuses.append(status)

    event_scores, status = _safe_score(
        "material_events",
        source_paths["material_events"],
        lambda: score_material_events_for_symbols(symbols, source_paths["material_events"]),
        _neutral_events(symbols),
        lambda path: len(load_material_events(path)),
    )
    statuses.append(status)

    institutional_scores, status = _safe_score(
        "institutional",
        source_paths["institutional"],
        lambda: score_institutional_for_symbols(symbols, source_paths["institutional"]),
        _neutral_institutional(symbols),
        lambda path: len(load_institutional(path)),
    )
    statuses.append(status)

    for data in [revenue_scores, valuation_scores, financial_scores, event_scores, institutional_scores]:
        frame = _merge_factor_data(frame, data)

    for column, default in _multi_factor_defaults().items():
        if column not in frame.columns:
            frame[column] = default
        if isinstance(default, (int, float, bool)):
            frame[column] = frame[column].fillna(default)
        else:
            frame[column] = frame[column].fillna(default)

    if not bool(multi_config.get("block_on_high_risk_event", True)):
        frame["event_blocked"] = False
    else:
        frame["event_blocked"] = frame["event_blocked"].apply(_to_bool)

    frame["original_total_score"] = pd.to_numeric(frame["total_score"], errors="coerce").fillna(50.0)
    frame["multi_factor_score"] = frame.apply(_calculate_multi_factor_score, axis=1)
    frame["multi_factor_reason"] = frame.apply(_multi_factor_reason, axis=1)

    if bool(multi_config.get("affect_ranking", False)):
        frame = frame.sort_values(["multi_factor_score", "original_total_score"], ascending=[False, False])
        frame["rank"] = range(1, len(frame) + 1)

    if bool(multi_config.get("affect_risk_pass", False)):
        frame.loc[frame["event_blocked"], "risk_pass"] = 0
        frame.loc[frame["event_blocked"], "risk_reason"] = (
            frame.loc[frame["event_blocked"], "risk_reason"].astype(str) + "；高風險重大訊息阻擋新進場"
        )

    return MultiFactorResult(frame.reset_index(drop=True), _status_frame(statuses))


def write_data_fetch_status(
    report_dir: str | Path,
    trade_date: str | pd.Timestamp,
    statuses: pd.DataFrame,
) -> Path:
    path = Path(report_dir) / f"data_fetch_status_{pd.to_datetime(trade_date).strftime('%Y%m%d')}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    statuses.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _safe_score(
    source_name: str,
    path: Path,
    scorer: Callable[[], pd.DataFrame],
    neutral: pd.DataFrame,
    row_counter: Callable[[Path], int],
) -> tuple[pd.DataFrame, DataFetchStatus]:
    try:
        if path.exists():
            rows = row_counter(path)
            return scorer(), DataFetchStatus(source_name, "OK", rows)
        return (
            neutral,
            DataFetchStatus(source_name, "MISSING", 0, warning=f"{path} not found; using neutral scores"),
        )
    except Exception as exc:  # pragma: no cover - defensive guard for malformed local data
        return (
            neutral,
            DataFetchStatus(source_name, "FAILED", 0, warning="using neutral scores", error_message=f"{type(exc).__name__}: {exc}"),
        )


def _calculate_multi_factor_score(row: pd.Series) -> float:
    score = (
        _number(row.get("original_total_score"), 50.0) * 0.50
        + _number(row.get("revenue_score"), 50.0) * 0.15
        + _number(row.get("valuation_score"), 50.0) * 0.10
        + _number(row.get("financial_score"), 50.0) * 0.15
        + _number(row.get("event_score"), 50.0) * 0.05
        + _number(row.get("institutional_score"), 50.0) * 0.05
    )
    return round(max(0.0, min(100.0, float(score))), 2)


def _multi_factor_reason(row: pd.Series) -> str:
    parts = [
        f"原始技術/動能分數 {row.get('original_total_score')}",
        str(row.get("revenue_reason", "")),
        str(row.get("valuation_reason", "")),
        str(row.get("financial_reason", "")),
        str(row.get("event_reason", "")),
        str(row.get("institutional_reason", "")),
    ]
    if _to_bool(row.get("event_blocked")):
        parts.append("高風險重大訊息，禁止新進場")
    return "；".join(part for part in parts if part and part != "nan")


def _status_frame(statuses: list[DataFetchStatus]) -> pd.DataFrame:
    columns = ["source_name", "status", "rows", "warning", "error_message"]
    return pd.DataFrame([status.__dict__ for status in statuses], columns=columns)


def _merge_factor_data(frame: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return frame
    result = frame.copy()
    factor = data.copy()
    factor["stock_id"] = factor["stock_id"].astype(str).str.strip()
    lookup = factor.drop_duplicates("stock_id").set_index("stock_id")
    result["stock_id"] = result["stock_id"].astype(str).str.strip()
    for column in factor.columns:
        if column == "stock_id":
            continue
        mapped = result["stock_id"].map(lookup[column])
        if column in result.columns:
            result[column] = result[column].where(~result[column].apply(_is_blank), mapped)
        else:
            result[column] = mapped
    return result


def _multi_factor_defaults() -> dict[str, object]:
    return {
        "revenue_score": 50.0,
        "revenue_reason": "基本面資料不足，採中性分數",
        "valuation_score": 50.0,
        "valuation_reason": "估值資料不足，採中性分數",
        "valuation_warning": "",
        "financial_score": 50.0,
        "financial_reason": "財報資料不足，採中性分數",
        "financial_warning": "",
        "event_score": 50.0,
        "event_reason": "近期無重大事件風險",
        "event_risk_level": "NONE",
        "event_blocked": False,
        "institutional_score": 50.0,
        "institutional_reason": "籌碼資料不足，採中性分數",
    }


def _neutral_revenue(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "revenue_yoy": None,
            "revenue_mom": None,
            "accumulated_revenue_yoy": None,
            "revenue_score": 50.0,
            "revenue_reason": "基本面資料不足，採中性分數",
            "fundamental_score": 50.0,
            "fundamental_reason": "基本面資料不足，採中性分數",
        }
    )


def _neutral_valuation(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "valuation_score": 50.0,
            "pe_ratio": None,
            "pb_ratio": None,
            "dividend_yield": None,
            "valuation_reason": "估值資料不足，採中性分數",
            "valuation_warning": "",
        }
    )


def _neutral_financials(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "financial_score": 50.0,
            "eps": None,
            "roe": None,
            "gross_margin": None,
            "operating_margin": None,
            "debt_ratio": None,
            "financial_reason": "財報資料不足，採中性分數",
            "financial_warning": "",
        }
    )


def _neutral_events(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "event_score": 50.0,
            "event_risk_level": "NONE",
            "event_reason": "近期無重大事件風險",
            "event_blocked": False,
        }
    )


def _neutral_institutional(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "institutional_score": 50.0,
            "foreign_net_buy": None,
            "investment_trust_net_buy": None,
            "dealer_net_buy": None,
            "institutional_reason": "籌碼資料不足，採中性分數",
        }
    )


def _number(value: object, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return parsed


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y"}


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
