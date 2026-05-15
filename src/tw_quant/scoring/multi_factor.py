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
from tw_quant.scoring.official_factors import (
    load_attention_disposition,
    load_credit,
    load_liquidity,
    load_sector_strength,
    score_attention_disposition_for_symbols,
    score_credit_for_symbols,
    score_liquidity_for_symbols,
    score_sector_strength_for_symbols,
)


ROOT = Path(__file__).resolve().parents[3]

MULTI_FACTOR_COLUMNS = [
    "original_total_score",
    "multi_factor_score",
    "multi_factor_reason",
    "revenue_score",
    "monthly_revenue",
    "revenue_yoy",
    "revenue_mom",
    "accumulated_revenue_yoy",
    "revenue_3m_trend",
    "revenue_12m_high",
    "revenue_warning",
    "revenue_reason",
    "valuation_score",
    "valuation_reason",
    "valuation_warning",
    "financial_score",
    "financial_reason",
    "financial_warning",
    "event_score",
    "event_risk_score",
    "event_reason",
    "event_risk_level",
    "event_keywords",
    "event_warning",
    "event_blocked",
    "institutional_score",
    "institutional_reason",
    "foreign_net_buy",
    "investment_trust_net_buy",
    "dealer_net_buy",
    "total_institutional_net_buy",
    "foreign_buy_days",
    "investment_trust_buy_days",
    "institutional_buy_ratio",
    "institutional_warning",
    "credit_score",
    "credit_reason",
    "margin_balance",
    "margin_change",
    "short_balance",
    "short_change",
    "securities_lending_sell_volume",
    "securities_lending_balance",
    "margin_usage_warning",
    "short_selling_warning",
    "is_attention_stock",
    "attention_reason",
    "is_disposition_stock",
    "disposition_start_date",
    "disposition_end_date",
    "disposition_reason",
    "industry",
    "stock_return_5d",
    "stock_return_20d",
    "market_return_5d",
    "market_return_20d",
    "sector_return_5d",
    "sector_return_20d",
    "relative_strength_5d",
    "relative_strength_20d",
    "sector_strength_rank",
    "sector_strength_score",
    "sector_strength_reason",
    "avg_volume_20d",
    "avg_turnover_20d",
    "intraday_trading_ratio",
    "liquidity_score",
    "liquidity_warning",
    "slippage_risk_score",
    "risk_flags",
    "data_source_warning",
    "system_comment",
    "pe_ratio",
    "pb_ratio",
    "dividend_yield",
    "eps",
    "roe",
    "gross_margin",
    "operating_margin",
    "debt_ratio",
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
    credit_path: str | Path | None = None,
    attention_path: str | Path | None = None,
    sector_strength_path: str | Path | None = None,
    liquidity_path: str | Path | None = None,
) -> MultiFactorResult:
    frame = candidates.copy()
    if frame.empty:
        return MultiFactorResult(frame, _status_frame([]))

    full_config = config or {}
    multi_config = full_config.get("multi_factor", full_config) if isinstance(full_config, dict) else {}
    event_risk_config = full_config.get("event_risk", {}) if isinstance(full_config, dict) else {}
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
        "margin_short": Path(credit_path) if credit_path else data_path / "margin_short.csv",
        "attention_disposition": Path(attention_path) if attention_path else data_path / "attention_disposition.csv",
        "sector_strength": Path(sector_strength_path) if sector_strength_path else data_path / "sector_strength.csv",
        "liquidity": Path(liquidity_path) if liquidity_path else data_path / "liquidity.csv",
    }

    symbols = frame["stock_id"].astype(str).str.strip().tolist()
    statuses: list[DataFetchStatus] = []

    score_specs: list[tuple[str, Path, Callable[[], pd.DataFrame], pd.DataFrame, Callable[[Path], int]]] = [
        (
            "monthly_revenue",
            source_paths["monthly_revenue"],
            lambda: score_revenue_for_symbols(symbols, source_paths["monthly_revenue"]),
            _neutral_revenue(symbols),
            lambda path: len(_load_monthly_revenue(path)),
        ),
        (
            "valuation",
            source_paths["valuation"],
            lambda: score_valuation_for_symbols(symbols, source_paths["valuation"]),
            _neutral_valuation(symbols),
            lambda path: len(load_valuation(path)),
        ),
        (
            "financials",
            source_paths["financials"],
            lambda: score_financials_for_symbols(symbols, source_paths["financials"]),
            _neutral_financials(symbols),
            lambda path: len(load_financials(path)),
        ),
        (
            "material_events",
            source_paths["material_events"],
            lambda: score_material_events_for_symbols(symbols, source_paths["material_events"]),
            _neutral_events(symbols),
            lambda path: len(load_material_events(path)),
        ),
        (
            "institutional",
            source_paths["institutional"],
            lambda: score_institutional_for_symbols(symbols, source_paths["institutional"]),
            _neutral_institutional(symbols),
            lambda path: len(load_institutional(path)),
        ),
        (
            "margin_short",
            source_paths["margin_short"],
            lambda: score_credit_for_symbols(symbols, source_paths["margin_short"], frame),
            _neutral_credit(symbols),
            lambda path: len(load_credit(path)),
        ),
        (
            "attention_disposition",
            source_paths["attention_disposition"],
            lambda: score_attention_disposition_for_symbols(
                symbols,
                source_paths["attention_disposition"],
                event_risk_config,
            ),
            _neutral_attention(symbols),
            lambda path: len(load_attention_disposition(path)),
        ),
        (
            "sector_strength",
            source_paths["sector_strength"],
            lambda: score_sector_strength_for_symbols(symbols, source_paths["sector_strength"]),
            _neutral_sector(symbols),
            lambda path: len(load_sector_strength(path)),
        ),
        (
            "liquidity",
            source_paths["liquidity"],
            lambda: score_liquidity_for_symbols(symbols, source_paths["liquidity"]),
            _neutral_liquidity(symbols),
            lambda path: len(load_liquidity(path)),
        ),
    ]

    for source_name, path, scorer, neutral, row_counter in score_specs:
        factor_data, status = _safe_score(source_name, path, scorer, neutral, row_counter)
        statuses.append(status)
        frame = _merge_factor_data(frame, factor_data)

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
    frame["risk_flags"] = frame.apply(_risk_flags, axis=1)
    frame["data_source_warning"] = _data_source_warning(statuses)
    frame["system_comment"] = frame.apply(_system_comment, axis=1)

    if bool(multi_config.get("affect_ranking", False)):
        frame = frame.sort_values(["multi_factor_score", "original_total_score"], ascending=[False, False])
        frame["rank"] = range(1, len(frame) + 1)

    if bool(multi_config.get("affect_risk_pass", False)):
        frame.loc[frame["event_blocked"], "risk_pass"] = 0
        frame.loc[frame["event_blocked"], "risk_reason"] = (
            frame.loc[frame["event_blocked"], "risk_reason"].astype(str) + "；高風險事件禁止進場"
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


def calculate_final_market_score(row: pd.Series) -> float:
    news_score = _number(row.get("news_sentiment_score"), 0.0)
    news_0_to_100 = max(0.0, min(100.0, (news_score + 100.0) / 2.0))
    score = (
        _number(row.get("momentum_score"), 50.0) * 0.25
        + _number(row.get("institutional_score"), _number(row.get("chip_score"), 50.0)) * 0.20
        + _number(row.get("fundamental_score"), 50.0) * 0.15
        + _number(row.get("valuation_score"), 50.0) * 0.10
        + _number(row.get("sector_strength_score"), 50.0) * 0.10
        + _number(row.get("event_risk_score"), _number(row.get("event_score"), 50.0)) * 0.10
        + _number(row.get("liquidity_score"), 50.0) * 0.05
        + news_0_to_100 * 0.05
    )
    return round(max(0.0, min(100.0, float(score))), 2)


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
        f"原始技術總分 {row.get('original_total_score')}",
        str(row.get("revenue_reason", "")),
        str(row.get("valuation_reason", "")),
        str(row.get("financial_reason", "")),
        str(row.get("event_reason", "")),
        str(row.get("institutional_reason", "")),
        str(row.get("credit_reason", "")),
        str(row.get("sector_strength_reason", "")),
        str(row.get("liquidity_warning", "")),
    ]
    if _to_bool(row.get("event_blocked")):
        parts.append("高風險事件或處置股，禁止新增進場")
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
        if column == "event_blocked" and column in result.columns:
            result[column] = result[column].apply(_to_bool) | mapped.apply(_to_bool)
            continue
        if column == "event_risk_score" and column in result.columns:
            existing = pd.to_numeric(result[column], errors="coerce")
            incoming = pd.to_numeric(mapped, errors="coerce")
            result[column] = existing.where(incoming.isna(), pd.concat([existing, incoming], axis=1).min(axis=1))
            continue
        if column == "event_risk_level" and column in result.columns:
            result[column] = [
                _higher_risk(existing, incoming)
                for existing, incoming in zip(result[column].tolist(), mapped.tolist())
            ]
            continue
        if column in result.columns:
            result[column] = result[column].where(~result[column].apply(_is_blank), mapped)
        else:
            result[column] = mapped
    return result


def _multi_factor_defaults() -> dict[str, object]:
    return {
        "revenue_score": 50.0,
        "revenue_reason": "基本面資料不足，採中性分數",
        "revenue_warning": "",
        "valuation_score": 50.0,
        "valuation_reason": "估值資料不足，採中性分數",
        "valuation_warning": "",
        "financial_score": 50.0,
        "financial_reason": "財報資料不足，採中性分數",
        "financial_warning": "",
        "event_score": 50.0,
        "event_risk_score": 50.0,
        "event_reason": "無重大事件資料",
        "event_risk_level": "NONE",
        "event_keywords": "",
        "event_warning": "",
        "event_blocked": False,
        "institutional_score": 50.0,
        "institutional_reason": "三大法人資料不足，採中性分數",
        "credit_score": 50.0,
        "credit_reason": "信用交易與借券資料不足，採中性分數",
        "sector_strength_score": 50.0,
        "sector_strength_reason": "產業相對強弱資料不足，採中性分數",
        "liquidity_score": 50.0,
        "liquidity_warning": "流動性資料不足，採中性分數",
        "slippage_risk_score": 50.0,
        "risk_flags": "",
        "data_source_warning": "",
        "system_comment": "",
    }


def _neutral_revenue(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "monthly_revenue": None,
            "revenue_yoy": None,
            "revenue_mom": None,
            "accumulated_revenue_yoy": None,
            "revenue_3m_trend": "neutral",
            "revenue_12m_high": False,
            "revenue_warning": "",
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
            "event_risk_score": 50.0,
            "event_risk_level": "NONE",
            "event_reason": "無重大事件資料",
            "event_keywords": "",
            "event_warning": "",
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
            "total_institutional_net_buy": None,
            "foreign_buy_days": 0,
            "investment_trust_buy_days": 0,
            "institutional_buy_ratio": None,
            "institutional_warning": "",
            "institutional_reason": "三大法人資料不足，採中性分數",
        }
    )


def _neutral_credit(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "credit_score": 50.0,
            "margin_balance": None,
            "margin_change": None,
            "short_balance": None,
            "short_change": None,
            "securities_lending_sell_volume": None,
            "securities_lending_balance": None,
            "margin_usage_warning": "信用交易與借券資料不足，採中性分數",
            "short_selling_warning": "",
            "credit_reason": "信用交易與借券資料不足，採中性分數",
            "credit_risk_flags": "",
        }
    )


def _neutral_attention(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "is_attention_stock": False,
            "attention_reason": "",
            "is_disposition_stock": False,
            "disposition_start_date": "",
            "disposition_end_date": "",
            "disposition_reason": "",
            "event_risk_score": 50.0,
            "event_risk_level": "NONE",
            "event_blocked": False,
            "attention_disposition_reason": "無注意股或處置股資料",
            "event_risk_flags": "",
        }
    )


def _neutral_sector(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "industry": "",
            "stock_return_5d": None,
            "stock_return_20d": None,
            "market_return_5d": None,
            "market_return_20d": None,
            "sector_return_5d": None,
            "sector_return_20d": None,
            "relative_strength_5d": None,
            "relative_strength_20d": None,
            "sector_strength_rank": None,
            "sector_strength_score": 50.0,
            "sector_strength_reason": "產業相對強弱資料不足，採中性分數",
        }
    )


def _neutral_liquidity(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": symbols,
            "avg_volume_20d": None,
            "avg_turnover_20d": None,
            "intraday_trading_ratio": None,
            "liquidity_score": 50.0,
            "liquidity_warning": "流動性資料不足，採中性分數",
            "slippage_risk_score": 50.0,
            "liquidity_risk_flags": "",
        }
    )


def _risk_flags(row: pd.Series) -> str:
    flags: list[str] = []
    for column in [
        "credit_risk_flags",
        "event_risk_flags",
        "liquidity_risk_flags",
        "valuation_warning",
        "financial_warning",
        "institutional_warning",
        "revenue_warning",
    ]:
        text = str(row.get(column, "") or "").strip()
        if text and text != "nan":
            flags.extend(part.strip() for part in text.replace("；", "|").split("|") if part.strip())
    if _to_bool(row.get("event_blocked")):
        flags.append("禁止新增進場")
    return "；".join(dict.fromkeys(flags))


def _system_comment(row: pd.Series) -> str:
    if _to_bool(row.get("event_blocked")):
        return "有處置股或重大負面事件，預設禁止新增進場"
    if _number(row.get("multi_factor_score"), 50.0) >= 70:
        return "多因子條件偏強，可列入優先觀察"
    if _risk_flags(row):
        return "有風險標籤，需降低優先度"
    return "資料中性，仍以原本技術面與風控結果為準"


def _data_source_warning(statuses: list[DataFetchStatus]) -> str:
    warnings = []
    for status in statuses:
        if status.status in {"MISSING", "FAILED", "EMPTY"} or status.warning:
            warnings.append(f"{status.source_name}:{status.status}")
    return "；".join(warnings)


def _higher_risk(existing: object, incoming: object) -> str:
    order = {"": 0, "NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}
    current = str(existing or "").strip().upper()
    new = str(incoming or "").strip().upper()
    return new if order.get(new, 0) > order.get(current, 0) else current


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
    return text in {"true", "1", "yes", "y", "是"}


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
