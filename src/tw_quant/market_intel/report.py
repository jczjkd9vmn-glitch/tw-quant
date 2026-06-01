"""Market intelligence report and cache utilities."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from tw_quant.market_intel.providers.base import MarketContext
from tw_quant.market_intel.providers.mock_provider import MockMarketIntelProvider
from tw_quant.market_intel.providers.real_provider import RealMarketIntelProvider
from tw_quant.market_intel.providers.yfinance_provider import YFinanceMarketIntelProvider
from tw_quant.market_intel.scoring import build_market_context


_RECOMPUTED_DATA_GAP_WARNINGS = {
    "基本面資料不足，採中性分數",
    "估值資料不足，採中性分數",
    "價格資料不足，動能採中性分數",
}
_DEFAULT_STALE_DAYS_THRESHOLD = 2
_GLOBAL_FRESHNESS_WARNINGS = {
    "資料來源缺失或快取資料，需人工確認。",
    "使用快取 / 非當日資料",
}

MARKET_INTEL_COLUMNS = [
    "market_intel_status",
    "requested_date",
    "actual_data_date",
    "fallback_date",
    "fallback_reason",
    "cache_age_days",
    "is_stale_data",
    "data_freshness_level",
    "market_intel_source",
    "market_intel_warning",
    "market_close",
    "market_volume",
    "volume_change_ratio",
    "market_pe_ratio",
    "market_pb_ratio",
    "market_dividend_yield",
    "market_revenue_growth_yoy",
    "market_eps_growth_yoy",
    "latest_news_titles",
    "matched_news_keywords",
    "news_sentiment_score",
    "market_fundamental_score",
    "market_valuation_score",
    "market_momentum_score",
    "market_chip_score",
    "credit_score",
    "event_risk_score",
    "liquidity_score",
    "sector_strength_score",
    "final_market_score",
    "confidence_score",
    "market_risk_score",
    "risk_flags",
    "final_comment",
    "data_source_warning",
    "system_comment",
]


def build_market_intel_report(
    candidates: pd.DataFrame,
    reports_dir: str | Path = "reports",
    trade_date: str | pd.Timestamp | None = None,
    config: dict | None = None,
    requested_date: str | pd.Timestamp | None = None,
    fallback_date: str | pd.Timestamp | None = None,
    fallback_reason: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if candidates.empty:
        return pd.DataFrame(columns=["stock_id"] + MARKET_INTEL_COLUMNS), _status("market_intel", "EMPTY", 0)

    active_config = config or {}
    if active_config.get("enabled", True) is False:
        frame = _neutral_from_candidates(candidates, "market intelligence disabled")
        return frame, _status("market_intel", "DISABLED", len(frame), warning="market intelligence disabled")

    report_dir = Path(reports_dir)
    date_label = _date_label(trade_date or candidates.get("trade_date", pd.Series([""])).iloc[0])
    requested_text = _date_text(requested_date or trade_date or candidates.get("trade_date", pd.Series([""])).iloc[0])
    actual_text = _date_text(trade_date or date_label)
    fallback_text = _date_text(fallback_date) if fallback_date is not None and str(fallback_date).strip() else ""
    cache_path = report_dir / "cache" / f"market_intel_{date_label}.json"
    cache_enabled = bool(active_config.get("cache_enabled", True))
    stale_days_threshold = _stale_days_threshold(active_config)
    if cache_enabled and cache_path.exists():
        frame = _read_cache(cache_path)
        provider_name = str(active_config.get("provider", "real")).strip().lower()
        cache_is_mock = "market_intel_source" in frame.columns and frame["market_intel_source"].astype(str).str.lower().eq("mock").all()
        if (
            not frame.empty
            and _cache_has_freshness_schema(frame)
            and not _cache_has_recomputed_data_gap_warnings(frame)
            and not (provider_name in {"real", "best_effort"} and cache_is_mock)
        ):
            frame = _apply_freshness(
                frame,
                requested_date=requested_text,
                actual_data_date=actual_text,
                fallback_date=fallback_text,
                fallback_reason=fallback_reason,
                cache_used=True,
                stale_days_threshold=stale_days_threshold,
            )
            status_value = _market_status_from_freshness(frame, cache_used=True, fallback_reason=fallback_reason)
            _write_csv(report_dir, date_label, frame)
            return frame, _status(
                "market_intel",
                status_value,
                len(frame),
                warning=_status_warning_text(frame, fallback_reason=fallback_reason),
                requested_period=requested_text,
                actual_period=actual_text,
                latest_available_period=fallback_text or actual_text,
                is_stale=_has_stale_data(frame),
                data_age_days=_max_cache_age(frame),
                fallback_reason=fallback_reason,
                data_freshness_level=_frame_freshness_level(frame),
                cache_used=True,
            )

    provider_name = str(active_config.get("provider", "real")).strip().lower()
    if provider_name == "yfinance":
        provider = YFinanceMarketIntelProvider()
    elif provider_name in {"real", "best_effort", "official"}:
        provider = RealMarketIntelProvider(
            data_dir=active_config.get("data_dir", "data"),
            allow_mock=bool(active_config.get("allow_mock", True)),
            block_disposition_stock=bool(active_config.get("block_disposition_stock", True)),
            block_attention_stock=bool(active_config.get("block_attention_stock", False)),
        )
    else:
        provider = MockMarketIntelProvider()
    symbols = candidates["stock_id"].astype(str).tolist()
    provider_contexts = {context.symbol: context for context in provider.fetch(symbols, as_of=_date_text(date_label))}
    rows = []
    for _, row in candidates.iterrows():
        symbol = str(row.get("stock_id", "")).strip()
        context = _context_from_candidate(row, provider_contexts.get(symbol), date_label)
        rows.append(_flatten_context(context))
    frame = pd.DataFrame(rows)
    frame = _apply_freshness(
        frame,
        requested_date=requested_text,
        actual_data_date=actual_text,
        fallback_date=fallback_text,
        fallback_reason=fallback_reason,
        cache_used=False,
        stale_days_threshold=stale_days_threshold,
    )
    if cache_enabled:
        _write_cache(cache_path, frame)
    _write_csv(report_dir, date_label, frame)
    warning = _warning_text(frame)
    if "market_intel_source" in frame.columns and frame["market_intel_source"].astype(str).str.lower().eq("mock").all():
        status_value = "MOCK"
    else:
        status_value = "OK_WITH_WARNING" if warning else "OK"
    return frame, _status(
        "market_intel",
        status_value,
        len(frame),
        warning=_status_warning_text(frame, fallback_reason=fallback_reason, base_warning=warning),
        requested_period=requested_text,
        actual_period=actual_text,
        latest_available_period=fallback_text or actual_text,
        is_stale=_has_stale_data(frame),
        data_age_days=_max_cache_age(frame),
        fallback_reason=fallback_reason,
        data_freshness_level=_frame_freshness_level(frame),
        cache_used=False,
    )


def _context_from_candidate(row: pd.Series, provider_context: MarketContext | None, date_label: str) -> MarketContext:
    provider_context = provider_context or build_market_context(
        symbol=str(row.get("stock_id", "")),
        date=_date_text(date_label),
        warning_message="market intelligence provider unavailable; using candidate data",
    )
    event_text = " ".join(
        str(row.get(column, ""))
        for column in ["event_reason", "event_keywords", "multi_factor_reason", "reason"]
        if not _is_blank(row.get(column))
    )
    warning = _provider_warning_without_recomputed_data_gaps(provider_context.warning_message)
    return build_market_context(
        symbol=str(row.get("stock_id", provider_context.symbol)),
        date=_date_text(date_label),
        close=_first_valid(row.get("close"), provider_context.close),
        volume=_first_valid(row.get("volume"), provider_context.volume),
        volume_change_ratio=provider_context.volume_change_ratio,
        pe_ratio=_first_valid(row.get("pe_ratio"), provider_context.pe_ratio),
        pb_ratio=_first_valid(row.get("pb_ratio"), provider_context.pb_ratio),
        dividend_yield=_first_valid(row.get("dividend_yield"), provider_context.dividend_yield),
        revenue_growth_yoy=_first_valid(row.get("revenue_yoy"), provider_context.revenue_growth_yoy),
        eps_growth_yoy=_first_valid(row.get("eps_yoy"), provider_context.eps_growth_yoy),
        roe=row.get("roe"),
        debt_ratio=row.get("debt_ratio"),
        momentum_score_hint=row.get("momentum_score"),
        chip_score=_first_valid(row.get("institutional_score"), row.get("chip_score")),
        credit_score=row.get("credit_score"),
        event_risk_score=_first_valid(row.get("event_risk_score"), row.get("event_score")),
        liquidity_score=row.get("liquidity_score"),
        sector_strength_score=row.get("sector_strength_score"),
        risk_flags=row.get("risk_flags"),
        data_source_warning=row.get("data_source_warning"),
        system_comment=row.get("system_comment"),
        latest_news_titles=[event_text] if event_text else provider_context.latest_news_titles,
        data_source=provider_context.data_source,
        warning_message=warning,
    )


def _provider_warning_without_recomputed_data_gaps(value: object) -> str:
    parts = [part.strip() for part in str(value or "").replace("|", "；").split("；")]
    keep = []
    for part in parts:
        if not part or part.lower() == "nan":
            continue
        if part in _RECOMPUTED_DATA_GAP_WARNINGS:
            continue
        keep.append(part)
    return "；".join(dict.fromkeys(keep))


def _flatten_context(context: MarketContext) -> dict[str, object]:
    return {
        "stock_id": context.symbol,
        "market_intel_status": "WARNING" if context.warning_message else "OK",
        "market_intel_source": context.data_source,
        "market_intel_warning": context.warning_message,
        "market_close": context.close,
        "market_volume": context.volume,
        "volume_change_ratio": context.volume_change_ratio,
        "market_pe_ratio": context.pe_ratio,
        "market_pb_ratio": context.pb_ratio,
        "market_dividend_yield": context.dividend_yield,
        "market_revenue_growth_yoy": context.revenue_growth_yoy,
        "market_eps_growth_yoy": context.eps_growth_yoy,
        "latest_news_titles": " | ".join(context.latest_news_titles),
        "matched_news_keywords": "；".join(context.matched_news_keywords),
        "news_sentiment_score": context.news_sentiment_score,
        "market_fundamental_score": context.fundamental_score,
        "market_valuation_score": context.valuation_score,
        "market_momentum_score": context.momentum_score,
        "market_chip_score": context.chip_score,
        "credit_score": context.credit_score,
        "event_risk_score": context.event_risk_score,
        "liquidity_score": context.liquidity_score,
        "sector_strength_score": context.sector_strength_score,
        "final_market_score": context.final_market_score,
        "confidence_score": context.confidence_score,
        "market_risk_score": context.risk_score,
        "risk_flags": "；".join(context.risk_flags),
        "final_comment": context.final_comment,
        "data_source_warning": context.data_source_warning,
        "system_comment": context.system_comment,
    }


def _neutral_from_candidates(candidates: pd.DataFrame, warning: str) -> pd.DataFrame:
    rows = []
    for _, row in candidates.iterrows():
        rows.append(
            _flatten_context(
                build_market_context(
                    symbol=str(row.get("stock_id", "")),
                    date=str(row.get("trade_date", "")),
                    close=row.get("close"),
                    data_source="disabled",
                    warning_message=warning,
                )
            )
        )
    return pd.DataFrame(rows)


def _read_cache(path: Path) -> pd.DataFrame:
    try:
        records = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return pd.DataFrame(columns=["stock_id"] + MARKET_INTEL_COLUMNS)
    frame = pd.DataFrame(records)
    for column in ["stock_id"] + MARKET_INTEL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return frame[["stock_id"] + MARKET_INTEL_COLUMNS].copy()


def _cache_has_freshness_schema(frame: pd.DataFrame) -> bool:
    if frame.empty or "data_freshness_level" not in frame.columns:
        return False
    values = frame["data_freshness_level"].fillna("").astype(str).str.strip()
    return bool(values.ne("").any())


def _cache_has_recomputed_data_gap_warnings(frame: pd.DataFrame) -> bool:
    if frame.empty or "market_intel_warning" not in frame.columns:
        return False
    warning_rows = [
        str(value).strip()
        for value in frame["market_intel_warning"].fillna("").astype(str).tolist()
        if str(value).strip()
    ]
    if not warning_rows:
        return False
    return all(any(part in warning for part in _RECOMPUTED_DATA_GAP_WARNINGS) for warning in warning_rows)


def _write_cache(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(frame.to_dict(orient="records"), ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(report_dir: Path, date_label: str, frame: pd.DataFrame) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    frame.to_csv(report_dir / f"market_intel_{date_label}.csv", index=False, encoding="utf-8-sig")


def _apply_freshness(
    frame: pd.DataFrame,
    *,
    requested_date: str,
    actual_data_date: str,
    fallback_date: str,
    fallback_reason: str,
    cache_used: bool,
    stale_days_threshold: int = _DEFAULT_STALE_DAYS_THRESHOLD,
) -> pd.DataFrame:
    output = frame.copy()
    age = _date_age_days(requested_date, actual_data_date)
    freshness_level = _data_freshness_level(
        requested_date=requested_date,
        actual_data_date=actual_data_date,
        fallback_date=fallback_date,
        fallback_reason=fallback_reason,
        cache_used=cache_used,
        stale_days_threshold=stale_days_threshold,
    )
    stale = freshness_level in {"STALE", "CACHE"}
    if "market_intel_warning" in output.columns:
        output["market_intel_warning"] = output["market_intel_warning"].apply(_remove_global_freshness_warnings)
    output["requested_date"] = requested_date
    output["actual_data_date"] = actual_data_date
    output["fallback_date"] = fallback_date
    output["fallback_reason"] = fallback_reason
    output["cache_age_days"] = age
    output["is_stale_data"] = stale
    output["data_freshness_level"] = freshness_level
    if cache_used:
        output["market_intel_status"] = _market_status_from_freshness(
            output,
            cache_used=True,
            fallback_reason=fallback_reason,
        )
    if stale:
        output["system_comment"] = output.get("system_comment", pd.Series([""] * len(output))).apply(
            lambda value: _append_warning(value, "市場資料過期，暫不建立買進候選。")
        )
    elif _is_non_trading_fallback(fallback_reason):
        output["system_comment"] = output.get("system_comment", pd.Series([""] * len(output))).apply(
            lambda value: _append_warning(value, "非交易日，使用最近交易日資料。")
        )
    output = _sanitize_legacy_risk_flags(output)
    for column in ["stock_id"] + MARKET_INTEL_COLUMNS:
        if column not in output.columns:
            output[column] = None
    return output[["stock_id"] + MARKET_INTEL_COLUMNS].copy()


def _data_freshness_level(
    *,
    requested_date: str,
    actual_data_date: str,
    fallback_date: str,
    fallback_reason: str,
    cache_used: bool,
    stale_days_threshold: int = _DEFAULT_STALE_DAYS_THRESHOLD,
) -> str:
    age = _date_age_days(requested_date, actual_data_date)
    if age is None:
        return "CACHE" if cache_used else "UNKNOWN"
    if age == 0:
        return "CURRENT"
    if _is_non_trading_fallback(fallback_reason) and _same_date(actual_data_date, fallback_date) and age <= stale_days_threshold:
        return "CURRENT"
    if age > stale_days_threshold:
        return "STALE"
    return "RECENT"


def _market_status_from_freshness(
    frame: pd.DataFrame,
    *,
    cache_used: bool,
    fallback_reason: str,
) -> str:
    level = _frame_freshness_level(frame)
    if level in {"STALE", "CACHE"}:
        return "CACHE" if cache_used else "OK_WITH_WARNING"
    if cache_used or fallback_reason:
        return "OK_WITH_FALLBACK"
    return "OK"


def _frame_freshness_level(frame: pd.DataFrame) -> str:
    if frame.empty or "data_freshness_level" not in frame.columns:
        return "UNKNOWN"
    values = [str(value).strip().upper() for value in frame["data_freshness_level"] if not _is_blank(value)]
    return values[0] if values else "UNKNOWN"


def _status_warning_text(
    frame: pd.DataFrame,
    *,
    fallback_reason: str,
    base_warning: str = "",
) -> str:
    warnings = [base_warning or _warning_text(frame)]
    level = _frame_freshness_level(frame)
    if level in {"STALE", "CACHE"}:
        warnings.append("市場資料過期，不建議短線進場；資料來源缺失或快取資料，需人工確認。")
    elif _is_non_trading_fallback(fallback_reason):
        warnings.append("非交易日，使用最近交易日資料。")
    parts = [warning for warning in warnings if warning]
    return "；".join(dict.fromkeys(parts))[:300]


def _is_non_trading_fallback(fallback_reason: str) -> bool:
    return str(fallback_reason or "").strip().lower() == "no trading data"


def _same_date(left: str, right: str) -> bool:
    left_date = pd.to_datetime(left, errors="coerce")
    right_date = pd.to_datetime(right, errors="coerce")
    if pd.isna(left_date) or pd.isna(right_date):
        return False
    return left_date.normalize() == right_date.normalize()


def _sanitize_legacy_risk_flags(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "risk_flags" not in frame.columns:
        return frame
    output = frame.copy()
    output["risk_flags"] = output["risk_flags"].apply(_legacy_risk_flags_without_positive_or_data)
    return output


def _legacy_risk_flags_without_positive_or_data(value: object) -> str:
    parts = [part.strip() for part in str(value or "").replace("|", "；").split("；") if part.strip()]
    keep = []
    for part in parts:
        if _is_positive_signal_text(part) or _is_data_quality_text(part):
            continue
        keep.append(part)
    return "；".join(dict.fromkeys(keep))


def _is_positive_signal_text(value: str) -> bool:
    return any(keyword in str(value) for keyword in ["相對強勢", "動能分數偏強", "流動性佳", "正向"])


def _is_data_quality_text(value: str) -> bool:
    return any(keyword in str(value) for keyword in ["資料不足", "缺少產業分類", "採中性", "ETF_METADATA_MISSING"])


def _status(
    source_name: str,
    status: str,
    rows: int,
    warning: str = "",
    error_message: str = "",
    requested_period: str = "",
    actual_period: str = "",
    latest_available_period: str = "",
    is_stale: bool = False,
    data_age_days: int | None = None,
    fallback_reason: str = "",
    data_freshness_level: str = "",
    cache_used: bool = False,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source_name": source_name,
                "status": status,
                "rows": rows,
                "warning": warning,
                "error_message": error_message,
                "requested_period": requested_period,
                "actual_period": actual_period,
                "latest_available_period": latest_available_period,
                "source_url_or_name": "market_intel provider",
                "is_real_data": status not in {"MOCK", "DISABLED", "EMPTY"},
                "is_mock": status == "MOCK",
                "is_stale": is_stale,
                "data_age_days": data_age_days,
                "data_freshness_level": data_freshness_level,
                "coverage_ratio": None,
                "affected_symbols_count": rows,
                "fallback_reason": fallback_reason,
                "fallback_action": "cache" if cache_used or status == "CACHE" else "non_trading_day_fallback" if fallback_reason else "",
            }
        ]
    )


def _warning_text(frame: pd.DataFrame) -> str:
    if frame.empty or "market_intel_warning" not in frame.columns:
        return ""
    warnings = frame["market_intel_warning"].fillna("").astype(str).apply(_remove_global_freshness_warnings).str.strip()
    return "；".join(sorted(set(warning for warning in warnings if warning)))[:300]


def _append_warning(value: object, addition: str) -> str:
    text = str(value or "").strip()
    if text.lower() == "nan":
        text = ""
    if addition in text:
        return text
    return f"{text}；{addition}" if text else addition


def _remove_global_freshness_warnings(value: object) -> str:
    parts = [part.strip() for part in str(value or "").replace("|", "；").split("；")]
    keep = []
    for part in parts:
        if not part or part.lower() == "nan":
            continue
        if part in _GLOBAL_FRESHNESS_WARNINGS:
            continue
        keep.append(part)
    return "；".join(dict.fromkeys(keep))


def _stale_days_threshold(config: dict | None) -> int:
    try:
        threshold = int((config or {}).get("stale_days_threshold", _DEFAULT_STALE_DAYS_THRESHOLD))
    except (TypeError, ValueError):
        threshold = _DEFAULT_STALE_DAYS_THRESHOLD
    return max(threshold, 0)


def _date_age_days(requested_date: str, actual_data_date: str) -> int | None:
    requested = pd.to_datetime(requested_date, errors="coerce")
    actual = pd.to_datetime(actual_data_date, errors="coerce")
    if pd.isna(requested) or pd.isna(actual):
        return None
    return max(int((requested.normalize() - actual.normalize()).days), 0)


def _has_stale_data(frame: pd.DataFrame) -> bool:
    if frame.empty or "is_stale_data" not in frame.columns:
        return False
    return bool(frame["is_stale_data"].apply(_to_bool).any())


def _max_cache_age(frame: pd.DataFrame) -> int | None:
    if frame.empty or "cache_age_days" not in frame.columns:
        return None
    values = pd.to_numeric(frame["cache_age_days"], errors="coerce").dropna()
    if values.empty:
        return None
    return int(values.max())


def _first_valid(*values: object) -> object:
    for value in values:
        if not _is_blank(value):
            return value
    return None


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _date_label(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return pd.Timestamp.today(tz="Asia/Taipei").strftime("%Y%m%d")
    return parsed.strftime("%Y%m%d")


def _date_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}
