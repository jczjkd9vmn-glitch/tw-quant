"""Market intelligence report and cache utilities."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from tw_quant.market_intel.providers.base import MarketContext
from tw_quant.market_intel.providers.mock_provider import MockMarketIntelProvider
from tw_quant.market_intel.providers.yfinance_provider import YFinanceMarketIntelProvider
from tw_quant.market_intel.scoring import build_market_context


MARKET_INTEL_COLUMNS = [
    "market_intel_status",
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
    "final_market_score",
    "confidence_score",
    "market_risk_score",
    "risk_flags",
    "final_comment",
]


def build_market_intel_report(
    candidates: pd.DataFrame,
    reports_dir: str | Path = "reports",
    trade_date: str | pd.Timestamp | None = None,
    config: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if candidates.empty:
        return pd.DataFrame(columns=["stock_id"] + MARKET_INTEL_COLUMNS), _status("market_intel", "EMPTY", 0)

    active_config = config or {}
    if active_config.get("enabled", True) is False:
        frame = _neutral_from_candidates(candidates, "market intelligence disabled")
        return frame, _status("market_intel", "DISABLED", len(frame), warning="market intelligence disabled")

    report_dir = Path(reports_dir)
    date_label = _date_label(trade_date or candidates.get("trade_date", pd.Series([""])).iloc[0])
    cache_path = report_dir / "cache" / f"market_intel_{date_label}.json"
    cache_enabled = bool(active_config.get("cache_enabled", True))
    if cache_enabled and cache_path.exists():
        frame = _read_cache(cache_path)
        return frame, _status("market_intel", "CACHE", len(frame), warning=_warning_text(frame))

    provider_name = str(active_config.get("provider", "mock")).strip().lower()
    provider = YFinanceMarketIntelProvider() if provider_name == "yfinance" else MockMarketIntelProvider()
    symbols = candidates["stock_id"].astype(str).tolist()
    provider_contexts = {context.symbol: context for context in provider.fetch(symbols, as_of=_date_text(date_label))}
    rows = []
    for _, row in candidates.iterrows():
        symbol = str(row.get("stock_id", "")).strip()
        context = _context_from_candidate(row, provider_contexts.get(symbol), date_label)
        rows.append(_flatten_context(context))
    frame = pd.DataFrame(rows)
    if cache_enabled:
        _write_cache(cache_path, frame)
    csv_path = report_dir / f"market_intel_{date_label}.csv"
    report_dir.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    warning = _warning_text(frame)
    return frame, _status("market_intel", "OK_WITH_WARNING" if warning else "OK", len(frame), warning=warning)


def _context_from_candidate(row: pd.Series, provider_context: MarketContext | None, date_label: str) -> MarketContext:
    provider_context = provider_context or build_market_context(
        symbol=str(row.get("stock_id", "")),
        date=_date_text(date_label),
        warning_message="市場資料 provider 無回應，使用候選股資料補足",
    )
    event_text = " ".join(
        str(row.get(column, ""))
        for column in ["event_reason", "multi_factor_reason", "reason"]
        if not _is_blank(row.get(column))
    )
    warning = provider_context.warning_message
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
        latest_news_titles=[event_text] if event_text else provider_context.latest_news_titles,
        data_source=provider_context.data_source,
        warning_message=warning,
    )


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
        "matched_news_keywords": "、".join(context.matched_news_keywords),
        "news_sentiment_score": context.news_sentiment_score,
        "market_fundamental_score": context.fundamental_score,
        "market_valuation_score": context.valuation_score,
        "market_momentum_score": context.momentum_score,
        "final_market_score": context.final_market_score,
        "confidence_score": context.confidence_score,
        "market_risk_score": context.risk_score,
        "risk_flags": "、".join(context.risk_flags),
        "final_comment": context.final_comment,
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
    return pd.DataFrame(records)


def _write_cache(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(frame.to_dict(orient="records"), ensure_ascii=False, indent=2), encoding="utf-8")


def _status(
    source_name: str,
    status: str,
    rows: int,
    warning: str = "",
    error_message: str = "",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source_name": source_name,
                "status": status,
                "rows": rows,
                "warning": warning,
                "error_message": error_message,
            }
        ]
    )


def _warning_text(frame: pd.DataFrame) -> str:
    if frame.empty or "market_intel_warning" not in frame.columns:
        return ""
    warnings = frame["market_intel_warning"].fillna("").astype(str).str.strip()
    return "；".join(sorted(set(warning for warning in warnings if warning)))[:300]


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
