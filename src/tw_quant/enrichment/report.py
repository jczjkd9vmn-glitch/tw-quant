"""Generate AI/rule-based enrichment reports from existing data files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from tw_quant.config import load_config
from tw_quant.enrichment.ai_enricher import AIEnricher
from tw_quant.enrichment.industry import load_industry_map
from tw_quant.enrichment.rule_based_enricher import ENRICHMENT_COLUMNS, RuleBasedEnricher


@dataclass(frozen=True)
class EnrichmentResult:
    trade_date: str
    output_path: Path | None
    cache_path: Path | None
    enrichment: pd.DataFrame
    warning: str = ""


def generate_ai_enrichment(
    reports_dir: str | Path = "reports",
    data_dir: str | Path = "data",
    config_path: str | Path = "config.yaml",
    trade_date: str | None = None,
) -> EnrichmentResult:
    config = load_config(config_path)
    enrich_config = config.get("ai_enrichment", {})
    report_dir = Path(reports_dir)
    data_path = Path(data_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    target_date = trade_date or _latest_date_label(report_dir) or pd.Timestamp.today().strftime("%Y-%m-%d")
    if not enrich_config.get("enabled", True):
        frame = pd.DataFrame(columns=ENRICHMENT_COLUMNS)
        return EnrichmentResult(target_date, None, None, frame, warning="ai_enrichment disabled")

    base = _load_universe(report_dir, data_path, target_date, config_path)
    if base.empty:
        frame = pd.DataFrame(columns=ENRICHMENT_COLUMNS)
        output_path = report_dir / f"ai_enrichment_{_date_label(target_date)}.csv"
        frame.to_csv(output_path, index=False, encoding="utf-8-sig")
        return EnrichmentResult(target_date, output_path, None, frame, warning="no symbols for enrichment")

    max_symbols = int(enrich_config.get("max_symbols_per_run", 30))
    base = base.head(max_symbols).copy()
    allow_external = bool(enrich_config.get("allow_external_ai", False))
    provider = str(enrich_config.get("provider", "rule_based")).strip().lower()
    enricher = AIEnricher(allow_external_ai=allow_external) if provider == "ai" else RuleBasedEnricher()
    try:
        enrichment = enricher.enrich(base, target_date)
        warning = ""
    except Exception as exc:
        enrichment = RuleBasedEnricher().enrich(base, target_date)
        warning = f"AI enrichment failed; used rule-based fallback: {type(exc).__name__}: {exc}"

    output_path = report_dir / f"ai_enrichment_{_date_label(target_date)}.csv"
    enrichment.to_csv(output_path, index=False, encoding="utf-8-sig")
    cache_path = None
    if enrich_config.get("cache_enabled", True):
        cache_dir = report_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"ai_enrichment_{_date_label(target_date)}.json"
        cache_path.write_text(enrichment.to_json(orient="records", force_ascii=False), encoding="utf-8")
    return EnrichmentResult(target_date, output_path, cache_path, enrichment, warning=warning)


def _load_universe(report_dir: Path, data_dir: Path, target_date: str, config_path: str | Path) -> pd.DataFrame:
    frames = [
        _read_latest(report_dir, "trading_decisions_*.csv"),
        _read_latest(report_dir, "candidates_*.csv"),
        _read_latest(report_dir, "market_intel_*.csv"),
    ]
    base = _combine_by_symbol(*frames)
    if base.empty:
        return base
    base = _merge_optional(base, load_industry_map(data_dir=data_dir, config_path=config_path))
    for data_file in [
        data_dir / "valuation.csv",
        data_dir / "financials.csv",
        data_dir / "margin_short.csv",
        data_dir / "institutional.csv",
        data_dir / "liquidity.csv",
        data_dir / "sector_strength.csv",
    ]:
        base = _merge_optional(base, _read_csv(data_file))
    base["trade_date"] = base.get("trade_date", target_date)
    return _derive_context_columns(base)


def _derive_context_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for column in ["margin_change_5d", "margin_change_20d", "price_return_5d", "price_return_20d", "institutional_net_buy_5d", "volume_change_5d"]:
        if column not in result.columns:
            result[column] = ""
    if "margin_change_5d" in result.columns and result["margin_change_5d"].fillna("").astype(str).eq("").all():
        result["margin_change_5d"] = result.get("margin_change", "")
    if "price_return_5d" in result.columns and result["price_return_5d"].fillna("").astype(str).eq("").all():
        result["price_return_5d"] = result.get("stock_return_5d", "")
    if "price_return_20d" in result.columns and result["price_return_20d"].fillna("").astype(str).eq("").all():
        result["price_return_20d"] = result.get("stock_return_20d", "")
    if "institutional_net_buy_5d" in result.columns and result["institutional_net_buy_5d"].fillna("").astype(str).eq("").all():
        result["institutional_net_buy_5d"] = result.get("institutional_5d_sum", result.get("total_institutional_net_buy", ""))
    if "industry_source" not in result.columns:
        result["industry_source"] = result.get("source", "")
    if "sector_strength_mode" not in result.columns:
        warnings = (
            result["sector_strength_warning"]
            if "sector_strength_warning" in result.columns
            else pd.Series([""] * len(result), index=result.index)
        )
        result["sector_strength_mode"] = warnings.apply(
            lambda value: "market_relative_fallback" if "全市場" in str(value) else "industry_relative"
        )
    return result


def _combine_by_symbol(*frames: pd.DataFrame) -> pd.DataFrame:
    usable = [frame.copy() for frame in frames if not frame.empty and "stock_id" in frame.columns]
    if not usable:
        return pd.DataFrame()
    combined = usable[0].copy()
    combined["stock_id"] = combined["stock_id"].astype(str).str.strip()
    for frame in usable[1:]:
        combined = _merge_optional(combined, frame)
    return combined.drop_duplicates("stock_id", keep="first")


def _merge_optional(base: pd.DataFrame, extra: pd.DataFrame) -> pd.DataFrame:
    if base.empty or extra.empty or "stock_id" not in extra.columns:
        return base
    result = base.copy()
    extra = extra.copy()
    result["stock_id"] = result["stock_id"].astype(str).str.strip()
    extra["stock_id"] = extra["stock_id"].astype(str).str.strip()
    lookup = extra.drop_duplicates("stock_id").set_index("stock_id")
    for column in lookup.columns:
        if column == "stock_id":
            continue
        if column not in result.columns:
            result[column] = result["stock_id"].map(lookup[column])
        else:
            mapped = result["stock_id"].map(lookup[column])
            result[column] = result[column].where(~result[column].apply(_blank), mapped)
    return result


def _latest_date_label(report_dir: Path) -> str | None:
    summaries = sorted(report_dir.glob("daily_summary_*.csv"))
    if summaries:
        frame = _read_csv(summaries[-1])
        if not frame.empty and "trade_date" in frame.columns:
            return pd.to_datetime(frame.iloc[0]["trade_date"]).strftime("%Y-%m-%d")
    candidates = sorted(report_dir.glob("candidates_*.csv"))
    if not candidates:
        return None
    return pd.to_datetime(candidates[-1].stem.rsplit("_", 1)[-1]).strftime("%Y-%m-%d")


def _read_latest(directory: Path, pattern: str) -> pd.DataFrame:
    files = sorted(directory.glob(pattern))
    return _read_csv(files[-1]) if files else pd.DataFrame()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype={"stock_id": str}, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


def _blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() == "" or str(value).strip().lower() in {"nan", "none", "-"}


def _date_label(value: str) -> str:
    return pd.to_datetime(value).strftime("%Y%m%d")
