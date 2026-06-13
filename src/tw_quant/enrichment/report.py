"""Generate AI/rule-based enrichment reports from existing data files."""

from __future__ import annotations

from dataclasses import dataclass
import json
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
    evidence_path: Path | None = None


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
        frame.to_csv(output_path, index=False, encoding="utf-8")
        evidence_path = report_dir / f"enrichment_evidence_{_date_label(target_date)}.csv"
        _evidence_frame(frame, target_date).to_csv(evidence_path, index=False, encoding="utf-8")
        return EnrichmentResult(
            target_date,
            output_path,
            None,
            frame,
            warning="no symbols for enrichment",
            evidence_path=evidence_path,
        )

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
    enrichment.to_csv(output_path, index=False, encoding="utf-8")
    evidence_path = report_dir / f"enrichment_evidence_{_date_label(target_date)}.csv"
    _evidence_frame(enrichment, target_date).to_csv(evidence_path, index=False, encoding="utf-8")
    cache_path = None
    if enrich_config.get("cache_enabled", True):
        cache_dir = report_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"ai_enrichment_{_date_label(target_date)}.json"
        cache_path.write_text(enrichment.to_json(orient="records", force_ascii=False), encoding="utf-8")
    return EnrichmentResult(target_date, output_path, cache_path, enrichment, warning=warning, evidence_path=evidence_path)


def _evidence_frame(enrichment: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    columns = [
        "trade_date",
        "stock_id",
        "stock_name",
        "source_name",
        "source_type",
        "source_date",
        "field_name",
        "field_value",
        "evidence_summary",
        "fallback_used",
        "confidence_impact",
    ]
    rows: list[dict[str, object]] = []
    if enrichment.empty or "source_evidence_json" not in enrichment.columns:
        return pd.DataFrame(columns=columns)
    for _, row in enrichment.iterrows():
        stock_id = str(row.get("stock_id", "")).strip()
        stock_name = str(row.get("stock_name", "")).strip()
        try:
            evidence_items = json.loads(str(row.get("source_evidence_json") or "[]"))
        except json.JSONDecodeError:
            evidence_items = []
        for item in evidence_items if isinstance(evidence_items, list) else []:
            if not isinstance(item, dict):
                continue
            field = str(item.get("field_name", "")).strip()
            value = item.get("field_value", "")
            fallback = "fallback" in str(item.get("warning", "")).lower() or "fallback" in str(value).lower()
            rows.append(
                {
                    "trade_date": trade_date,
                    "stock_id": stock_id,
                    "stock_name": stock_name,
                    "source_name": item.get("source_name", ""),
                    "source_type": item.get("source_type", ""),
                    "source_date": item.get("source_date", ""),
                    "field_name": field,
                    "field_value": value,
                    "evidence_summary": f"{field}={value}" if field else str(value),
                    "fallback_used": fallback,
                    "confidence_impact": item.get("confidence", ""),
                }
            )
    return pd.DataFrame(rows, columns=columns)


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
    index = result.index
    columns = {column: result[column] for column in result.columns}

    def current(column: str, default: object = "") -> pd.Series:
        if column in columns:
            return columns[column]
        if isinstance(default, pd.Series):
            return default.reindex(index)
        return pd.Series([default] * len(result), index=index)

    for column in ["margin_change_5d", "margin_change_20d", "price_return_5d", "price_return_20d", "institutional_net_buy_5d", "volume_change_5d"]:
        columns.setdefault(column, pd.Series([""] * len(result), index=index))
    if _series_is_blank(columns["margin_change_5d"]):
        columns["margin_change_5d"] = current("margin_change")
    if _series_is_blank(columns["price_return_5d"]):
        columns["price_return_5d"] = current("stock_return_5d")
    if _series_is_blank(columns["price_return_20d"]):
        columns["price_return_20d"] = current("stock_return_20d")
    if _series_is_blank(columns["institutional_net_buy_5d"]):
        columns["institutional_net_buy_5d"] = current("institutional_5d_sum", current("total_institutional_net_buy"))
    if "industry_source" not in columns:
        columns["industry_source"] = current("source")
    if "industry" not in columns and "industry_main" in columns:
        columns["industry"] = columns["industry_main"]
    if "industry_main" not in columns and "industry" in columns:
        columns["industry_main"] = columns["industry"]
    if "industry_sub" not in columns and "sub_industry" in columns:
        columns["industry_sub"] = columns["sub_industry"]
    if "sector_strength_mode" not in columns:
        warnings = current("sector_strength_warning")
        columns["sector_strength_mode"] = warnings.apply(
            lambda value: "market_relative_fallback" if "全市場" in str(value) else "industry_relative"
        )
    return pd.DataFrame(columns, index=index)


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
    result = result.assign(stock_id=result["stock_id"].astype(str).str.strip())
    extra["stock_id"] = extra["stock_id"].astype(str).str.strip()
    lookup = extra.drop_duplicates("stock_id").set_index("stock_id")
    extra_columns = [column for column in lookup.columns if column != "stock_id"]
    if not extra_columns:
        return result
    aligned = result[["stock_id"]].merge(
        lookup[extra_columns],
        left_on="stock_id",
        right_index=True,
        how="left",
        sort=False,
    )
    aligned.index = result.index
    aligned = aligned.drop(columns=["stock_id"])
    merged_columns: dict[str, pd.Series] = {}
    for column in lookup.columns:
        if column in result.columns and column in aligned.columns:
            merged_columns[column] = result[column].where(~result[column].apply(_blank), aligned[column])
    output_columns: dict[str, pd.Series] = {}
    for column in result.columns:
        output_columns[column] = merged_columns.get(column, result[column])
    for column in extra_columns:
        if column not in result.columns:
            output_columns[column] = aligned[column]
    return pd.DataFrame(output_columns, index=result.index)


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
        return pd.read_csv(path, dtype={"stock_id": str}, encoding="utf-8")
    except Exception:
        return pd.DataFrame()


def _blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() == "" or str(value).strip().lower() in {"nan", "none", "-"}


def _series_is_blank(series: pd.Series) -> bool:
    return series.fillna("").astype(str).str.strip().str.lower().isin(["", "nan", "none", "-"]).all()


def _date_label(value: str) -> str:
    return pd.to_datetime(value).strftime("%Y%m%d")
