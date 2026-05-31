"""Generate proposal-only industry candidates from AnySearch results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from tw_quant.data_sources.anysearch_client import AnySearchClient, AnySearchError


ANYSEARCH_INDUSTRY_COLUMNS = [
    "stock_id",
    "stock_name",
    "query",
    "proposed_market_type",
    "proposed_industry",
    "proposed_sub_industry",
    "source_title",
    "source_url",
    "source_type",
    "confidence",
    "reason",
    "status",
    "checked_at",
]

DEFAULT_ANYSEARCH_CONFIG = {
    "enabled": False,
    "max_requests_per_run": 30,
    "cache_days": 14,
    "timeout_seconds": 15,
    "retry_count": 2,
    "only_priority_levels": ["HIGH"],
    "write_mode": "proposal_only",
}


@dataclass(frozen=True)
class AnySearchIndustryResearchResult:
    candidates: pd.DataFrame
    output_path: Path
    status: str
    api_calls: int = 0
    cache_hits: int = 0
    warning: str = ""


def generate_anysearch_industry_research_report(
    reports_dir: str | Path = "reports",
    config_path: str | Path = "config/anysearch.yml",
    cache_dir: str | Path = "data/cache/anysearch",
    client: AnySearchClient | Any = None,
    trade_date: str | pd.Timestamp | None = None,
) -> AnySearchIndustryResearchResult:
    report_dir = Path(reports_dir)
    output_path = report_dir / "anysearch_industry_candidates.csv"
    report_dir.mkdir(parents=True, exist_ok=True)

    config = load_anysearch_config(config_path)
    if not bool(config.get("enabled", False)):
        frame = _empty_candidates()
        _write_candidates(output_path, frame)
        return AnySearchIndustryResearchResult(
            candidates=frame,
            output_path=output_path,
            status="SKIPPED",
            warning="disabled",
        )

    if str(config.get("write_mode", "")).strip().lower() != "proposal_only":
        frame = _empty_candidates()
        _write_candidates(output_path, frame)
        return AnySearchIndustryResearchResult(
            candidates=frame,
            output_path=output_path,
            status="SKIPPED",
            warning="write_mode must be proposal_only",
        )

    if not os.getenv("ANYSEARCH_API_KEY", "").strip():
        frame = _empty_candidates()
        _write_candidates(output_path, frame)
        return AnySearchIndustryResearchResult(
            candidates=frame,
            output_path=output_path,
            status="SKIPPED",
            warning="missing ANYSEARCH_API_KEY",
        )

    priority = _read_csv(report_dir / "missing_industry_priority.csv")
    targets = _select_targets(
        priority,
        levels=[str(level).upper() for level in config.get("only_priority_levels", ["HIGH"])],
        limit=int(config.get("max_requests_per_run", 30) or 30),
    )
    if targets.empty:
        frame = _empty_candidates()
        _write_candidates(output_path, frame)
        return AnySearchIndustryResearchResult(
            candidates=frame,
            output_path=output_path,
            status="OK",
            warning="no priority targets",
        )

    active_client = client or AnySearchClient.from_env(
        timeout_seconds=int(config.get("timeout_seconds", 15) or 15),
        retry_count=int(config.get("retry_count", 2) or 2),
    )
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    checked_at = _now_text()
    rows: list[dict[str, object]] = []
    api_calls = 0
    cache_hits = 0
    warnings: list[str] = []

    for _, row in targets.iterrows():
        query = build_anysearch_query(row)
        cached = _read_fresh_cache(cache_path, row, query, int(config.get("cache_days", 14) or 14))
        if cached:
            cache_hits += 1
            rows.append(_normalize_candidate(cached["candidate"]))
            continue

        try:
            raw_result = _search(active_client, query)
            api_calls += 1
            candidate = build_candidate_from_search(row, query, raw_result, checked_at)
            _write_cache(cache_path, row, query, raw_result, candidate)
            rows.append(candidate)
        except (AnySearchError, RuntimeError, OSError, ValueError, TypeError) as exc:
            warnings.append(f"{row.get('stock_id')}: {exc}")
            rows.append(_manual_check_row(row, query, checked_at, f"AnySearch 查詢失敗：{exc}"))

    frame = pd.DataFrame(rows, columns=ANYSEARCH_INDUSTRY_COLUMNS)
    _write_candidates(output_path, frame)
    return AnySearchIndustryResearchResult(
        candidates=frame,
        output_path=output_path,
        status="OK",
        api_calls=api_calls,
        cache_hits=cache_hits,
        warning="; ".join(warnings),
    )


def build_anysearch_query(row: pd.Series | dict[str, object]) -> str:
    stock_id = str(row.get("stock_id", "") or "").strip()
    stock_name = str(row.get("stock_name", "") or "").strip()
    return f"{stock_id} {stock_name} ETF ETN 追蹤指數 基金 產業分類 官方"


def build_candidate_from_search(
    row: pd.Series | dict[str, object],
    query: str,
    raw_result: object,
    checked_at: str,
) -> dict[str, object]:
    stock_id = str(row.get("stock_id", "") or "").strip()
    stock_name = str(row.get("stock_name", "") or "").strip()
    source = _extract_source(raw_result)
    combined_text = " ".join(
        [
            stock_id,
            stock_name,
            source.get("title", ""),
            source.get("snippet", ""),
            _raw_text(raw_result),
        ]
    )
    proposed_market_type = _proposed_market_type(stock_id, combined_text)
    proposed_industry = proposed_market_type if proposed_market_type in {"ETF", "ETN"} else ""
    proposed_sub_industry = _proposed_sub_industry(stock_name, combined_text, proposed_market_type)
    source_url = source.get("url", "")
    confidence = _confidence(source_url, proposed_market_type, proposed_industry, proposed_sub_industry, combined_text)
    if not source_url:
        confidence = min(confidence, 0.5)
    status = "PENDING_REVIEW" if source_url and proposed_sub_industry and confidence >= 0.65 else "NEEDS_MANUAL_CHECK"
    reason = _candidate_reason(source_url, proposed_market_type, proposed_sub_industry, confidence)
    return _normalize_candidate(
        {
            "stock_id": stock_id,
            "stock_name": stock_name,
            "query": query,
            "proposed_market_type": proposed_market_type,
            "proposed_industry": proposed_industry,
            "proposed_sub_industry": proposed_sub_industry,
            "source_title": source.get("title", ""),
            "source_url": source_url,
            "source_type": _source_type(source_url),
            "confidence": round(confidence, 2),
            "reason": reason,
            "status": status,
            "checked_at": checked_at,
        }
    )


def load_anysearch_config(path: str | Path) -> dict[str, object]:
    config = dict(DEFAULT_ANYSEARCH_CONFIG)
    config_path = Path(path)
    if not config_path.exists():
        return config
    parsed = _parse_anysearch_yaml(config_path.read_text(encoding="utf-8"))
    config.update(parsed.get("anysearch", {}))
    config["max_requests_per_run"] = min(max(int(config.get("max_requests_per_run", 30) or 30), 0), 30)
    if not isinstance(config.get("only_priority_levels"), list):
        config["only_priority_levels"] = ["HIGH"]
    return config


def _select_targets(priority: pd.DataFrame, *, levels: list[str], limit: int) -> pd.DataFrame:
    if priority.empty or "stock_id" not in priority.columns:
        return pd.DataFrame()
    frame = priority.copy()
    level_series = frame.get("priority_level", pd.Series([""] * len(frame))).fillna("").astype(str).str.upper()
    frame = frame[level_series.isin(set(levels))].copy()
    if frame.empty:
        return frame
    score = pd.to_numeric(frame.get("priority_score", pd.Series([0] * len(frame))), errors="coerce").fillna(0)
    frame["_priority_score"] = score
    frame = frame.sort_values(["_priority_score", "stock_id"], ascending=[False, True])
    return frame.drop(columns=["_priority_score"]).head(limit)


def _read_fresh_cache(
    cache_dir: Path,
    row: pd.Series | dict[str, object],
    query: str,
    cache_days: int,
) -> dict[str, Any] | None:
    path = _cache_file(cache_dir, str(row.get("stock_id", "") or ""), query)
    if not path.exists():
        return None
    try:
        cached = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    checked_at = _parse_datetime(cached.get("checked_at"))
    if checked_at is None:
        return None
    age_seconds = (datetime.now(timezone.utc) - checked_at).total_seconds()
    if age_seconds > max(int(cache_days), 0) * 86400:
        return None
    candidate = cached.get("candidate")
    if not isinstance(candidate, dict):
        return None
    return cached


def _write_cache(
    cache_dir: Path,
    row: pd.Series | dict[str, object],
    query: str,
    raw_result: object,
    candidate: dict[str, object],
) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "stock_id": str(row.get("stock_id", "") or ""),
        "query": query,
        "checked_at": str(candidate.get("checked_at", _now_text())),
        "raw_result": raw_result,
        "candidate": candidate,
    }
    _cache_file(cache_dir, str(row.get("stock_id", "") or ""), query).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _cache_file(cache_dir: Path, stock_id: str, query: str) -> Path:
    digest = hashlib.sha256(query.encode("utf-8")).hexdigest()[:12]
    safe_stock_id = re.sub(r"[^A-Za-z0-9]", "_", stock_id)
    return cache_dir / f"{safe_stock_id}_{digest}.json"


def _search(client: Any, query: str) -> object:
    try:
        return client.search(query, max_results=5)
    except TypeError:
        return client.search(query)


def _extract_source(raw_result: object) -> dict[str, str]:
    data = _json_or_raw(raw_result)
    source = _source_from_data(data)
    if source.get("url"):
        return source

    text = _raw_text(raw_result)
    markdown = re.search(r"\[([^\]]{2,160})\]\((https?://[^)\s]+)\)", text)
    if markdown:
        return {"title": markdown.group(1).strip(), "url": markdown.group(2).strip(), "snippet": text[:500]}
    url_match = re.search(r"https?://[^\s)>\]]+", text)
    if url_match:
        return {"title": _line_before_url(text, url_match.start()), "url": url_match.group(0).strip(), "snippet": text[:500]}
    return {"title": "", "url": "", "snippet": text[:500]}


def _source_from_data(data: object) -> dict[str, str]:
    if isinstance(data, dict):
        url = str(data.get("url") or data.get("link") or data.get("source_url") or "").strip()
        title = str(data.get("title") or data.get("name") or data.get("source_title") or "").strip()
        snippet = str(data.get("snippet") or data.get("description") or data.get("content") or "").strip()
        if url:
            return {"title": title, "url": url, "snippet": snippet}
        for value in data.values():
            found = _source_from_data(value)
            if found.get("url"):
                return found
    if isinstance(data, list):
        for item in data:
            found = _source_from_data(item)
            if found.get("url"):
                return found
    return {"title": "", "url": "", "snippet": ""}


def _json_or_raw(raw_result: object) -> object:
    if not isinstance(raw_result, str):
        return raw_result
    try:
        return json.loads(raw_result)
    except json.JSONDecodeError:
        return raw_result


def _raw_text(raw_result: object) -> str:
    if isinstance(raw_result, str):
        return raw_result
    try:
        return json.dumps(raw_result, ensure_ascii=False)
    except TypeError:
        return str(raw_result)


def _line_before_url(text: str, url_start: int) -> str:
    prefix = text[:url_start].splitlines()
    for line in reversed(prefix):
        stripped = line.strip(" -#*:|")
        if stripped:
            return stripped[:160]
    return ""


def _proposed_market_type(stock_id: str, text: str) -> str:
    upper = text.upper()
    if stock_id.startswith("020") or "ETN" in upper:
        return "ETN"
    if stock_id.startswith("00") or "ETF" in upper:
        return "ETF"
    return ""


def _proposed_sub_industry(stock_name: str, text: str, market_type: str) -> str:
    if market_type not in {"ETF", "ETN"}:
        return ""
    rules = [
        ("未來通訊", "全球通訊 ETF"),
        ("全球創新", "全球創新 ETF"),
        ("半導體", "半導體 ETF"),
        ("高股息", "高股息 ETF"),
        ("ESG", "ESG ETF"),
        ("科技", "科技 ETF"),
        ("債", "債券 ETF"),
    ]
    combined = f"{stock_name} {text}".upper()
    for keyword, label in rules:
        if keyword.upper() in combined:
            return label.replace("ETF", market_type)
    return ""


def _confidence(
    source_url: str,
    market_type: str,
    industry: str,
    sub_industry: str,
    text: str,
) -> float:
    score = 0.4
    if source_url:
        score += 0.2
    if market_type and industry:
        score += 0.1
    if sub_industry:
        score += 0.1
    if any(keyword in text.upper() for keyword in ["ETF", "ETN", "追蹤", "基金"]):
        score += 0.1
    return min(score, 0.85)


def _candidate_reason(source_url: str, market_type: str, sub_industry: str, confidence: float) -> str:
    if not source_url:
        return "未取得明確來源網址，需人工查證。"
    if not market_type:
        return "來源存在但尚無法判定市場類型，需人工查證。"
    if confidence < 0.65:
        return "來源訊息不足，僅列為人工檢查候選。"
    return f"來源顯示為 {market_type}，候選子類型為 {sub_industry}；需人工確認後才可寫入正式產業分類。"


def _source_type(source_url: str) -> str:
    if not source_url:
        return ""
    host = urlparse(source_url).netloc.lower()
    if "twse.com.tw" in host:
        return "twse"
    if "tpex.org.tw" in host:
        return "tpex"
    return "web"


def _manual_check_row(
    row: pd.Series | dict[str, object],
    query: str,
    checked_at: str,
    reason: str,
) -> dict[str, object]:
    return _normalize_candidate(
        {
            "stock_id": str(row.get("stock_id", "") or "").strip(),
            "stock_name": str(row.get("stock_name", "") or "").strip(),
            "query": query,
            "proposed_market_type": _proposed_market_type(str(row.get("stock_id", "") or ""), str(row.get("stock_name", "") or "")),
            "proposed_industry": "",
            "proposed_sub_industry": "",
            "source_title": "",
            "source_url": "",
            "source_type": "",
            "confidence": 0.0,
            "reason": reason,
            "status": "NEEDS_MANUAL_CHECK",
            "checked_at": checked_at,
        }
    )


def _normalize_candidate(row: dict[str, object]) -> dict[str, object]:
    normalized = {column: row.get(column, "") for column in ANYSEARCH_INDUSTRY_COLUMNS}
    status = str(normalized.get("status", "") or "").upper()
    normalized["status"] = status if status in {"PENDING_REVIEW", "NEEDS_MANUAL_CHECK"} else "NEEDS_MANUAL_CHECK"
    try:
        confidence = float(normalized.get("confidence", 0) or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    if not str(normalized.get("source_url", "") or "").strip():
        confidence = min(confidence, 0.5)
    normalized["confidence"] = round(max(min(confidence, 1.0), 0.0), 2)
    return normalized


def _empty_candidates() -> pd.DataFrame:
    return pd.DataFrame(columns=ANYSEARCH_INDUSTRY_COLUMNS)


def _write_candidates(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.reindex(columns=ANYSEARCH_INDUSTRY_COLUMNS).to_csv(path, index=False, encoding="utf-8-sig")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _parse_anysearch_yaml(text: str) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    section = ""
    pending_list_key = ""
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if indent == 0 and stripped.endswith(":"):
            section = stripped[:-1].strip()
            result.setdefault(section, {})
            pending_list_key = ""
            continue
        if not section:
            continue
        if stripped.startswith("- ") and pending_list_key:
            result[section].setdefault(pending_list_key, [])
            value = _parse_scalar(stripped[2:].strip())
            current = result[section][pending_list_key]
            if isinstance(current, list):
                current.append(value)
            continue
        if ":" not in stripped:
            continue
        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if raw_value == "":
            result[section][key] = []
            pending_list_key = key
        else:
            result[section][key] = _parse_scalar(raw_value)
            pending_list_key = ""
    return result


def _parse_scalar(value: str) -> object:
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part.strip()) for part in inner.split(",")]
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
