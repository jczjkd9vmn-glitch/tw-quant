"""Official TWSE/TPEx market index ingestion helpers.

The output is intentionally kept as a CSV-friendly table. Benchmark logic reads
only this explicit official source instead of guessing index rows from stock
price tables.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


MARKET_INDEX_COLUMNS = [
    "trade_date",
    "index_id",
    "index_name",
    "open",
    "high",
    "low",
    "close",
    "source",
    "is_official",
]

TWSE_TAIEX_URL = "https://openapi.twse.com.tw/v1/indicesReport/MI_5MINS_HIST"
TPEX_INDEX_URL = "https://www.tpex.org.tw/openapi/v1/tpex_index"

TAIEX_SOURCE = "twse_openapi:MI_5MINS_HIST"
TPEX_SOURCE = "tpex_openapi:tpex_index"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}


class OfficialMarketIndexError(RuntimeError):
    """Raised when all official market index sources fail."""


def fetch_official_market_indices(
    *,
    timeout_seconds: int = 15,
    session: object | None = None,
    sources: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Fetch official TAIEX and TPEx index rows from public exchange APIs."""

    enabled_sources = {str(source).lower() for source in (sources or ["twse", "tpex"])}
    requester = _requester(session)
    frames: list[pd.DataFrame] = []
    errors: list[str] = []

    if "twse" in enabled_sources or "taiex" in enabled_sources:
        try:
            frames.append(fetch_twse_taiex(timeout_seconds=timeout_seconds, requester=requester))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"TAIEX: {type(exc).__name__}: {exc}")

    if "tpex" in enabled_sources:
        try:
            frames.append(fetch_tpex_index(timeout_seconds=timeout_seconds, requester=requester))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"TPEx: {type(exc).__name__}: {exc}")

    combined = _normalize_output(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
    if combined.empty and errors:
        raise OfficialMarketIndexError("; ".join(errors))
    return combined


def fetch_twse_taiex(*, timeout_seconds: int = 15, requester: object | None = None) -> pd.DataFrame:
    """Fetch TAIEX OHLC rows from TWSE OpenAPI."""

    payload = _get_json(requester or _default_get, TWSE_TAIEX_URL, timeout_seconds)
    rows: list[dict[str, object]] = []
    for item in _list_payload(payload):
        trade_date = _date_text(_field(item, ["Date", "日期"]))
        close = _number(_field(item, ["ClosingIndex", "收盤指數"]))
        if not trade_date or close is None:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": _number(_field(item, ["OpeningIndex", "開盤指數"])),
                "high": _number(_field(item, ["HighestIndex", "最高指數"])),
                "low": _number(_field(item, ["LowestIndex", "最低指數"])),
                "close": close,
                "source": TAIEX_SOURCE,
                "is_official": True,
            }
        )
    return _normalize_output(pd.DataFrame(rows))


def fetch_tpex_index(*, timeout_seconds: int = 15, requester: object | None = None) -> pd.DataFrame:
    """Fetch TPEx index OHLC rows from TPEx OpenAPI."""

    payload = _get_json(requester or _default_get, TPEX_INDEX_URL, timeout_seconds)
    rows: list[dict[str, object]] = []
    for item in _list_payload(payload):
        trade_date = _date_text(_field(item, ["Date", "日期"]))
        close = _number(_field(item, ["Close", "收盤"]))
        if not trade_date or close is None:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "index_id": "TPEx",
                "index_name": "櫃買指數",
                "open": _number(_field(item, ["Open", "開盤"])),
                "high": _number(_field(item, ["High", "最高"])),
                "low": _number(_field(item, ["Low", "最低"])),
                "close": close,
                "source": TPEX_SOURCE,
                "is_official": True,
            }
        )
    return _normalize_output(pd.DataFrame(rows))


def update_market_indices_csv(
    output_path: str | Path,
    fetched: pd.DataFrame,
    *,
    merge_existing: bool = True,
) -> pd.DataFrame:
    """Merge fetched official index rows into ``data/market_indices.csv``."""

    path = Path(output_path)
    frames: list[pd.DataFrame] = []
    if merge_existing and path.exists():
        try:
            frames.append(pd.read_csv(path, encoding="utf-8-sig", dtype={"index_id": str}))
        except Exception:
            frames.append(pd.DataFrame(columns=MARKET_INDEX_COLUMNS))
    frames.append(fetched)

    merged = _normalize_output(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
    if not merged.empty:
        merged = merged.drop_duplicates(subset=["trade_date", "index_id"], keep="last")
        merged = merged.sort_values(["trade_date", "index_id"]).reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(path, index=False, encoding="utf-8-sig")
    return merged


def _requester(session: object | None) -> object:
    if session is None:
        requester = requests.Session()
        requester.headers.update(DEFAULT_HEADERS)
        return requester.get
    if hasattr(session, "get"):
        return session.get  # type: ignore[union-attr]
    return session


def _default_get(url: str, *, timeout: int) -> object:
    return requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)


def _get_json(requester: object, url: str, timeout_seconds: int) -> object:
    response = requester(url, timeout=timeout_seconds)  # type: ignore[operator]
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()
    if hasattr(response, "json"):
        return response.json()
    raise ValueError(f"official index response has no json(): {url}")


def _list_payload(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _normalize_output(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=MARKET_INDEX_COLUMNS)
    result = frame.copy()
    for column in MARKET_INDEX_COLUMNS:
        if column not in result.columns:
            result[column] = None
    result = result[MARKET_INDEX_COLUMNS].copy()
    result["trade_date"] = result["trade_date"].apply(_date_text)
    result["index_id"] = result["index_id"].astype(str).str.strip()
    result["index_name"] = result["index_name"].astype(str).str.strip()
    result["source"] = result["source"].astype(str).str.strip()
    for column in ["open", "high", "low", "close"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result["is_official"] = result["is_official"].apply(_truthy)
    result = result.dropna(subset=["close"])
    result = result[(result["trade_date"] != "") & (result["index_id"] != "")]
    return result[MARKET_INDEX_COLUMNS].reset_index(drop=True)


def _field(item: dict[str, object], keys: list[str]) -> object:
    normalized = {_normalize_key(key): value for key, value in item.items()}
    for key in keys:
        if key in item:
            return item[key]
        value = normalized.get(_normalize_key(key))
        if value is not None:
            return value
    return None


def _normalize_key(value: object) -> str:
    return str(value).strip().replace(" ", "").lower()


def _number(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if text in {"", "--", "-"}:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if pd.isna(number):
        return None
    return number


def _date_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 7:
        return f"{int(digits[:3]) + 1911:04d}-{int(digits[3:5]):02d}-{int(digits[5:7]):02d}"
    if len(digits) == 8:
        return f"{int(digits[:4]):04d}-{int(digits[4:6]):02d}-{int(digits[6:8]):02d}"
    parsed = pd.to_datetime(text, errors="coerce")
    return "" if pd.isna(parsed) else parsed.strftime("%Y-%m-%d")


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}
