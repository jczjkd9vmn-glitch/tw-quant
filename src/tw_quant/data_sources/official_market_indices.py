"""Official TWSE/TPEx market index ingestion helpers.

The output is intentionally kept as a CSV-friendly table. Benchmark logic reads
only this explicit official source instead of guessing index rows from stock
price tables.
"""

from __future__ import annotations

from pathlib import Path
from math import ceil
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
TWSE_TAIEX_MONTH_URL = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST"
TPEX_INDEX_MONTH_URL = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41_result.php"

TAIEX_SOURCE = "twse_openapi:MI_5MINS_HIST"
TPEX_SOURCE = "tpex_openapi:tpex_index"
TAIEX_HISTORY_SOURCE = "twse_official:MI_5MINS_HIST_monthly"
TPEX_HISTORY_SOURCE = "tpex_official:daily_trading_index_st41"
# A 252-trading-day return needs 253 closing points.
DEFAULT_HISTORY_DAYS = 253

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
    history_days: int = 5,
    as_of: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Fetch official TAIEX and TPEx index rows from public exchange APIs."""

    enabled_sources = {str(source).lower() for source in (sources or ["twse", "tpex"])}
    requester = _requester(session)
    frames: list[pd.DataFrame] = []
    errors: list[str] = []

    if "twse" in enabled_sources or "taiex" in enabled_sources:
        try:
            frames.append(
                fetch_twse_taiex(
                    timeout_seconds=timeout_seconds,
                    requester=requester,
                    history_days=history_days,
                    as_of=as_of,
                )
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"TAIEX: {type(exc).__name__}: {exc}")

    if "tpex" in enabled_sources:
        try:
            frames.append(
                fetch_tpex_index(
                    timeout_seconds=timeout_seconds,
                    requester=requester,
                    history_days=history_days,
                    as_of=as_of,
                )
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"TPEx: {type(exc).__name__}: {exc}")

    combined = _normalize_output(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
    if combined.empty and errors:
        raise OfficialMarketIndexError("; ".join(errors))
    return combined


def fetch_twse_taiex(
    *,
    timeout_seconds: int = 15,
    requester: object | None = None,
    history_days: int = 5,
    as_of: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Fetch TAIEX OHLC rows from TWSE OpenAPI."""

    if history_days > 5:
        historical = _fetch_twse_taiex_history(
            timeout_seconds=timeout_seconds,
            requester=requester,
            history_days=history_days,
            as_of=as_of,
        )
        if not historical.empty:
            return historical

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


def fetch_tpex_index(
    *,
    timeout_seconds: int = 15,
    requester: object | None = None,
    history_days: int = 5,
    as_of: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Fetch TPEx index OHLC rows from TPEx OpenAPI."""

    frames: list[pd.DataFrame] = []
    if history_days > 5:
        historical = _fetch_tpex_index_history(
            timeout_seconds=timeout_seconds,
            requester=requester,
            history_days=history_days,
            as_of=as_of,
        )
        if not historical.empty:
            frames.append(historical)

    try:
        payload = _get_json(requester or _default_get, TPEX_INDEX_URL, timeout_seconds)
    except Exception:
        if frames:
            return frames[0]
        raise
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
    recent = _normalize_output(pd.DataFrame(rows))
    if not frames:
        return recent
    frames.append(recent)
    combined = _normalize_output(pd.concat(frames, ignore_index=True))
    if not combined.empty:
        combined = combined.drop_duplicates(subset=["trade_date", "index_id"], keep="last")
        combined = _tail_history(combined, history_days)
    return combined


def _fetch_twse_taiex_history(
    *,
    timeout_seconds: int,
    requester: object | None,
    history_days: int,
    as_of: str | pd.Timestamp | None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for month in _history_months(history_days, as_of):
        url = f"{TWSE_TAIEX_MONTH_URL}?date={month.strftime('%Y%m01')}&response=json"
        payload = _get_json(requester or _default_get, url, timeout_seconds)
        rows.extend(_twse_month_rows(payload))
    return _tail_history(_normalize_output(pd.DataFrame(rows)), history_days)


def _fetch_tpex_index_history(
    *,
    timeout_seconds: int,
    requester: object | None,
    history_days: int,
    as_of: str | pd.Timestamp | None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for month in _history_months(history_days, as_of):
        roc_month = f"{month.year - 1911:03d}/{month.month:02d}"
        url = f"{TPEX_INDEX_MONTH_URL}?l=zh-tw&d={roc_month}&o=json"
        payload = _get_json(requester or _default_get, url, timeout_seconds)
        rows.extend(_tpex_month_rows(payload))
    return _tail_history(_normalize_output(pd.DataFrame(rows)), history_days)


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


def _twse_month_rows(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, list):
        rows = []
        for item in _list_payload(payload):
            trade_date = _date_text(_field(item, ["Date", "日期"]))
            close = _number(_field(item, ["ClosingIndex", "收盤指數"]))
            if trade_date and close is not None:
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
        return rows
    if not isinstance(payload, dict):
        return []
    fields = [str(field).strip() for field in payload.get("fields", [])] if isinstance(payload.get("fields"), list) else []
    data = payload.get("data")
    if not isinstance(data, list):
        return []
    rows = []
    for raw in data:
        if not isinstance(raw, list):
            continue
        item = {fields[index]: value for index, value in enumerate(raw) if index < len(fields)}
        trade_date = _date_text(item.get("日期"))
        close = _number(item.get("收盤指數"))
        if not trade_date or close is None:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": _number(item.get("開盤指數")),
                "high": _number(item.get("最高指數")),
                "low": _number(item.get("最低指數")),
                "close": close,
                "source": TAIEX_HISTORY_SOURCE,
                "is_official": True,
            }
        )
    return rows


def _tpex_month_rows(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, list):
        return _tpex_openapi_rows(payload)
    if not isinstance(payload, dict):
        return []
    table_rows: list[object] = []
    tables = payload.get("tables")
    if isinstance(tables, list):
        for table in tables:
            if not isinstance(table, dict):
                continue
            if str(table.get("title", "")).strip() != "日成交量值指數":
                continue
            data = table.get("data")
            if isinstance(data, list):
                table_rows.extend(data)
    if not table_rows and isinstance(payload.get("aaData"), list):
        table_rows = list(payload["aaData"])  # type: ignore[index]

    rows = []
    for raw in table_rows:
        if not isinstance(raw, list) or len(raw) < 5:
            continue
        trade_date = _date_text(raw[0])
        close = _number(raw[4])
        if not trade_date or close is None:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "index_id": "TPEx",
                "index_name": "櫃買指數",
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "source": TPEX_HISTORY_SOURCE,
                "is_official": True,
            }
        )
    return rows


def _tpex_openapi_rows(payload: object) -> list[dict[str, object]]:
    rows = []
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
    return rows


def _history_months(history_days: int, as_of: str | pd.Timestamp | None) -> list[pd.Timestamp]:
    target = pd.to_datetime(as_of, errors="coerce") if as_of is not None else pd.Timestamp.today()
    if pd.isna(target):
        target = pd.Timestamp.today()
    month = pd.Timestamp(year=int(target.year), month=int(target.month), day=1)
    month_count = max(1, ceil(max(history_days, 1) / 18) + 3)
    return [month - pd.DateOffset(months=offset) for offset in range(month_count - 1, -1, -1)]


def _tail_history(frame: pd.DataFrame, history_days: int) -> pd.DataFrame:
    if frame.empty:
        return _normalize_output(frame)
    result = _normalize_output(frame)
    result["trade_date"] = result["trade_date"].apply(_date_text)
    result = result.sort_values(["index_id", "trade_date"]).drop_duplicates(["trade_date", "index_id"], keep="last")
    trimmed = []
    for _, group in result.groupby("index_id", dropna=False):
        trimmed.append(group.tail(max(int(history_days), 1)))
    return pd.concat(trimmed, ignore_index=True).sort_values(["trade_date", "index_id"]).reset_index(drop=True)


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
