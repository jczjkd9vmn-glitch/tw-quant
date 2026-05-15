"""Best-effort TWSE official data provider.

Provider outages or unexpected payloads are returned as warnings. They must not
stop the daily workflow.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd
import requests

from tw_quant.data_sources.base import ProviderResult, empty_result, failed_result
from tw_quant.data_sources.cache import read_cache, write_cache


INSTITUTIONAL_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "foreign_net_buy",
    "investment_trust_net_buy",
    "dealer_net_buy",
    "total_institutional_net_buy",
]

MARGIN_SHORT_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "margin_balance",
    "margin_change",
    "short_balance",
    "short_change",
    "securities_lending_sell_volume",
    "securities_lending_balance",
]

ATTENTION_DISPOSITION_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "is_attention_stock",
    "attention_reason",
    "is_disposition_stock",
    "disposition_start_date",
    "disposition_end_date",
    "disposition_reason",
]


class TWSEProvider:
    source_name = "twse"

    def __init__(
        self,
        requester: Callable[..., object] | None = None,
        timeout: int = 15,
        cache_dir: str | Path | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self.requester = requester or requests.get
        self.timeout = timeout
        self.cache_dir = Path(cache_dir) if cache_dir is not None else None
        self.cache_enabled = cache_enabled

    def fetch_institutional(self, as_of: str | date | None = None) -> ProviderResult:
        date_label = _date_label(as_of)
        cached = self._read_cached("institutional", date_label, INSTITUTIONAL_COLUMNS)
        if cached is not None:
            frame, warning = cached
            return ProviderResult("institutional", frame, "CACHE", warning=warning)
        try:
            payload = self._get_json(
                "https://www.twse.com.tw/rwd/zh/fund/T86",
                {"date": date_label, "selectType": "ALLBUT0999", "response": "json"},
            )
            table = normalize_table_payload(payload, ["證券代號", "證券名稱"])
            frame = normalize_institutional_table(table, date_label)
            return self._result_with_cache("institutional", date_label, frame, INSTITUTIONAL_COLUMNS)
        except Exception as exc:  # noqa: BLE001
            return failed_result("institutional", INSTITUTIONAL_COLUMNS, exc)

    def fetch_margin_short(self, as_of: str | date | None = None) -> ProviderResult:
        date_label = _date_label(as_of)
        cached = self._read_cached("margin_short", date_label, MARGIN_SHORT_COLUMNS)
        if cached is not None:
            frame, warning = cached
            return ProviderResult("margin_short", frame, "CACHE", warning=warning)
        try:
            payload = self._get_json(
                "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN",
                {"date": date_label, "selectType": "ALL", "response": "json"},
            )
            table = normalize_table_payload(payload, ["股票代號", "股票名稱"])
            frame = normalize_margin_short_table(table, date_label)
            return self._result_with_cache("margin_short", date_label, frame, MARGIN_SHORT_COLUMNS)
        except Exception as exc:  # noqa: BLE001
            return failed_result("margin_short", MARGIN_SHORT_COLUMNS, exc)

    def fetch_attention_disposition(self, as_of: str | date | None = None) -> ProviderResult:
        date_label = _date_label(as_of)
        cached = self._read_cached("attention_disposition", date_label, ATTENTION_DISPOSITION_COLUMNS)
        if cached is not None:
            frame, warning = cached
            return ProviderResult("attention_disposition", frame, "CACHE", warning=warning)
        return empty_result(
            "attention_disposition",
            ATTENTION_DISPOSITION_COLUMNS,
            "TWSE attention/disposition endpoint is not configured; fallback to local csv",
        )

    def _get_json(self, url: str, params: dict[str, str]) -> dict:
        response = self.requester(url, params=params, timeout=self.timeout)
        if hasattr(response, "raise_for_status"):
            response.raise_for_status()
        if hasattr(response, "json"):
            return response.json()
        raise ValueError("response object has no json()")

    def _read_cached(
        self,
        source_name: str,
        date_label: str,
        columns: list[str],
    ) -> tuple[pd.DataFrame, str] | None:
        if not self.cache_dir or not self.cache_enabled:
            return None
        frame, warning = read_cache(self.cache_dir, source_name, date_label, columns)
        if frame is None:
            return None
        return frame, warning

    def _result_with_cache(
        self,
        source_name: str,
        date_label: str,
        frame: pd.DataFrame,
        columns: list[str],
    ) -> ProviderResult:
        for column in columns:
            if column not in frame.columns:
                frame[column] = None
        frame = frame[columns].copy()
        if self.cache_dir and self.cache_enabled:
            write_cache(self.cache_dir, source_name, date_label, frame)
        status = "OK" if not frame.empty else "EMPTY"
        warning = "" if not frame.empty else "official source returned empty data"
        return ProviderResult(source_name, frame, status, warning=warning)


def normalize_table_payload(payload: dict, required_fields: list[str]) -> pd.DataFrame:
    candidates: list[tuple[list[str], list[list[object]]]] = []
    if isinstance(payload.get("fields"), list) and isinstance(payload.get("data"), list):
        candidates.append((payload["fields"], payload["data"]))
    for table in payload.get("tables", []) or []:
        if isinstance(table, dict) and isinstance(table.get("fields"), list) and isinstance(table.get("data"), list):
            candidates.append((table["fields"], table["data"]))
    for fields_key, data_key in [("fields9", "data9"), ("fields1", "data1")]:
        if isinstance(payload.get(fields_key), list) and isinstance(payload.get(data_key), list):
            candidates.append((payload[fields_key], payload[data_key]))

    for fields, rows in candidates:
        field_text = " ".join(str(field) for field in fields)
        if all(required in field_text for required in required_fields):
            return pd.DataFrame(rows, columns=[str(field).strip() for field in fields])
    raise ValueError("official payload has no matching table")


def normalize_institutional_table(frame: pd.DataFrame, date_label: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        stock_id = _first_value(row, ["證券代號", "股票代號"])
        if _is_blank(stock_id):
            continue
        foreign = _number(_first_value(row, ["外資及陸資買賣超股數", "外陸資買賣超股數", "外資買賣超股數"]))
        trust = _number(_first_value(row, ["投信買賣超股數"]))
        dealer = _number(_first_value(row, ["自營商買賣超股數"]))
        total = _number(_first_value(row, ["三大法人買賣超股數", "合計買賣超股數"]))
        if total is None:
            total = sum(value or 0.0 for value in [foreign, trust, dealer])
        rows.append(
            {
                "trade_date": _date_text(date_label),
                "stock_id": str(stock_id).strip(),
                "stock_name": str(_first_value(row, ["證券名稱", "股票名稱"]) or "").strip(),
                "foreign_net_buy": foreign,
                "investment_trust_net_buy": trust,
                "dealer_net_buy": dealer,
                "total_institutional_net_buy": total,
            }
        )
    return pd.DataFrame(rows, columns=INSTITUTIONAL_COLUMNS)


def normalize_margin_short_table(frame: pd.DataFrame, date_label: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        stock_id = _first_value(row, ["股票代號", "證券代號"])
        if _is_blank(stock_id):
            continue
        margin_balance = _number(_first_value(row, ["融資今日餘額", "融資餘額"]))
        margin_prev = _number(_first_value(row, ["融資昨日餘額"]))
        short_balance = _number(_first_value(row, ["融券今日餘額", "融券餘額"]))
        short_prev = _number(_first_value(row, ["融券昨日餘額"]))
        rows.append(
            {
                "trade_date": _date_text(date_label),
                "stock_id": str(stock_id).strip(),
                "stock_name": str(_first_value(row, ["股票名稱", "證券名稱"]) or "").strip(),
                "margin_balance": margin_balance,
                "margin_change": _number(_first_value(row, ["融資增減"])) or _diff(margin_balance, margin_prev),
                "short_balance": short_balance,
                "short_change": _number(_first_value(row, ["融券增減"])) or _diff(short_balance, short_prev),
                "securities_lending_sell_volume": _number(_first_value(row, ["借券賣出", "借券賣出股數"])),
                "securities_lending_balance": _number(_first_value(row, ["借券餘額", "借券賣出餘額"])),
            }
        )
    return pd.DataFrame(rows, columns=MARGIN_SHORT_COLUMNS)


def _first_value(row: pd.Series, contains: list[str]) -> object:
    for pattern in contains:
        for column in row.index:
            if pattern in str(column):
                return row[column]
    return None


def _number(value: object) -> float | None:
    if _is_blank(value):
        return None
    text = str(value).replace(",", "").replace("--", "").strip()
    try:
        number = float(text)
    except ValueError:
        return None
    if pd.isna(number):
        return None
    return number


def _diff(value: float | None, previous: float | None) -> float | None:
    if value is None or previous is None:
        return None
    return value - previous


def _date_label(value: str | date | None) -> str:
    if value is None:
        return pd.Timestamp.today(tz="Asia/Taipei").strftime("%Y%m%d")
    return pd.to_datetime(value).strftime("%Y%m%d")


def _date_text(value: object) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
