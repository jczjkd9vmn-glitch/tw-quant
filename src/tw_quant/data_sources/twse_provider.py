"""Best-effort TWSE official data provider.

Provider outages or unexpected payloads are returned as warnings. They must not
stop the daily workflow.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
import requests

from tw_quant.data_sources.base import ProviderResult, failed_result
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

TWSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.7,en;q=0.6",
    "Referer": "https://www.twse.com.tw/zh/",
    "Connection": "keep-alive",
}


class TWSEProvider:
    source_name = "twse"

    def __init__(
        self,
        requester: Callable[..., object] | object | None = None,
        timeout: int = 15,
        cache_dir: str | Path | None = None,
        cache_enabled: bool = True,
    ) -> None:
        if requester is None:
            self.session = requests.Session()
            self.session.headers.update(TWSE_HEADERS)
            self.requester = self.session.get
        elif hasattr(requester, "get"):
            self.session = requester
            self.requester = requester.get  # type: ignore[union-attr]
        else:
            self.session = None
            self.requester = requester  # type: ignore[assignment]
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
            if _is_no_data_payload(payload):
                return ProviderResult("institutional", pd.DataFrame(columns=INSTITUTIONAL_COLUMNS), "EMPTY", "official source returned no data")
            table = normalize_table_payload(payload, [["證券代號", "股票代號", "代號"], ["證券名稱", "股票名稱", "名稱"]])
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
            if _is_no_data_payload(payload):
                return ProviderResult("margin_short", pd.DataFrame(columns=MARGIN_SHORT_COLUMNS), "EMPTY", "official source returned no data")
            table = normalize_table_payload(
                payload,
                [
                    ["代號", "股票代號", "證券代號"],
                    ["名稱", "股票名稱", "證券名稱"],
                    ["今日餘額", "融資今日餘額", "融資餘額"],
                ],
            )
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
        try:
            params = {"startDate": date_label, "endDate": date_label, "response": "json", "sortKind": "STKNO"}
            notice_payload = self._get_json("https://www.twse.com.tw/rwd/zh/announcement/notice", params)
            punish_payload = self._get_json("https://www.twse.com.tw/rwd/zh/announcement/punish", params)
            frame = normalize_attention_disposition_payloads(notice_payload, punish_payload, date_label)
            return self._result_with_cache(
                "attention_disposition",
                date_label,
                frame,
                ATTENTION_DISPOSITION_COLUMNS,
            )
        except Exception as exc:  # noqa: BLE001
            return failed_result("attention_disposition", ATTENTION_DISPOSITION_COLUMNS, exc)

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
        if self.cache_dir and self.cache_enabled and not frame.empty:
            write_cache(self.cache_dir, source_name, date_label, frame)
        status = "OK" if not frame.empty else "EMPTY"
        warning = "" if not frame.empty else "official source returned empty data"
        return ProviderResult(source_name, frame, status, warning=warning)


RequiredField = str | list[str] | tuple[str, ...]


def normalize_table_payload(payload: dict, required_fields: list[RequiredField]) -> pd.DataFrame:
    candidates = list(_candidate_tables(payload))
    for fields, rows in candidates:
        unique_fields = _unique_fields(fields)
        if _matches_required_fields(unique_fields, required_fields):
            return pd.DataFrame(rows, columns=unique_fields)
    raise ValueError(
        "official payload has no matching table; "
        f"payload keys={list(payload.keys())}; "
        f"fields summary={_fields_summary(candidates)}; "
        f"required={_required_summary(required_fields)}"
    )


def _is_no_data_payload(payload: dict) -> bool:
    stat = str(payload.get("stat", ""))
    return bool(stat and "沒有符合條件" in stat)


def normalize_institutional_table(frame: pd.DataFrame, date_label: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        stock_id = _first_value(row, ["證券代號", "股票代號", "代號"])
        if _is_blank(stock_id):
            continue
        foreign = _number(_first_value(row, ["外陸資買賣超", "外資買賣超", "外資"]))
        trust = _number(_first_value(row, ["投信買賣超", "投信"]))
        dealer = _number(_first_value(row, ["自營商買賣超", "自營商"]))
        total = _number(_first_value(row, ["三大法人買賣超", "合計買賣超", "總計"]))
        if total is None:
            total = sum(value or 0.0 for value in [foreign, trust, dealer])
        rows.append(
            {
                "trade_date": _date_text(date_label),
                "stock_id": str(stock_id).strip(),
                "stock_name": str(_first_value(row, ["證券名稱", "股票名稱", "名稱"]) or "").strip(),
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
        stock_id = _first_value(row, ["股票代號", "證券代號", "代號"])
        if _is_blank(stock_id):
            continue
        margin_balance = _number(_first_value(row, ["融資今日餘額", "融資餘額", "今日餘額"]))
        margin_prev = _number(_first_value(row, ["融資前日餘額", "前日餘額"]))
        margin_change = _number(_first_value(row, ["融資增減", "融資變動", "融資差額"]))
        if margin_change is None:
            margin_change = _diff(margin_balance, margin_prev)

        short_balance = _number(_first_value(row, ["融券今日餘額", "融券餘額", "今日餘額_2"]))
        short_prev = _number(_first_value(row, ["融券前日餘額", "前日餘額_2"]))
        short_change = _number(_first_value(row, ["融券增減", "融券變動", "融券差額"]))
        if short_change is None:
            short_change = _diff(short_balance, short_prev)

        rows.append(
            {
                "trade_date": _date_text(date_label),
                "stock_id": str(stock_id).strip(),
                "stock_name": str(_first_value(row, ["股票名稱", "證券名稱", "名稱"]) or "").strip(),
                "margin_balance": margin_balance,
                "margin_change": margin_change,
                "short_balance": short_balance,
                "short_change": short_change,
                "securities_lending_sell_volume": _number(_first_value(row, ["借券賣出", "借券賣出股數", "借券賣出成交量"])),
                "securities_lending_balance": _number(_first_value(row, ["借券餘額", "借券賣出餘額", "借券餘額股數"])),
            }
        )
    return pd.DataFrame(rows, columns=MARGIN_SHORT_COLUMNS)


def normalize_attention_disposition_payloads(
    notice_payload: dict,
    punish_payload: dict,
    date_label: str,
) -> pd.DataFrame:
    records: dict[str, dict[str, object]] = {}
    notice_table = _payload_to_frame(notice_payload)
    if notice_table is not None:
        for _, row in notice_table.iterrows():
            stock_id = _first_value(row, ["證券代號", "股票代號", "代號"])
            if _is_blank(stock_id):
                continue
            key = str(stock_id).strip()
            record = records.setdefault(key, _empty_attention_record(key, date_label))
            record["stock_name"] = str(_first_value(row, ["證券名稱", "股票名稱", "名稱"]) or record["stock_name"]).strip()
            record["trade_date"] = _date_text(_first_value(row, ["日期", "公告日期"]) or date_label)
            record["is_attention_stock"] = True
            record["attention_reason"] = str(_first_value(row, ["注意交易資訊", "注意原因", "原因"]) or "").strip()

    punish_table = _payload_to_frame(punish_payload)
    if punish_table is not None:
        for _, row in punish_table.iterrows():
            stock_id = _first_value(row, ["證券代號", "股票代號", "代號"])
            if _is_blank(stock_id):
                continue
            key = str(stock_id).strip()
            record = records.setdefault(key, _empty_attention_record(key, date_label))
            record["stock_name"] = str(_first_value(row, ["證券名稱", "股票名稱", "名稱"]) or record["stock_name"]).strip()
            record["trade_date"] = _date_text(_first_value(row, ["公布日期", "日期"]) or date_label)
            record["is_disposition_stock"] = True
            period = str(_first_value(row, ["處置起迄時間", "處置期間"]) or "")
            start_date, end_date = _parse_period(period)
            record["disposition_start_date"] = start_date
            record["disposition_end_date"] = end_date
            reason_parts = [
                _first_value(row, ["處置條件", "條件"]),
                _first_value(row, ["處置措施", "措施"]),
                _first_value(row, ["處置內容", "內容"]),
            ]
            record["disposition_reason"] = "；".join(str(part).strip() for part in reason_parts if not _is_blank(part))

    return pd.DataFrame(records.values(), columns=ATTENTION_DISPOSITION_COLUMNS)


def _payload_to_frame(payload: dict) -> pd.DataFrame | None:
    try:
        return normalize_table_payload(payload, [["證券代號", "股票代號", "代號"], ["證券名稱", "股票名稱", "名稱"]])
    except ValueError:
        return None


def _candidate_tables(payload: dict) -> Iterable[tuple[list[object], list[list[object]]]]:
    if isinstance(payload.get("fields"), list) and isinstance(payload.get("data"), list):
        yield payload["fields"], payload["data"]
    for table in payload.get("tables", []) or []:
        if isinstance(table, dict) and isinstance(table.get("fields"), list) and isinstance(table.get("data"), list):
            yield table["fields"], table["data"]
    for key, fields in payload.items():
        if not (isinstance(key, str) and key.startswith("fields") and isinstance(fields, list)):
            continue
        suffix = key.removeprefix("fields")
        data_key = f"data{suffix}"
        rows = payload.get(data_key)
        if isinstance(rows, list):
            yield fields, rows


def _matches_required_fields(fields: list[str], required_fields: list[RequiredField]) -> bool:
    field_text = " ".join(fields)
    return all(_field_group_matches(field_text, field, fields) for field in required_fields)


def _field_group_matches(field_text: str, required: RequiredField, fields: list[str]) -> bool:
    aliases = [required] if isinstance(required, str) else list(required)
    normalized_text = _normalize_text(field_text)
    return any(_normalize_text(alias) in normalized_text for alias in aliases) or any(
        _normalize_text(alias) in _normalize_text(field) for alias in aliases for field in fields
    )


def _unique_fields(fields: list[object]) -> list[str]:
    counts: dict[str, int] = {}
    output: list[str] = []
    for field in fields:
        text = str(field).strip()
        counts[text] = counts.get(text, 0) + 1
        output.append(text if counts[text] == 1 else f"{text}_{counts[text]}")
    return output


def _fields_summary(candidates: list[tuple[list[object], list[list[object]]]]) -> str:
    summaries = []
    for index, (fields, rows) in enumerate(candidates[:8]):
        preview = "|".join(str(field).strip() for field in fields[:12])
        summaries.append(f"table{index}: rows={len(rows)} fields={preview}")
    return "; ".join(summaries)[:700]


def _required_summary(required_fields: list[RequiredField]) -> str:
    return "; ".join(",".join(item) if not isinstance(item, str) else item for item in required_fields)


def _empty_attention_record(stock_id: str, date_label: str) -> dict[str, object]:
    return {
        "trade_date": _date_text(date_label),
        "stock_id": stock_id,
        "stock_name": "",
        "is_attention_stock": False,
        "attention_reason": "",
        "is_disposition_stock": False,
        "disposition_start_date": "",
        "disposition_end_date": "",
        "disposition_reason": "",
    }


def _first_value(row: pd.Series, contains: list[str]) -> object:
    for pattern in contains:
        normalized_pattern = _normalize_text(pattern)
        for column in row.index:
            if normalized_pattern in _normalize_text(str(column)):
                value = row[column]
                if isinstance(value, pd.Series):
                    return value.iloc[0] if not value.empty else None
                return value
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


def _parse_period(value: str) -> tuple[str, str]:
    dates = re.findall(r"\d{2,4}[/-]\d{1,2}[/-]\d{1,2}", value)
    if len(dates) >= 2:
        return _date_text(dates[0]), _date_text(dates[1])
    if len(dates) == 1:
        parsed = _date_text(dates[0])
        return parsed, parsed
    return "", ""


def _date_label(value: str | date | None) -> str:
    if value is None:
        return pd.Timestamp.today(tz="Asia/Taipei").strftime("%Y%m%d")
    return pd.to_datetime(value).strftime("%Y%m%d")


def _date_text(value: object) -> str:
    if _is_blank(value):
        return ""
    text = str(value).strip()
    match = re.fullmatch(r"(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})", text)
    if not match:
        match = re.fullmatch(r"(\d{2,3})年(\d{1,2})月(\d{1,2})日?", text)
    if match and int(match.group(1)) < 1911:
        year = int(match.group(1)) + 1911
        return f"{year:04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    return pd.to_datetime(text).strftime("%Y-%m-%d")


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", value).lower()


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
