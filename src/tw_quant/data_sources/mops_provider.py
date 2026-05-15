"""Best-effort MOPS official provider.

The module deliberately returns warnings instead of raising when the public
source changes format, is blocked, or is unavailable.
"""

from __future__ import annotations

from datetime import date
from io import StringIO
from pathlib import Path
from typing import Callable

import pandas as pd
import requests

from tw_quant.data_sources.base import ProviderResult, empty_result, failed_result
from tw_quant.data_sources.cache import read_cache, write_cache


MONTHLY_REVENUE_COLUMNS = [
    "stock_id",
    "stock_name",
    "year_month",
    "revenue",
    "revenue_yoy",
    "revenue_mom",
    "accumulated_revenue",
    "accumulated_revenue_yoy",
]

MATERIAL_EVENT_COLUMNS = [
    "event_date",
    "stock_id",
    "stock_name",
    "title",
    "summary",
    "event_type",
    "event_sentiment",
    "event_risk_level",
]

MOPS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.7,en;q=0.6",
    "Referer": "https://mops.twse.com.tw/mops/web/index",
    "Connection": "keep-alive",
}

SECURITY_BLOCK_MARKERS = [
    "THE PAGE CANNOT BE ACCESSED",
    "FOR SECURITY REASONS",
    "頁面無法執行",
]


class MOPSProvider:
    source_name = "mops"

    def __init__(
        self,
        requester: Callable[..., object] | object | None = None,
        timeout: int = 15,
        cache_dir: str | Path | None = None,
        cache_enabled: bool = True,
    ) -> None:
        if requester is None:
            self.session = requests.Session()
            self.session.headers.update(MOPS_HEADERS)
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

    def fetch_monthly_revenue(self, as_of: str | date | None = None) -> ProviderResult:
        date_label = _date_label(as_of)
        cached = self._read_cached("monthly_revenue", date_label, MONTHLY_REVENUE_COLUMNS)
        if cached is not None:
            frame, warning = cached
            return ProviderResult("monthly_revenue", frame, "CACHE", warning=warning)
        try:
            parsed = pd.to_datetime(as_of or pd.Timestamp.today(tz="Asia/Taipei"))
            roc_year = parsed.year - 1911
            month = parsed.month
            url = f"https://mops.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{month}_0.html"
            response = self.requester(url, timeout=self.timeout)
            if hasattr(response, "raise_for_status"):
                response.raise_for_status()
            text = getattr(response, "text", "")
            if is_mops_security_block(text):
                return ProviderResult(
                    "monthly_revenue",
                    pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS),
                    "FAILED",
                    "MOPS security block detected; fallback to existing csv",
                    "security block: MOPS returned an access denied page",
                )
            frame = normalize_monthly_revenue_html(text, f"{parsed.year}{month:02d}")
            return self._result_with_cache("monthly_revenue", date_label, frame, MONTHLY_REVENUE_COLUMNS)
        except Exception as exc:  # noqa: BLE001
            return failed_result("monthly_revenue", MONTHLY_REVENUE_COLUMNS, exc)

    def fetch_material_events(self, as_of: str | date | None = None) -> ProviderResult:
        date_label = _date_label(as_of)
        cached = self._read_cached("material_events", date_label, MATERIAL_EVENT_COLUMNS)
        if cached is not None:
            frame, warning = cached
            return ProviderResult("material_events", frame, "CACHE", warning=warning)
        return empty_result(
            "material_events",
            MATERIAL_EVENT_COLUMNS,
            "MOPS material event endpoint is not configured; fallback to local csv",
        )

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


def normalize_monthly_revenue_html(html: str, year_month: str) -> pd.DataFrame:
    if not html.strip() or is_mops_security_block(html):
        return pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS)
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS)
    rows: list[dict[str, object]] = []
    for table in tables:
        table.columns = [_flatten_column(column) for column in table.columns]
        field_text = " ".join(table.columns)
        if not _has_any(field_text, ["公司代號", "股票代號", "證券代號"]) or not _has_any(
            field_text,
            ["當月營收", "本月營收", "營業收入"],
        ):
            continue
        for _, row in table.iterrows():
            stock_id = _first_value(row, ["公司代號", "股票代號", "證券代號"])
            if _is_blank(stock_id) or not str(stock_id).strip().isdigit():
                continue
            rows.append(
                {
                    "stock_id": str(stock_id).strip(),
                    "stock_name": str(_first_value(row, ["公司名稱", "公司簡稱", "股票名稱", "證券名稱"]) or "").strip(),
                    "year_month": year_month,
                    "revenue": _number(_first_value(row, ["當月營收", "本月營收", "營業收入"])),
                    "revenue_yoy": _number(_first_value(row, ["去年同月增減", "年增率", "YoY"])),
                    "revenue_mom": _number(_first_value(row, ["上月比較增減", "月增率", "MoM"])),
                    "accumulated_revenue": _number(_first_value(row, ["當月累計營收", "累計營收"])),
                    "accumulated_revenue_yoy": _number(
                        _first_value(row, ["前期比較增減", "累計營收年增", "累計增減"])
                    ),
                }
            )
    return pd.DataFrame(rows, columns=MONTHLY_REVENUE_COLUMNS)


def is_mops_security_block(html: str) -> bool:
    upper = html.upper()
    return any(marker.upper() in upper for marker in SECURITY_BLOCK_MARKERS)


def _flatten_column(column: object) -> str:
    if isinstance(column, tuple):
        return " ".join(str(part).strip() for part in column if str(part).strip() and not str(part).startswith("Unnamed"))
    return str(column).strip()


def _has_any(text: str, patterns: list[str]) -> bool:
    return any(pattern in text for pattern in patterns)


def _first_value(row: pd.Series, contains: list[str]) -> object:
    for pattern in contains:
        for column in row.index:
            if pattern in str(column):
                value = row[column]
                if isinstance(value, pd.Series):
                    return value.iloc[0] if not value.empty else None
                return value
    return None


def _number(value: object) -> float | None:
    if _is_blank(value):
        return None
    text = str(value).replace(",", "").replace("--", "").replace("%", "").strip()
    try:
        number = float(text)
    except ValueError:
        return None
    if pd.isna(number):
        return None
    return number


def _date_label(value: str | date | None) -> str:
    if value is None:
        return pd.Timestamp.today(tz="Asia/Taipei").strftime("%Y%m%d")
    return pd.to_datetime(value).strftime("%Y%m%d")


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
