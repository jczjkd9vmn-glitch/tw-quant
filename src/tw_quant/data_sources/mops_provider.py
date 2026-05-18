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
    "revenue_data_month",
    "requested_revenue_month",
    "latest_available_month",
    "revenue_source_status",
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

MONTHLY_REVENUE_OPENAPI_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
MATERIAL_EVENTS_OPENAPI_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap04_L"
FINANCIALS_OPENAPI_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci"

FINANCIAL_COLUMNS = [
    "stock_id",
    "stock_name",
    "financial_quarter",
    "eps",
    "eps_yoy",
    "roe",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "debt_ratio",
    "operating_cash_flow",
    "financial_source",
    "financial_source_status",
    "financial_period",
]

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
        requested_month = _year_month_text(as_of)
        cached = self._read_cached("monthly_revenue", date_label, MONTHLY_REVENUE_COLUMNS)
        if cached is not None:
            frame, warning = cached
            latest_month = _latest_period(frame, "year_month")
            return ProviderResult(
                "monthly_revenue",
                frame,
                "CACHE",
                warning=warning,
                requested_period=requested_month,
                actual_period=latest_month,
                latest_available_period=latest_month,
                source_url_or_name="reports/cache/monthly_revenue",
                is_real_data=True,
                data_age_days=_period_age_days(latest_month, date_label),
                coverage_ratio=1.0 if not frame.empty else 0.0,
                affected_symbols_count=len(frame),
            )
        try:
            return self._fetch_monthly_revenue_openapi(date_label, requested_month)
        except Exception:
            pass

        errors: list[str] = []
        for year_month in _month_candidates(as_of, lookback_months=6):
            try:
                parsed = pd.to_datetime(f"{year_month}01", format="%Y%m%d")
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
                        requested_period=requested_month,
                        source_url_or_name=url,
                    )
                frame = normalize_monthly_revenue_html(text, year_month)
                if frame.empty:
                    errors.append(f"{year_month}: empty")
                    continue
                frame["requested_revenue_month"] = requested_month
                frame["latest_available_month"] = year_month
                frame["revenue_data_month"] = year_month
                frame["revenue_source_status"] = "OK" if year_month == requested_month else "OK_WITH_FALLBACK"
                result = self._result_with_cache("monthly_revenue", date_label, frame, MONTHLY_REVENUE_COLUMNS)
                status = "OK" if year_month == requested_month else "OK_WITH_FALLBACK"
                warning = "" if status == "OK" else f"requested month {requested_month} unavailable; used latest available month {year_month}"
                return ProviderResult(
                    "monthly_revenue",
                    result.data,
                    status,
                    warning=warning,
                    requested_period=requested_month,
                    actual_period=year_month,
                    latest_available_period=year_month,
                    source_url_or_name=url,
                    is_real_data=True,
                    data_age_days=_period_age_days(year_month, date_label),
                    coverage_ratio=1.0 if not result.data.empty else 0.0,
                    affected_symbols_count=len(result.data),
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{year_month}: {type(exc).__name__}")
                continue
        return ProviderResult(
            "monthly_revenue",
            pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS),
            "FAILED",
            "monthly revenue source unavailable; fallback to existing csv",
            "; ".join(errors[-3:]),
            requested_period=requested_month,
            source_url_or_name="MOPS monthly revenue HTML",
        )

    def _fetch_monthly_revenue_openapi(self, date_label: str, requested_month: str) -> ProviderResult:
        response = self.requester(MONTHLY_REVENUE_OPENAPI_URL, timeout=self.timeout)
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
                requested_period=requested_month,
                source_url_or_name=MONTHLY_REVENUE_OPENAPI_URL,
            )
        payload = response.json() if hasattr(response, "json") else []
        frame = normalize_monthly_revenue_openapi(payload, requested_month)
        if frame.empty:
            return ProviderResult(
                "monthly_revenue",
                pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS),
                "EMPTY",
                "official monthly revenue source returned no data",
                requested_period=requested_month,
                source_url_or_name=MONTHLY_REVENUE_OPENAPI_URL,
                is_real_data=True,
            )
        actual_month = str(frame["year_month"].dropna().astype(str).max())
        status = "OK" if actual_month == requested_month else "OK_WITH_FALLBACK"
        warning = "" if status == "OK" else f"requested month {requested_month} unavailable; used latest available month {actual_month}"
        result = self._result_with_cache("monthly_revenue", date_label, frame, MONTHLY_REVENUE_COLUMNS)
        return ProviderResult(
            "monthly_revenue",
            result.data,
            status,
            warning=warning,
            requested_period=requested_month,
            actual_period=actual_month,
            latest_available_period=actual_month,
            source_url_or_name=MONTHLY_REVENUE_OPENAPI_URL,
            is_real_data=True,
            data_age_days=_period_age_days(actual_month, date_label),
            coverage_ratio=1.0 if not result.data.empty else 0.0,
            affected_symbols_count=len(result.data),
        )

    def fetch_financials(self, as_of: str | date | None = None) -> ProviderResult:
        date_label = _date_label(as_of)
        cached = self._read_cached("financials", date_label, FINANCIAL_COLUMNS)
        if cached is not None:
            frame, warning = cached
            period = _latest_period(frame, "financial_period")
            return ProviderResult(
                "financials",
                frame,
                "CACHE",
                warning=warning,
                actual_period=period,
                latest_available_period=period,
                source_url_or_name="reports/cache/financials",
                is_real_data=True,
                data_age_days=_period_age_days(period, date_label),
                coverage_ratio=1.0 if not frame.empty else 0.0,
                affected_symbols_count=len(frame),
            )
        try:
            response = self.requester(FINANCIALS_OPENAPI_URL, timeout=self.timeout)
            if hasattr(response, "raise_for_status"):
                response.raise_for_status()
            payload = response.json() if hasattr(response, "json") else []
            frame = normalize_financials_openapi(payload)
            result = self._result_with_cache("financials", date_label, frame, FINANCIAL_COLUMNS)
            period = _latest_period(result.data, "financial_period")
            return ProviderResult(
                "financials",
                result.data,
                result.status,
                warning=result.warning,
                actual_period=period,
                latest_available_period=period,
                source_url_or_name=FINANCIALS_OPENAPI_URL,
                is_real_data=True,
                data_age_days=_period_age_days(period, date_label),
                coverage_ratio=1.0 if not result.data.empty else 0.0,
                affected_symbols_count=len(result.data),
            )
        except Exception as exc:  # noqa: BLE001
            return failed_result("financials", FINANCIAL_COLUMNS, exc)

    def fetch_material_events(self, as_of: str | date | None = None) -> ProviderResult:
        date_label = _date_label(as_of)
        cached = self._read_cached("material_events", date_label, MATERIAL_EVENT_COLUMNS)
        if cached is not None:
            frame, warning = cached
            return ProviderResult(
                "material_events",
                frame,
                "CACHE",
                warning=warning,
                source_url_or_name="reports/cache/material_events",
                is_real_data=True,
                coverage_ratio=1.0 if not frame.empty else 0.0,
                affected_symbols_count=len(frame),
            )
        try:
            response = self.requester(MATERIAL_EVENTS_OPENAPI_URL, timeout=self.timeout)
            if hasattr(response, "raise_for_status"):
                response.raise_for_status()
            payload = response.json() if hasattr(response, "json") else []
            frame = normalize_material_events_openapi(payload, date_label)
            result = self._result_with_cache("material_events", date_label, frame, MATERIAL_EVENT_COLUMNS)
            return ProviderResult(
                "material_events",
                result.data,
                result.status,
                warning=result.warning,
                source_url_or_name=MATERIAL_EVENTS_OPENAPI_URL,
                is_real_data=True,
                coverage_ratio=1.0 if not result.data.empty else 0.0,
                affected_symbols_count=len(result.data),
            )
        except Exception as exc:  # noqa: BLE001
            return failed_result("material_events", MATERIAL_EVENT_COLUMNS, exc)

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


def normalize_monthly_revenue_openapi(payload: object, requested_month: str) -> pd.DataFrame:
    if not isinstance(payload, list):
        return pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS)
    rows: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        stock_id = _dict_value(item, ["公司代號", "stock_id", "Code"])
        year_month = _normalize_revenue_month(_dict_value(item, ["資料年月", "year_month"]))
        if _is_blank(stock_id) or _is_blank(year_month):
            continue
        rows.append(
            {
                "stock_id": str(stock_id).strip(),
                "stock_name": str(_dict_value(item, ["公司名稱", "stock_name", "Name"]) or "").strip(),
                "year_month": year_month,
                "revenue": _number(_dict_value(item, ["營業收入-當月營收", "revenue"])),
                "revenue_yoy": _number(_dict_value(item, ["營業收入-去年同月增減(%)", "revenue_yoy"])),
                "revenue_mom": _number(_dict_value(item, ["營業收入-上月比較增減(%)", "revenue_mom"])),
                "accumulated_revenue": _number(_dict_value(item, ["累計營業收入-當月累計營收", "accumulated_revenue"])),
                "accumulated_revenue_yoy": _number(_dict_value(item, ["累計營業收入-前期比較增減(%)", "accumulated_revenue_yoy"])),
                "revenue_data_month": year_month,
                "requested_revenue_month": requested_month,
                "latest_available_month": year_month,
                "revenue_source_status": "OK" if year_month == requested_month else "OK_WITH_FALLBACK",
            }
        )
    frame = pd.DataFrame(rows, columns=MONTHLY_REVENUE_COLUMNS)
    if frame.empty:
        return frame
    latest_month = str(frame["year_month"].dropna().astype(str).max())
    frame = frame[frame["year_month"].astype(str) == latest_month].copy()
    frame["latest_available_month"] = latest_month
    frame["revenue_source_status"] = "OK" if latest_month == requested_month else "OK_WITH_FALLBACK"
    return frame[MONTHLY_REVENUE_COLUMNS].reset_index(drop=True)


def normalize_financials_openapi(payload: object) -> pd.DataFrame:
    if not isinstance(payload, list):
        return pd.DataFrame(columns=FINANCIAL_COLUMNS)
    rows: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        stock_id = _dict_value(item, ["公司代號", "stock_id"])
        if _is_blank(stock_id):
            continue
        revenue = _number(_dict_value(item, ["營業收入"]))
        gross_profit = _number(_dict_value(item, ["營業毛利（毛損）", "營業毛利"]))
        operating_income = _number(_dict_value(item, ["營業利益（損失）", "營業利益"]))
        net_income = _number(_dict_value(item, ["本期淨利（淨損）", "本期淨利"]))
        year = str(_dict_value(item, ["年度"]) or "").strip()
        quarter = str(_dict_value(item, ["季別"]) or "").strip()
        period = f"{year}Q{quarter}" if year or quarter else ""
        rows.append(
            {
                "stock_id": str(stock_id).strip(),
                "stock_name": str(_dict_value(item, ["公司名稱", "stock_name"]) or "").strip(),
                "financial_quarter": period,
                "eps": _number(_dict_value(item, ["基本每股盈餘（元）", "基本每股盈餘", "eps"])),
                "eps_yoy": None,
                "roe": None,
                "gross_margin": _ratio(gross_profit, revenue),
                "operating_margin": _ratio(operating_income, revenue),
                "net_margin": _ratio(net_income, revenue),
                "debt_ratio": None,
                "operating_cash_flow": None,
                "financial_source": "TWSE OpenAPI t187ap06_L_ci",
                "financial_source_status": "OK",
                "financial_period": period,
            }
        )
    return pd.DataFrame(rows, columns=FINANCIAL_COLUMNS)


def normalize_material_events_openapi(payload: object, date_label: str) -> pd.DataFrame:
    if not isinstance(payload, list):
        return pd.DataFrame(columns=MATERIAL_EVENT_COLUMNS)
    rows: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        stock_id = _dict_value(item, ["公司代號", "stock_id"])
        if _is_blank(stock_id):
            continue
        title = str(_dict_value(item, ["主旨 ", "主旨", "title"]) or "").strip()
        summary = str(_dict_value(item, ["說明", "summary"]) or "").strip()
        text = f"{title} {summary}"
        sentiment, risk_level = _event_sentiment(text)
        event_date = _date_text(_dict_value(item, ["發言日期", "事實發生日", "出表日期"]) or date_label)
        rows.append(
            {
                "event_date": event_date,
                "stock_id": str(stock_id).strip(),
                "stock_name": str(_dict_value(item, ["公司名稱", "stock_name"]) or "").strip(),
                "title": title,
                "summary": summary,
                "event_type": "material_event",
                "event_sentiment": sentiment,
                "event_risk_level": risk_level,
            }
        )
    return pd.DataFrame(rows, columns=MATERIAL_EVENT_COLUMNS)


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


def _date_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _year_month_text(value: str | date | None) -> str:
    if value is None:
        return pd.Timestamp.today(tz="Asia/Taipei").strftime("%Y%m")
    return pd.to_datetime(value).strftime("%Y%m")


def _month_candidates(value: str | date | None, lookback_months: int) -> list[str]:
    start = pd.to_datetime(value or pd.Timestamp.today(tz="Asia/Taipei")).replace(day=1)
    return [(start - pd.DateOffset(months=offset)).strftime("%Y%m") for offset in range(max(1, lookback_months))]


def _latest_period(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return ""
    values = frame[column].dropna().astype(str)
    values = values[values.str.strip() != ""]
    return "" if values.empty else str(values.max())


def _period_age_days(period: str, date_label: str) -> int | None:
    if not period:
        return None
    digits = "".join(ch for ch in str(period) if ch.isdigit())
    try:
        if len(digits) >= 8:
            actual = pd.to_datetime(digits[:8], format="%Y%m%d")
        elif len(digits) >= 6:
            actual = pd.to_datetime(digits[:6] + "01", format="%Y%m%d")
        else:
            return None
        current = pd.to_datetime(date_label, format="%Y%m%d")
    except (TypeError, ValueError):
        return None
    return int(max(0, (current - actual).days))


def _normalize_revenue_month(value: object) -> str:
    if _is_blank(value):
        return ""
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 5:
        return f"{int(digits[:3]) + 1911}{int(digits[3:]):02d}"
    if len(digits) >= 6:
        year = int(digits[:4])
        month = int(digits[4:6])
        if year < 1911:
            year += 1911
        return f"{year:04d}{month:02d}"
    return text


def _dict_value(item: dict, keys: list[str]) -> object:
    normalized = {_normalize_text(str(key)): value for key, value in item.items()}
    for key in keys:
        direct = item.get(key)
        if direct is not None:
            return direct
        value = normalized.get(_normalize_text(key))
        if value is not None:
            return value
    return None


def _normalize_text(value: str) -> str:
    return "".join(str(value).split()).lower()


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return round(float(numerator) / float(denominator) * 100.0, 4)


def _event_sentiment(text: str) -> tuple[str, str]:
    negative = ["處分", "裁罰", "調查", "停工", "停業", "虧損", "下修", "警示", "注意股", "處置股", "違約", "訴訟"]
    positive = ["營收成長", "獲利成長", "接單", "擴產", "股利", "合作", "得標", "新產品", "法人買超"]
    if any(keyword in text for keyword in negative):
        return "NEGATIVE", "HIGH"
    if any(keyword in text for keyword in positive):
        return "POSITIVE", "LOW"
    return "NEUTRAL", "LOW"


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
