"""Market data fetchers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd

from tw_quant.data.exceptions import DataFetchError, DataQualityError


REQUIRED_STOCK_FIELDS = {
    "證券代號",
    "證券名稱",
    "成交股數",
    "開盤價",
    "最高價",
    "最低價",
    "收盤價",
}

TWSE_COLUMNS = {
    "證券代號": "symbol",
    "證券名稱": "name",
    "成交股數": "volume",
    "成交金額": "turnover",
    "開盤價": "open",
    "最高價": "high",
    "最低價": "low",
    "收盤價": "close",
}


@dataclass(frozen=True)
class TWSEDailyFetcher:
    """Fetch TWSE daily closing data and normalize it into a DataFrame."""

    url: str = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
    timeout_seconds: int = 30
    verbose: bool = False

    def fetch(self, trade_date: date | datetime | str) -> pd.DataFrame:
        import requests
        import urllib3

        parsed_date = _to_date(trade_date)
        params = {
            "date": parsed_date.strftime("%Y%m%d"),
            "type": "ALLBUT0999",
            "response": "json",
        }
        try:
            response = requests.get(
                self.url,
                params=params,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload: dict[str, Any] = response.json()
        except requests.exceptions.Timeout as exc:
            raise DataFetchError(
                f"TWSE fetch timeout date={parsed_date} url={self.url} reason={exc}"
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            raise DataFetchError(
                f"TWSE connection error date={parsed_date} url={self.url} reason={exc}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise DataFetchError(
                f"TWSE request error date={parsed_date} url={self.url} reason={exc}"
            ) from exc
        except urllib3.exceptions.HTTPError as exc:
            raise DataFetchError(
                f"TWSE HTTP error date={parsed_date} url={self.url} reason={exc}"
            ) from exc
        except ValueError as exc:
            raise DataFetchError(
                f"TWSE JSON decode error date={parsed_date} url={self.url} reason={exc}"
            ) from exc
        return normalize_twse_payload(payload, parsed_date, verbose=self.verbose)


def normalize_twse_payload(
    payload: dict[str, Any],
    trade_date: date,
    verbose: bool = False,
) -> pd.DataFrame:
    fields, rows = _find_stock_table(payload, verbose=verbose)

    raw = pd.DataFrame(rows, columns=fields)
    missing = [column for column in REQUIRED_STOCK_FIELDS if column not in raw.columns]
    if missing:
        raise DataQualityError(f"TWSE payload missing columns: {', '.join(missing)}")

    for optional_column in TWSE_COLUMNS:
        if optional_column not in raw.columns:
            raw[optional_column] = None

    frame = raw.rename(columns=TWSE_COLUMNS)[list(TWSE_COLUMNS.values())].copy()
    frame["trade_date"] = pd.to_datetime(trade_date)
    frame["market"] = "TSE"
    frame["source"] = "TWSE_MI_INDEX"
    frame["symbol"] = frame["symbol"].astype(str).str.strip()
    frame["name"] = frame["name"].astype(str).str.strip()

    for column in ["open", "high", "low", "close", "volume", "turnover"]:
        frame[column] = frame[column].map(_parse_market_number)

    frame = frame.dropna(subset=["symbol", "open", "high", "low", "close", "volume"])
    frame = frame[frame["symbol"] != ""]
    if frame.empty:
        raise DataQualityError("TWSE normalized frame is empty")
    return frame[
        [
            "trade_date",
            "symbol",
            "name",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "market",
            "source",
        ]
    ]


def _find_stock_table(
    payload: dict[str, Any],
    verbose: bool = False,
) -> tuple[list[str], list[list[Any]]]:
    candidates: list[tuple[str, list[str], list[list[Any]]]] = []

    candidates.extend(
        [
            ("payload.fields/data", payload.get("fields") or [], payload.get("data") or []),
            ("payload.fields9/data9", payload.get("fields9") or [], payload.get("data9") or []),
        ]
    )

    tables = payload.get("tables") or []
    if isinstance(tables, list):
        for index, table in enumerate(tables):
            if not isinstance(table, dict):
                continue
            title = str(table.get("title") or f"tables[{index}]")
            candidates.append((title, table.get("fields") or [], table.get("data") or []))

    for _title, fields, rows in candidates:
        normalized_fields = [_normalize_field_name(field) for field in fields]
        if rows and REQUIRED_STOCK_FIELDS.issubset(set(normalized_fields)):
            return normalized_fields, rows

    if verbose:
        _print_table_debug(payload)
    raise DataQualityError("TWSE payload has no stock daily closing table")


def _print_table_debug(payload: dict[str, Any]) -> None:
    tables = payload.get("tables") or []
    if not tables:
        print("TWSE payload debug: no tables found")
        return

    print("TWSE payload debug: available tables")
    for index, table in enumerate(tables):
        if not isinstance(table, dict):
            print(f"- tables[{index}]: non-dict table")
            continue
        title = table.get("title") or f"tables[{index}]"
        fields = table.get("fields") or []
        print(f"- title={title}; fields={fields}")


def _normalize_field_name(value: Any) -> str:
    return str(value).strip()


def _parse_market_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "--", "---", "X0.00"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.to_datetime(value).date()
