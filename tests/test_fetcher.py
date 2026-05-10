from __future__ import annotations

from datetime import date

import pytest
import requests

from tw_quant.data.exceptions import DataFetchError, DataQualityError
from tw_quant.data.fetcher import TWSEDailyFetcher, normalize_twse_payload


STOCK_FIELDS = [
    "證券代號",
    "證券名稱",
    "成交股數",
    "成交金額",
    "開盤價",
    "最高價",
    "最低價",
    "收盤價",
]

STOCK_ROW = ["2330", "台積電", "1,000", "600,000", "590.00", "605.00", "588.00", "600.00"]


def test_normalize_outer_fields_data_payload() -> None:
    frame = normalize_twse_payload(
        {"fields": STOCK_FIELDS, "data": [STOCK_ROW]},
        date(2026, 5, 8),
    )

    assert frame.loc[0, "symbol"] == "2330"
    assert frame.loc[0, "name"] == "台積電"
    assert frame.loc[0, "close"] == 600.0
    assert frame.loc[0, "volume"] == 1000.0


def test_normalize_tables_payload_selects_stock_table() -> None:
    payload = {
        "tables": [
            {
                "title": "價格指數",
                "fields": ["指數", "收盤指數", "漲跌(+/-)", "漲跌點數"],
                "data": [["發行量加權股價指數", "20000.00", "+", "100.00"]],
            },
            {
                "title": "每日收盤行情",
                "fields": STOCK_FIELDS,
                "data": [STOCK_ROW],
            },
        ]
    }

    frame = normalize_twse_payload(payload, date(2026, 5, 8))

    assert list(frame["symbol"]) == ["2330"]
    assert frame.loc[0, "market"] == "TSE"
    assert frame.loc[0, "source"] == "TWSE_MI_INDEX"


def test_tables_with_only_price_index_raise_data_quality_error(capsys: pytest.CaptureFixture[str]) -> None:
    payload = {
        "tables": [
            {
                "title": "價格指數",
                "fields": ["指數", "收盤指數", "漲跌(+/-)", "漲跌點數"],
                "data": [["發行量加權股價指數", "20000.00", "+", "100.00"]],
            }
        ]
    }

    with pytest.raises(DataQualityError, match="no stock daily closing table"):
        normalize_twse_payload(payload, date(2026, 5, 8))

    assert capsys.readouterr().out == ""

    with pytest.raises(DataQualityError, match="no stock daily closing table"):
        normalize_twse_payload(payload, date(2026, 5, 8), verbose=True)

    output = capsys.readouterr().out
    assert "價格指數" in output
    assert "收盤指數" in output


def test_empty_payload_raise_data_quality_error(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(DataQualityError, match="no stock daily closing table"):
        normalize_twse_payload({}, date(2026, 5, 8))

    assert capsys.readouterr().out == ""

    with pytest.raises(DataQualityError, match="no stock daily closing table"):
        normalize_twse_payload({}, date(2026, 5, 8), verbose=True)

    assert "no tables found" in capsys.readouterr().out


def test_normalize_fields9_data9_payload() -> None:
    frame = normalize_twse_payload(
        {"fields9": STOCK_FIELDS, "data9": [STOCK_ROW]},
        date(2026, 5, 8),
    )

    assert frame.loc[0, "symbol"] == "2330"
    assert frame.loc[0, "high"] == 605.0


def test_fetch_timeout_is_converted_to_data_fetch_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def timeout_get(*_args, **_kwargs):
        raise requests.exceptions.Timeout("read operation timed out")

    monkeypatch.setattr(requests, "get", timeout_get)
    fetcher = TWSEDailyFetcher(url="https://example.invalid/twse", timeout_seconds=1)

    with pytest.raises(DataFetchError) as exc_info:
        fetcher.fetch(date(2026, 5, 8))

    message = str(exc_info.value)
    assert "2026-05-08" in message
    assert "https://example.invalid/twse" in message
    assert "timed out" in message
