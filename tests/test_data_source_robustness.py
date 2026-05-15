from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import scripts.fetch_multi_factor_data as fetch_module
from tw_quant.data_sources.base import ProviderResult
from tw_quant.data_sources.mops_provider import MONTHLY_REVENUE_COLUMNS, MOPSProvider, normalize_monthly_revenue_html
from tw_quant.data_sources.twse_provider import (
    ATTENTION_DISPOSITION_COLUMNS,
    MARGIN_SHORT_COLUMNS,
    TWSEProvider,
    normalize_margin_short_table,
    normalize_table_payload,
)
from tw_quant.scoring.official_factors import score_attention_disposition_for_symbols


SECURITY_PAGE = """
<html><body>
THE PAGE CANNOT BE ACCESSED
FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED
</body></html>
"""


class FakeResponse:
    def __init__(self, text: str = "", payload: dict | None = None) -> None:
        self.text = text
        self._payload = payload or {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_mops_security_page_returns_failed_result() -> None:
    provider = MOPSProvider(requester=lambda *_args, **_kwargs: FakeResponse(text=SECURITY_PAGE), cache_enabled=False)

    result = provider.fetch_monthly_revenue("20260515")

    assert result.status == "FAILED"
    assert result.data.empty
    assert "security block" in result.warning
    assert "security block" in result.error_message


def test_mops_normal_html_table_parses() -> None:
    html = """
    <table>
      <thead>
        <tr>
          <th>公司代號</th><th>公司名稱</th><th>當月營收</th>
          <th>上月比較增減(%)</th><th>去年同月增減(%)</th>
          <th>當月累計營收</th><th>前期比較增減(%)</th>
        </tr>
      </thead>
      <tbody>
        <tr><td>2330</td><td>台積電</td><td>100,000</td><td>5.5</td><td>20.1</td><td>500,000</td><td>18.2</td></tr>
      </tbody>
    </table>
    """

    frame = normalize_monthly_revenue_html(html, "202605")

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["stock_id"] == "2330"
    assert row["revenue"] == 100000
    assert row["revenue_yoy"] == 20.1


def test_fetch_multi_factor_keeps_existing_monthly_revenue_on_mops_security_block(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    reports_dir = tmp_path / "reports"
    data_dir.mkdir()
    existing = pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "TSMC",
                "year_month": "202605",
                "revenue": 1,
                "revenue_yoy": 2,
                "revenue_mom": 3,
                "accumulated_revenue": 4,
                "accumulated_revenue_yoy": 5,
            }
        ]
    )
    path = data_dir / "monthly_revenue.csv"
    existing.to_csv(path, index=False)
    before = path.read_text(encoding="utf-8")

    class SecurityMOPSProvider:
        def __init__(self, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

        def fetch_monthly_revenue(self, trade_date: str) -> ProviderResult:
            return ProviderResult(
                "monthly_revenue",
                pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS),
                "FAILED",
                "MOPS security block detected; fallback to existing csv",
                SECURITY_PAGE,
            )

        def fetch_material_events(self, trade_date: str) -> ProviderResult:
            return ProviderResult("material_events", pd.DataFrame(), "EMPTY", "empty")

    class EmptyTWSEProvider:
        def __init__(self, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

        def fetch_institutional(self, trade_date: str) -> ProviderResult:
            return ProviderResult("institutional", pd.DataFrame(), "EMPTY", "empty")

        def fetch_margin_short(self, trade_date: str) -> ProviderResult:
            return ProviderResult("margin_short", pd.DataFrame(), "EMPTY", "empty")

        def fetch_attention_disposition(self, trade_date: str) -> ProviderResult:
            return ProviderResult("attention_disposition", pd.DataFrame(), "EMPTY", "empty")

    monkeypatch.setattr(fetch_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(fetch_module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(fetch_module, "MOPSProvider", SecurityMOPSProvider)
    monkeypatch.setattr(fetch_module, "TWSEProvider", EmptyTWSEProvider)

    status = fetch_module.run_fetch_multi_factor_data("20260515")

    row = status[status["source_name"] == "monthly_revenue"].iloc[0]
    assert path.read_text(encoding="utf-8") == before
    assert row["status"] == "OK"
    assert row["fallback_action"] == "kept_existing_csv"
    assert len(row["error_message"]) <= 300


def test_margin_short_fields_data_standard_format_parses() -> None:
    payload = {
        "fields": ["代號", "名稱", "融資今日餘額", "融資前日餘額", "融券今日餘額", "融券前日餘額"],
        "data": [["2330", "台積電", "100", "90", "20", "30"]],
    }

    table = normalize_table_payload(payload, [["代號"], ["名稱"], ["融資今日餘額"]])
    frame = normalize_margin_short_table(table, "20260515")

    assert frame.iloc[0]["margin_balance"] == 100
    assert frame.iloc[0]["margin_change"] == 10
    assert frame.iloc[0]["short_balance"] == 20
    assert frame.iloc[0]["short_change"] == -10


def test_margin_short_fields9_data9_parses() -> None:
    payload = {
        "fields9": ["股票代號", "股票名稱", "融資餘額", "融券餘額"],
        "data9": [["2330", "台積電", "100", "20"]],
    }

    table = normalize_table_payload(payload, [["股票代號"], ["股票名稱"], ["融資餘額"]])
    frame = normalize_margin_short_table(table, "20260515")

    assert frame.iloc[0]["stock_id"] == "2330"
    assert frame.iloc[0]["margin_balance"] == 100
    assert frame.iloc[0]["short_balance"] == 20


def test_margin_short_tables_duplicate_fields_parse_short_side() -> None:
    payload = {
        "tables": [
            {
                "title": "信用交易統計",
                "fields": ["項目", "買進", "賣出", "前日餘額", "今日餘額"],
                "data": [["融資", "1", "2", "3", "4"]],
            },
            {
                "title": "融資融券彙總",
                "fields": [
                    "代號",
                    "名稱",
                    "買進",
                    "賣出",
                    "前日餘額",
                    "今日餘額",
                    "買進",
                    "賣出",
                    "前日餘額",
                    "今日餘額",
                ],
                "data": [["2330", "台積電", "0", "0", "90", "100", "0", "0", "30", "20"]],
            },
        ]
    }

    table = normalize_table_payload(payload, [["代號"], ["名稱"], ["今日餘額"]])
    frame = normalize_margin_short_table(table, "20260515")

    assert frame.iloc[0]["margin_change"] == 10
    assert frame.iloc[0]["short_change"] == -10


def test_margin_short_semantic_aliases_and_missing_fields_do_not_crash() -> None:
    payload = {
        "fields": ["股票代號", "股票名稱", "融資餘額"],
        "data": [["2330", "台積電", "100"]],
    }

    table = normalize_table_payload(payload, [["股票代號"], ["股票名稱"], ["融資餘額"]])
    frame = normalize_margin_short_table(table, "20260515")

    assert frame.iloc[0]["margin_balance"] == 100
    assert pd.isna(frame.iloc[0]["short_balance"])


def test_margin_short_no_matching_table_returns_failed_with_debug_summary() -> None:
    payload = {"stat": "OK", "tables": [{"title": "bad", "fields": ["項目", "今日餘額"], "data": []}]}

    with pytest.raises(ValueError) as exc_info:
        normalize_table_payload(payload, [["代號"], ["名稱"], ["今日餘額"]])

    message = str(exc_info.value)
    assert "payload keys" in message
    assert "fields summary" in message

    provider = TWSEProvider(requester=lambda *_args, **_kwargs: FakeResponse(payload=payload), cache_enabled=False)
    result = provider.fetch_margin_short("20260515")
    assert result.status == "FAILED"
    assert "payload keys" in result.error_message


def test_attention_and_disposition_provider_parses_official_payloads() -> None:
    notice_payload = {
        "fields": ["編號", "證券代號", "證券名稱", "注意交易資訊", "日期"],
        "data": [["1", "2330", "台積電", "週轉率過高", "115/05/15"]],
    }
    punish_payload = {
        "fields": ["編號", "公布日期", "證券代號", "證券名稱", "處置條件", "處置起迄時間", "處置措施", "處置內容"],
        "data": [["1", "115/05/15", "1597", "直得", "連續三次", "115/05/16～115/05/29", "第一次處置", "處置內容"]],
    }

    def fake_request(url: str, **_kwargs) -> FakeResponse:
        return FakeResponse(payload=notice_payload if "notice" in url else punish_payload)

    result = TWSEProvider(requester=fake_request, cache_enabled=False).fetch_attention_disposition("20260515")

    assert result.status == "OK"
    attention = result.data[result.data["stock_id"] == "2330"].iloc[0]
    disposition = result.data[result.data["stock_id"] == "1597"].iloc[0]
    assert bool(attention["is_attention_stock"]) is True
    assert bool(disposition["is_disposition_stock"]) is True
    assert disposition["disposition_start_date"] == "2026-05-16"


def test_disposition_from_provider_blocks_when_config_enabled(tmp_path: Path) -> None:
    path = tmp_path / "attention_disposition.csv"
    frame = pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "stock_id": "1597",
                "stock_name": "直得",
                "is_attention_stock": False,
                "attention_reason": "",
                "is_disposition_stock": True,
                "disposition_start_date": "2026-05-16",
                "disposition_end_date": "2026-05-29",
                "disposition_reason": "第一次處置",
            }
        ],
        columns=ATTENTION_DISPOSITION_COLUMNS,
    )

    frame.to_csv(path, index=False, encoding="utf-8-sig")
    row = score_attention_disposition_for_symbols(["1597"], path, {"block_disposition_stock": True}).iloc[0]

    assert bool(row["event_blocked"]) is True
    assert row["event_risk_level"] == "HIGH"


def test_attention_provider_failure_does_not_overwrite_existing_csv(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    reports_dir = tmp_path / "reports"
    data_dir.mkdir()
    path = data_dir / "attention_disposition.csv"
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "stock_id": "1597",
                "stock_name": "直得",
                "is_attention_stock": False,
                "attention_reason": "",
                "is_disposition_stock": True,
                "disposition_start_date": "2026-05-16",
                "disposition_end_date": "2026-05-29",
                "disposition_reason": "第一次處置",
            }
        ]
    ).to_csv(path, index=False)
    before = path.read_text(encoding="utf-8")

    class EmptyMOPSProvider:
        def __init__(self, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

        def fetch_monthly_revenue(self, trade_date: str) -> ProviderResult:
            return ProviderResult("monthly_revenue", pd.DataFrame(), "EMPTY", "empty")

        def fetch_material_events(self, trade_date: str) -> ProviderResult:
            return ProviderResult("material_events", pd.DataFrame(), "EMPTY", "empty")

    class FailedTWSEProvider:
        def __init__(self, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

        def fetch_institutional(self, trade_date: str) -> ProviderResult:
            return ProviderResult("institutional", pd.DataFrame(), "EMPTY", "empty")

        def fetch_margin_short(self, trade_date: str) -> ProviderResult:
            return ProviderResult("margin_short", pd.DataFrame(), "EMPTY", "empty")

        def fetch_attention_disposition(self, trade_date: str) -> ProviderResult:
            return ProviderResult("attention_disposition", pd.DataFrame(), "FAILED", "provider failed", "boom")

    monkeypatch.setattr(fetch_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(fetch_module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(fetch_module, "MOPSProvider", EmptyMOPSProvider)
    monkeypatch.setattr(fetch_module, "TWSEProvider", FailedTWSEProvider)

    status = fetch_module.run_fetch_multi_factor_data("20260515")

    row = status[status["source_name"] == "attention_disposition"].iloc[0]
    assert path.read_text(encoding="utf-8") == before
    assert row["status"] == "OK"
    assert row["fallback_action"] == "kept_existing_csv"


def test_data_fetch_status_truncates_long_error_message(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    reports_dir = tmp_path / "reports"

    class LongErrorMOPSProvider:
        def __init__(self, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

        def fetch_monthly_revenue(self, trade_date: str) -> ProviderResult:
            return ProviderResult(
                "monthly_revenue",
                pd.DataFrame(columns=MONTHLY_REVENUE_COLUMNS),
                "FAILED",
                "MOPS security block detected; fallback to existing csv",
                "THE PAGE CANNOT BE ACCESSED " * 50,
            )

        def fetch_material_events(self, trade_date: str) -> ProviderResult:
            return ProviderResult("material_events", pd.DataFrame(), "EMPTY", "empty")

    class EmptyTWSEProvider:
        def __init__(self, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

        def fetch_institutional(self, trade_date: str) -> ProviderResult:
            return ProviderResult("institutional", pd.DataFrame(), "EMPTY", "empty")

        def fetch_margin_short(self, trade_date: str) -> ProviderResult:
            return ProviderResult("margin_short", pd.DataFrame(), "EMPTY", "empty")

        def fetch_attention_disposition(self, trade_date: str) -> ProviderResult:
            return ProviderResult("attention_disposition", pd.DataFrame(), "EMPTY", "empty")

    monkeypatch.setattr(fetch_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(fetch_module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(fetch_module, "MOPSProvider", LongErrorMOPSProvider)
    monkeypatch.setattr(fetch_module, "TWSEProvider", EmptyTWSEProvider)

    status = fetch_module.run_fetch_multi_factor_data("20260515")

    row = status[status["source_name"] == "monthly_revenue"].iloc[0]
    assert len(row["error_message"]) <= 300
