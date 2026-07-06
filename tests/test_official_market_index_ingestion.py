from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report
from tw_quant.data_sources.official_market_indices import (
    fetch_official_market_indices,
    update_market_indices_csv,
)
from tw_quant.reporting.benchmark import select_benchmark_snapshot
from tw_quant.reporting.benchmark import benchmark_return_for_window


def test_fetch_official_market_indices_normalizes_twse_and_tpex_payloads() -> None:
    session = _FakeSession(
        {
            "MI_5MINS_HIST": [
                {
                    "Date": "1150601",
                    "OpeningIndex": "44872.82",
                    "HighestIndex": "45931.10",
                    "LowestIndex": "44872.82",
                    "ClosingIndex": "45337.91",
                }
            ],
            "tpex_index": [
                {
                    "Date": "20260601",
                    "Open": "443.97",
                    "High": "451.09",
                    "Low": "443.97",
                    "Close": "446.02",
                }
            ],
        }
    )

    frame = fetch_official_market_indices(session=session)

    assert list(frame.columns) == [
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
    assert set(frame["index_id"]) == {"TAIEX", "TPEx"}
    taiex = frame[frame["index_id"] == "TAIEX"].iloc[0]
    tpex = frame[frame["index_id"] == "TPEx"].iloc[0]
    assert taiex["trade_date"] == "2026-06-01"
    assert taiex["close"] == 45337.91
    assert taiex["source"] == "twse_openapi:MI_5MINS_HIST"
    assert bool(taiex["is_official"]) is True
    assert tpex["trade_date"] == "2026-06-01"
    assert tpex["close"] == 446.02
    assert tpex["source"] == "tpex_openapi:tpex_index"


def test_update_market_indices_csv_merges_and_deduplicates(tmp_path: Path) -> None:
    output_path = tmp_path / "market_indices.csv"
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-01",
                "index_id": "TAIEX",
                "index_name": "old",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "source": "old",
                "is_official": True,
            }
        ]
    ).to_csv(output_path, index=False, encoding="utf-8")

    fetched = pd.DataFrame(
        [
            {
                "trade_date": "2026-06-01",
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "source": "twse_openapi:MI_5MINS_HIST",
                "is_official": True,
            }
        ]
    )

    merged = update_market_indices_csv(output_path, fetched)

    assert len(merged) == 1
    assert merged.iloc[0]["index_name"] == "發行量加權股價指數"
    assert pd.read_csv(output_path, dtype={"index_id": str}).iloc[0]["source"] == "twse_openapi:MI_5MINS_HIST"


def test_benchmark_uses_official_taiex_before_0050_fallback(tmp_path: Path) -> None:
    _write_official_taiex(tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.99,
                "stock_return_20d": 0.99,
            }
        ]
    ).to_csv(tmp_path / "sector_strength.csv", index=False, encoding="utf-8")

    snapshot = select_benchmark_snapshot(tmp_path, "2026-06-05")

    assert snapshot["source_label"] == "正式加權指數"
    assert snapshot["benchmark_is_official"] is True
    assert snapshot["fallback_reason"] == ""
    assert snapshot["can_judge_alpha"] is True
    assert snapshot["benchmark_history_days"] == 6
    assert snapshot["can_judge_alpha_5d"] is True
    assert snapshot["can_judge_alpha_20d"] is False
    assert snapshot["can_judge_alpha_60d"] is False
    assert snapshot["returns"]["5d"] == 0.03
    assert snapshot["returns"]["20d"] is None


def test_benchmark_prefers_total_return_index_when_available(tmp_path: Path) -> None:
    rows = []
    for offset, trade_date in enumerate(pd.bdate_range(end="2026-06-05", periods=6)):
        rows.append(
            {
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 100 + offset,
                "high": 100 + offset,
                "low": 100 + offset,
                "close": 100 + offset,
                "source": "twse_openapi:MI_5MINS_HIST",
                "is_official": True,
            }
        )
        rows.append(
            {
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "index_id": "TAIEX_TR",
                "index_name": "發行量加權報酬指數",
                "open": 200 + offset,
                "high": 200 + offset,
                "low": 200 + offset,
                "close": 200 + offset,
                "source": "twse_official:TAIEX_TR",
                "is_official": True,
            }
        )
    pd.DataFrame(rows).to_csv(tmp_path / "market_indices.csv", index=False, encoding="utf-8")

    snapshot = select_benchmark_snapshot(tmp_path, "2026-06-05")

    assert snapshot["source_label"] == "正式加權報酬指數"
    assert snapshot["benchmark_is_official"] is True
    assert snapshot["warning"] == "official benchmark history_days=6，20d,60d,120d,252d alpha=DATA_INSUFFICIENT"
    assert snapshot["returns"]["5d"] == 0.025


def test_benchmark_window_requires_sufficient_official_history(tmp_path: Path) -> None:
    _write_official_taiex(tmp_path)

    insufficient = benchmark_return_for_window(tmp_path, "2026-06-05", 20)

    assert insufficient["benchmark_is_official"] is True
    assert insufficient["can_judge_alpha"] is False
    assert insufficient["return"] is None
    assert insufficient["status"] == "DATA_INSUFFICIENT"
    assert "DATA_INSUFFICIENT" in str(insufficient["warning"])


def test_benchmark_window_uses_official_history_when_available(tmp_path: Path) -> None:
    _write_official_taiex_history(tmp_path, periods=61)

    snapshot = select_benchmark_snapshot(tmp_path, "2026-06-05")
    result_20d = benchmark_return_for_window(tmp_path, "2026-06-05", 20)
    result_60d = benchmark_return_for_window(tmp_path, "2026-06-05", 60)

    assert snapshot["benchmark_history_days"] == 61
    assert snapshot["can_judge_alpha_20d"] is True
    assert snapshot["can_judge_alpha_60d"] is True
    assert snapshot["can_judge_alpha_120d"] is False
    assert result_20d["can_judge_alpha"] is True
    assert result_20d["return"] == snapshot["returns"]["20d"]
    assert result_60d["can_judge_alpha"] is True
    assert result_60d["return"] == snapshot["returns"]["60d"]


def test_benchmark_falls_back_to_0050_when_official_taiex_missing(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.01,
                "stock_return_20d": 0.02,
            }
        ]
    ).to_csv(tmp_path / "sector_strength.csv", index=False, encoding="utf-8")

    snapshot = select_benchmark_snapshot(tmp_path, "2026-06-05")

    assert snapshot["source_label"] == "0050 fallback"
    assert snapshot["benchmark_is_official"] is False
    assert snapshot["fallback_reason"] == "missing_official_market_index"
    assert snapshot["can_judge_alpha"] is False
    assert snapshot["benchmark_history_days"] == 0
    assert snapshot["can_judge_alpha_20d"] is False


def test_html_displays_official_benchmark_metadata(tmp_path: Path) -> None:
    _write_minimal_html_inputs(tmp_path)
    _write_official_taiex(tmp_path)

    output_path = generate_html_report(tmp_path, docs_dir=tmp_path / "docs")
    html = output_path.read_text(encoding="utf-8")

    assert "benchmark_source=正式加權指數" in html
    assert "benchmark_is_official=true" in html
    assert "fallback_reason=" in html
    assert "can_judge_alpha=true" in html
    assert "benchmark_history_days=6" in html
    assert "can_judge_alpha_20d=false" in html
    assert "DATA_INSUFFICIENT" in html
    assert "benchmark_source=0050 fallback" not in html


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self.payload


class _FakeSession:
    def __init__(self, payloads: dict[str, object]) -> None:
        self.payloads = payloads

    def get(self, url: str, *, timeout: int) -> _FakeResponse:
        assert timeout == 15
        for key, payload in self.payloads.items():
            if key in url:
                return _FakeResponse(payload)
        raise AssertionError(f"unexpected url: {url}")


def _write_official_taiex(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-29",
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 100,
                "high": 100,
                "low": 100,
                "close": 100.0,
                "source": "twse_openapi:MI_5MINS_HIST",
                "is_official": True,
            },
            {
                "trade_date": "2026-06-01",
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 100,
                "high": 101,
                "low": 100,
                "close": 100.5,
                "source": "twse_openapi:MI_5MINS_HIST",
                "is_official": True,
            },
            {
                "trade_date": "2026-06-02",
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 100,
                "high": 101,
                "low": 100,
                "close": 101.0,
                "source": "twse_openapi:MI_5MINS_HIST",
                "is_official": True,
            },
            {
                "trade_date": "2026-06-03",
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 101,
                "high": 102,
                "low": 101,
                "close": 101.5,
                "source": "twse_openapi:MI_5MINS_HIST",
                "is_official": True,
            },
            {
                "trade_date": "2026-06-04",
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 101,
                "high": 103,
                "low": 101,
                "close": 102.0,
                "source": "twse_openapi:MI_5MINS_HIST",
                "is_official": True,
            },
            {
                "trade_date": "2026-06-05",
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 102,
                "high": 104,
                "low": 102,
                "close": 103.0,
                "source": "twse_openapi:MI_5MINS_HIST",
                "is_official": True,
            },
        ]
    ).to_csv(path / "market_indices.csv", index=False, encoding="utf-8")


def _write_official_taiex_history(path: Path, *, periods: int) -> None:
    dates = pd.bdate_range(end="2026-06-05", periods=periods)
    rows = []
    for offset, trade_date in enumerate(dates):
        close = 100.0 + (offset * 0.1)
        rows.append(
            {
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "source": "twse_official:MI_5MINS_HIST_monthly",
                "is_official": True,
            }
        )
    pd.DataFrame(rows).to_csv(path / "market_indices.csv", index=False, encoding="utf-8")


def _write_minimal_html_inputs(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "requested_date": "2026-06-05",
                "trade_date": "2026-06-05",
                "status": "OK",
                "total_capital": 1_000_000.0,
                "total_equity_after_cost": 1_020_000.0,
                "total_equity": 1_020_000.0,
                "market_regime_score": 65.0,
            }
        ]
    ).to_csv(path / "daily_summary_20260605.csv", index=False, encoding="utf-8")
