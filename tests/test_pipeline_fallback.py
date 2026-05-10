from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

import tw_quant.data.pipeline as pipeline_module
from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.data.exceptions import DataQualityError, TradingHalted
from tw_quant.data.pipeline import run_daily_pipeline


def test_run_daily_pipeline_falls_back_to_latest_sqlite_trade_date(tmp_path, monkeypatch) -> None:
    config_path, db_url = _config(tmp_path)
    engine = create_db_engine(db_url)
    init_db(engine)
    save_daily_prices(engine, _price_frame("20260508"))
    fetcher = FakeFetcher({date(2026, 5, 10): DataQualityError("no trading data")})
    monkeypatch.setattr(pipeline_module, "TWSEDailyFetcher", lambda **_kwargs: fetcher)

    result = run_daily_pipeline(
        config_path=config_path,
        trade_date="20260510",
        allow_fallback_latest=True,
    )

    assert result.trade_date == date(2026, 5, 8)
    assert result.fallback_date == date(2026, 5, 8)
    assert result.fallback_reason == "no trading data"
    assert result.fetched_rows == 0


def test_run_daily_pipeline_fails_when_fallback_has_no_sqlite_data(tmp_path, monkeypatch) -> None:
    config_path, _db_url = _config(tmp_path)
    fetcher = FakeFetcher({date(2026, 5, 10): DataQualityError("no trading data")})
    monkeypatch.setattr(pipeline_module, "TWSEDailyFetcher", lambda **_kwargs: fetcher)

    with pytest.raises(TradingHalted, match="no price history available for fallback"):
        run_daily_pipeline(
            config_path=config_path,
            trade_date="20260510",
            allow_fallback_latest=True,
        )


def test_run_daily_pipeline_runs_explicit_valid_trade_date_normally(tmp_path, monkeypatch) -> None:
    config_path, _db_url = _config(tmp_path)
    fetcher = FakeFetcher({date(2026, 5, 8): _price_frame("20260508")})
    monkeypatch.setattr(pipeline_module, "TWSEDailyFetcher", lambda **_kwargs: fetcher)

    result = run_daily_pipeline(
        config_path=config_path,
        trade_date="20260508",
        allow_fallback_latest=True,
    )

    assert result.trade_date == date(2026, 5, 8)
    assert result.fallback_date is None
    assert result.fallback_reason == ""
    assert result.fetched_rows == 1


class FakeFetcher:
    def __init__(self, payloads: dict[date, pd.DataFrame | Exception]):
        self.payloads = payloads

    def fetch(self, trade_date: date) -> pd.DataFrame:
        payload = self.payloads.get(trade_date)
        if isinstance(payload, Exception):
            raise payload
        if payload is None:
            raise DataQualityError("no trading data")
        return payload.copy()


def _config(tmp_path) -> tuple[str, str]:
    db_url = f"sqlite:///{(tmp_path / 'tw_quant.sqlite').as_posix()}"
    path = tmp_path / "config.yaml"
    path.write_text(
        "\n".join(
            [
                "database:",
                f"  url: {db_url}",
                "strategy:",
                "  minimum_total_score: 0",
                "  min_history_days: 1",
                "  max_candidates: 5",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return str(path), db_url


def _price_frame(trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "symbol": "2330",
                "name": "TSMC",
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 104.0,
                "volume": 2_000_000,
                "turnover": 208_000_000,
                "market": "TSE",
                "source": "TEST",
            }
        ]
    )
