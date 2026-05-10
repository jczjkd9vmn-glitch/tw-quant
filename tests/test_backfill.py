from __future__ import annotations

from datetime import date

import pandas as pd

from tw_quant.data.backfill import backfill_prices, resolve_backfill_dates
from tw_quant.data.database import (
    create_db_engine,
    init_db,
    load_candidate_scores,
    load_price_history,
    save_daily_prices,
)
from tw_quant.data.exceptions import DataFetchError, DataQualityError
from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.strategy.scoring import ScoringConfig


class FakeFetcher:
    def __init__(self, payloads: dict[date, pd.DataFrame | Exception]):
        self.payloads = payloads
        self.calls: list[date] = []

    def fetch(self, trade_date: date) -> pd.DataFrame:
        self.calls.append(trade_date)
        payload = self.payloads.get(trade_date)
        if isinstance(payload, Exception):
            raise payload
        if payload is None:
            raise DataQualityError("TWSE empty data")
        return payload.copy()


def test_normal_backfill_multiple_days() -> None:
    engine = create_db_engine("sqlite:///:memory:")
    payloads = {
        _date("20250101"): _price_frame("20250101", close=100),
        _date("20250102"): _price_frame("20250102", close=101),
        _date("20250103"): _price_frame("20250103", close=102),
    }

    result = backfill_prices(
        engine=engine,
        fetcher=FakeFetcher(payloads),
        risk_manager=_risk_manager(),
        scoring_config=ScoringConfig(min_history_days=40),
        trade_dates=resolve_backfill_dates(start="20250101", end="20250103"),
        sleep_seconds=0,
        retry_interval_seconds=0,
    )

    history = load_price_history(engine)
    assert result.summary.attempted_days == 3
    assert result.summary.success_days == 3
    assert result.summary.total_rows == 3
    assert len(history) == 3


def test_backfill_skips_empty_holiday_data() -> None:
    engine = create_db_engine("sqlite:///:memory:")
    fetcher = FakeFetcher(
        {
            _date("20250101"): _price_frame("20250101", close=100),
            _date("20250102"): DataQualityError("TWSE empty data"),
            _date("20250103"): _price_frame("20250103", close=102),
        }
    )

    result = backfill_prices(
        engine=engine,
        fetcher=fetcher,
        risk_manager=_risk_manager(),
        scoring_config=ScoringConfig(min_history_days=40),
        trade_dates=resolve_backfill_dates(start="20250101", end="20250103"),
        sleep_seconds=0,
        retry_interval_seconds=0,
    )

    assert result.summary.success_days == 2
    assert result.summary.skipped_days == 1
    skipped = [day for day in result.days if day.status == "skipped"][0]
    assert skipped.trade_date == _date("20250102")
    assert "TWSE empty data" in skipped.skipped_reason


def test_backfill_does_not_rewrite_existing_dates() -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    save_daily_prices(engine, _price_frame("20250101", close=100))
    fetcher = FakeFetcher(
        {
            _date("20250101"): _price_frame("20250101", close=999),
            _date("20250102"): _price_frame("20250102", close=101),
        }
    )

    result = backfill_prices(
        engine=engine,
        fetcher=fetcher,
        risk_manager=_risk_manager(),
        scoring_config=ScoringConfig(min_history_days=40),
        trade_dates=resolve_backfill_dates(start="20250101", end="20250102"),
        sleep_seconds=0,
        retry_interval_seconds=0,
    )

    history = load_price_history(engine)
    existing_day = history[history["trade_date"] == pd.Timestamp("2025-01-01")].iloc[0]
    assert result.days[0].status == "skipped"
    assert result.days[0].skipped_reason == "already exists in SQLite"
    assert len(fetcher.calls) == 1
    assert len(history) == 2
    assert existing_day["close"] == 100


def test_backfill_recalculates_scores_when_history_is_enough() -> None:
    engine = create_db_engine("sqlite:///:memory:")
    business_days = pd.bdate_range("2025-01-01", periods=45)
    payloads = {
        trade_date.date(): _price_frame(trade_date.date(), close=100 + index)
        for index, trade_date in enumerate(business_days)
    }

    result = backfill_prices(
        engine=engine,
        fetcher=FakeFetcher(payloads),
        risk_manager=_risk_manager(),
        scoring_config=ScoringConfig(minimum_total_score=60, min_history_days=40, max_candidates=5),
        trade_dates=resolve_backfill_dates(start=business_days[0].date(), end=business_days[-1].date()),
        sleep_seconds=0,
        retry_interval_seconds=0,
    )

    scores = load_candidate_scores(engine, trade_date=str(business_days[-1].date()))
    assert result.summary.success_days == 45
    assert result.summary.scoring_date == business_days[-1].date()
    assert result.summary.scored_rows > 0
    assert not scores.empty


def test_backfill_data_fetch_error_does_not_stop_batch() -> None:
    engine = create_db_engine("sqlite:///:memory:")
    fetcher = FakeFetcher(
        {
            _date("20250101"): DataFetchError("TWSE fetch timeout date=2025-01-01"),
            _date("20250102"): _price_frame("20250102", close=101),
        }
    )

    result = backfill_prices(
        engine=engine,
        fetcher=fetcher,
        risk_manager=_risk_manager(),
        scoring_config=ScoringConfig(min_history_days=40),
        trade_dates=resolve_backfill_dates(start="20250101", end="20250102"),
        retries=1,
        sleep_seconds=0,
        retry_interval_seconds=0,
    )

    assert result.summary.failed_days == 1
    assert result.summary.success_days == 1
    assert result.days[0].status == "failed"
    assert result.days[1].status == "success"


def test_backfill_retry_succeeds_after_fetch_error() -> None:
    engine = create_db_engine("sqlite:///:memory:")
    fetcher = SequenceFetcher(
        {
            _date("20250101"): [
                DataFetchError("TWSE fetch timeout date=2025-01-01"),
                _price_frame("20250101", close=100),
            ]
        }
    )

    result = backfill_prices(
        engine=engine,
        fetcher=fetcher,
        risk_manager=_risk_manager(),
        scoring_config=ScoringConfig(min_history_days=40),
        trade_dates=[_date("20250101")],
        retries=3,
        sleep_seconds=0,
        retry_interval_seconds=0,
    )

    assert result.summary.success_days == 1
    assert result.summary.failed_days == 0
    assert result.days[0].attempts == 2


def test_backfill_retry_failure_counts_failed_days() -> None:
    engine = create_db_engine("sqlite:///:memory:")
    fetcher = SequenceFetcher(
        {
            _date("20250101"): [
                DataFetchError("TWSE fetch timeout date=2025-01-01"),
                DataFetchError("TWSE fetch timeout date=2025-01-01"),
                DataFetchError("TWSE fetch timeout date=2025-01-01"),
            ]
        }
    )

    result = backfill_prices(
        engine=engine,
        fetcher=fetcher,
        risk_manager=_risk_manager(),
        scoring_config=ScoringConfig(min_history_days=40),
        trade_dates=[_date("20250101")],
        retries=3,
        sleep_seconds=0,
        retry_interval_seconds=0,
    )

    assert result.summary.success_days == 0
    assert result.summary.failed_days == 1
    assert result.days[0].status == "failed"
    assert result.days[0].error == "timeout after 3 retries"
    assert result.days[0].attempts == 3


def _risk_manager() -> RiskManager:
    return RiskManager(
        RiskConfig(min_liquidity_value=1_000, max_volatility_20=0.20, max_position_pct=0.20)
    )


def _price_frame(trade_date: str | date, close: float, symbol: str = "2330") -> pd.DataFrame:
    parsed_date = pd.to_datetime(trade_date)
    return pd.DataFrame(
        [
            {
                "trade_date": parsed_date,
                "symbol": symbol,
                "name": "台積電",
                "open": close * 0.99,
                "high": close * 1.02,
                "low": close * 0.98,
                "close": close,
                "volume": 2_000_000,
                "turnover": close * 2_000_000,
                "market": "TSE",
                "source": "TEST",
            }
        ]
    )


def _date(value: str) -> date:
    return pd.to_datetime(value).date()


class SequenceFetcher:
    def __init__(self, payloads: dict[date, list[pd.DataFrame | Exception]]):
        self.payloads = {key: list(value) for key, value in payloads.items()}
        self.calls: list[date] = []

    def fetch(self, trade_date: date) -> pd.DataFrame:
        self.calls.append(trade_date)
        payloads = self.payloads.get(trade_date, [])
        if not payloads:
            raise DataQualityError("TWSE empty data")
        payload = payloads.pop(0)
        if isinstance(payload, Exception):
            raise payload
        return payload.copy()
