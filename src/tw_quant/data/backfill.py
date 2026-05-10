"""Historical price backfill workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import time
from typing import Iterable, Protocol

import pandas as pd
from sqlalchemy import Engine

from tw_quant.config import load_config
from tw_quant.data.database import (
    create_db_engine,
    init_db,
    load_existing_price_dates,
    load_price_history,
    save_candidate_scores,
    save_daily_prices,
)
from tw_quant.data.exceptions import DataFetchError, DataQualityError
from tw_quant.data.fetcher import TWSEDailyFetcher
from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.strategy.scoring import ScoringConfig, StockScorer


class DailyFetcher(Protocol):
    def fetch(self, trade_date: date) -> pd.DataFrame:
        ...


@dataclass(frozen=True)
class BackfillDayResult:
    trade_date: date
    status: str
    fetched_rows: int = 0
    saved_rows: int = 0
    skipped_reason: str = ""
    error: str = ""
    attempts: int = 0


@dataclass(frozen=True)
class BackfillSummary:
    attempted_days: int
    success_days: int
    skipped_days: int
    failed_days: int
    total_rows: int
    scored_rows: int = 0
    candidate_rows: int = 0
    scoring_date: date | None = None
    warning: str = ""


@dataclass(frozen=True)
class BackfillResult:
    days: list[BackfillDayResult]
    summary: BackfillSummary


def run_backfill(
    config_path: str | Path = "config.yaml",
    start: str | date | None = None,
    end: str | date | None = None,
    days: int | None = None,
    fetcher: DailyFetcher | None = None,
    retries: int = 3,
    sleep_seconds: float = 0.5,
    timeout_seconds: int = 30,
    verbose: bool = False,
) -> BackfillResult:
    config = load_config(config_path)
    engine = create_db_engine(config["database"]["url"])
    init_db(engine)
    risk_manager = RiskManager(RiskConfig.from_mapping(config["risk"]))
    scoring_config = ScoringConfig.from_mapping(config["strategy"])
    daily_fetcher = fetcher or TWSEDailyFetcher(
        url=config["data"]["twse_url"],
        timeout_seconds=timeout_seconds,
        verbose=verbose,
    )

    backfill_dates = resolve_backfill_dates(start=start, end=end, days=days)
    return backfill_prices(
        engine=engine,
        fetcher=daily_fetcher,
        risk_manager=risk_manager,
        scoring_config=scoring_config,
        trade_dates=backfill_dates,
        retries=retries,
        sleep_seconds=sleep_seconds,
    )


def resolve_backfill_dates(
    start: str | date | None = None,
    end: str | date | None = None,
    days: int | None = None,
) -> list[date]:
    if days is not None:
        if days <= 0:
            raise ValueError("--days must be positive")
        if start is not None:
            raise ValueError("--start cannot be combined with --days")
        end_date = _to_date(end) if end is not None else pd.Timestamp.today().date()
        start_date = end_date - timedelta(days=days - 1)
    else:
        if start is None or end is None:
            raise ValueError("--start and --end are required when --days is not used")
        start_date = _to_date(start)
        end_date = _to_date(end)

    if start_date > end_date:
        raise ValueError("start date must be on or before end date")
    return [start_date + timedelta(days=offset) for offset in range((end_date - start_date).days + 1)]


def backfill_prices(
    engine: Engine,
    fetcher: DailyFetcher,
    risk_manager: RiskManager,
    scoring_config: ScoringConfig,
    trade_dates: Iterable[date],
    retries: int = 3,
    sleep_seconds: float = 0.5,
    retry_interval_seconds: float = 2.0,
) -> BackfillResult:
    init_db(engine)
    if retries < 1:
        raise ValueError("retries must be at least 1")
    if sleep_seconds < 0:
        raise ValueError("sleep_seconds cannot be negative")
    if retry_interval_seconds < 0:
        raise ValueError("retry_interval_seconds cannot be negative")

    existing_dates = load_existing_price_dates(engine)
    day_results: list[BackfillDayResult] = []

    for trade_date in trade_dates:
        normalized_date = _to_date(trade_date)
        if normalized_date in existing_dates:
            day_results.append(
                BackfillDayResult(
                    trade_date=normalized_date,
                    status="skipped",
                    skipped_reason="already exists in SQLite",
                    attempts=0,
                )
            )
            _sleep_after_date(sleep_seconds)
            continue

        try:
            prices, attempts = _fetch_with_retry(
                fetcher=fetcher,
                trade_date=normalized_date,
                retries=retries,
                retry_interval_seconds=retry_interval_seconds,
            )
            fetched_rows = len(prices)
            risk_manager.validate_price_data(prices)
            saved_rows = save_daily_prices(engine, prices)
            for saved_date in pd.to_datetime(prices["trade_date"]).dt.date.unique():
                existing_dates.add(saved_date)
            day_results.append(
                BackfillDayResult(
                    trade_date=normalized_date,
                    status="success",
                    fetched_rows=fetched_rows,
                    saved_rows=saved_rows,
                    attempts=attempts,
                )
            )
        except DataQualityError as exc:
            day_results.append(
                BackfillDayResult(
                    trade_date=normalized_date,
                    status="skipped",
                    skipped_reason=str(exc),
                    attempts=1,
                )
            )
        except DataFetchError as exc:
            day_results.append(
                BackfillDayResult(
                    trade_date=normalized_date,
                    status="failed",
                    error=_format_fetch_failure(exc, retries),
                    attempts=retries,
                )
            )
        except Exception as exc:
            day_results.append(
                BackfillDayResult(
                    trade_date=normalized_date,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                    attempts=1,
                )
            )
        _sleep_after_date(sleep_seconds)

    scoring = recalculate_latest_scores(engine, risk_manager, scoring_config)
    summary = BackfillSummary(
        attempted_days=len(day_results),
        success_days=sum(day.status == "success" for day in day_results),
        skipped_days=sum(day.status == "skipped" for day in day_results),
        failed_days=sum(day.status == "failed" for day in day_results),
        total_rows=sum(day.saved_rows for day in day_results),
        scored_rows=scoring.scored_rows,
        candidate_rows=scoring.candidate_rows,
        scoring_date=scoring.scoring_date,
        warning=scoring.warning,
    )
    return BackfillResult(days=day_results, summary=summary)


@dataclass(frozen=True)
class _ScoringResult:
    scored_rows: int
    candidate_rows: int
    scoring_date: date | None
    warning: str = ""


def recalculate_latest_scores(
    engine: Engine,
    risk_manager: RiskManager,
    scoring_config: ScoringConfig,
) -> _ScoringResult:
    history = load_price_history(engine)
    if history.empty:
        return _ScoringResult(
            scored_rows=0,
            candidate_rows=0,
            scoring_date=None,
            warning="no price history available for scoring",
        )

    try:
        risk_manager.validate_price_data(history)
    except DataQualityError as exc:
        return _ScoringResult(
            scored_rows=0,
            candidate_rows=0,
            scoring_date=None,
            warning=f"price history failed quality checks: {exc}",
        )

    latest_date = pd.to_datetime(history["trade_date"]).max().date()
    scorer = StockScorer(scoring_config, risk_manager=risk_manager)
    scores = scorer.score(history, as_of=latest_date)
    if scores.empty:
        return _ScoringResult(
            scored_rows=0,
            candidate_rows=0,
            scoring_date=latest_date,
            warning=(
                "price data saved, but there is not enough history to score stocks yet; "
                f"strategy requires at least {scorer.config.min_history_days} rows per symbol"
            ),
        )

    scores = risk_manager.apply_candidate_controls(scores)
    saved_scores = save_candidate_scores(engine, scores)
    return _ScoringResult(
        scored_rows=saved_scores,
        candidate_rows=int(scores["is_candidate"].sum()),
        scoring_date=latest_date,
    )


def _to_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return pd.to_datetime(value).date()


def _fetch_with_retry(
    fetcher: DailyFetcher,
    trade_date: date,
    retries: int,
    retry_interval_seconds: float,
) -> tuple[pd.DataFrame, int]:
    last_error: DataFetchError | None = None
    for attempt in range(1, retries + 1):
        try:
            return fetcher.fetch(trade_date), attempt
        except DataFetchError as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(retry_interval_seconds)
    if last_error is None:
        raise DataFetchError(f"fetch failed date={trade_date}")
    raise last_error


def _sleep_after_date(sleep_seconds: float) -> None:
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)


def _format_fetch_failure(exc: DataFetchError, retries: int) -> str:
    text = str(exc)
    if "timeout" in text.lower() or "timed out" in text.lower():
        return f"timeout after {retries} retries"
    return f"fetch failed after {retries} retries: {text}"
