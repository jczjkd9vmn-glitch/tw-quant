"""Daily batch workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from tw_quant.config import load_config
from tw_quant.data.database import (
    create_db_engine,
    init_db,
    load_latest_price_date,
    load_price_history,
    save_candidate_scores,
    save_daily_prices,
)
from tw_quant.data.exceptions import DataQualityError, TradingHalted
from tw_quant.data.fetcher import TWSEDailyFetcher
from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.strategy.scoring import ScoringConfig, StockScorer


@dataclass(frozen=True)
class DailyRunResult:
    trade_date: date
    fetched_rows: int
    scored_rows: int
    candidate_rows: int
    message: str = ""
    fallback_date: date | None = None
    fallback_reason: str = ""


def run_daily_pipeline(
    config_path: str | Path = "config.yaml",
    trade_date: str | date | None = None,
    fetch: bool = True,
    allow_fallback_latest: bool = False,
) -> DailyRunResult:
    config = load_config(config_path)
    engine = create_db_engine(config["database"]["url"])
    init_db(engine)
    target_date = pd.to_datetime(trade_date or pd.Timestamp.today()).date()

    fetched_rows = 0
    fallback_date: date | None = None
    fallback_reason = ""
    risk_manager = RiskManager(RiskConfig.from_mapping(config["risk"]))

    if fetch:
        fetcher = TWSEDailyFetcher(
            url=config["data"]["twse_url"],
            timeout_seconds=int(config["data"]["request_timeout_seconds"]),
        )
        try:
            prices = fetcher.fetch(target_date)
        except DataQualityError as exc:
            if not allow_fallback_latest:
                raise
            latest_date = load_latest_price_date(engine)
            if latest_date is None:
                raise TradingHalted("no price history available for fallback") from exc
            target_date = latest_date
            fallback_date = latest_date
            fallback_reason = "no trading data"
        else:
            risk_manager.validate_price_data(prices)
            fetched_rows = save_daily_prices(engine, prices)

    history = load_price_history(engine, end_date=str(target_date))
    if history.empty:
        raise TradingHalted("no price history available for scoring")

    try:
        risk_manager.validate_price_data(history)
    except DataQualityError as exc:
        raise TradingHalted(f"price history failed quality checks: {exc}") from exc

    scorer = StockScorer(
        ScoringConfig.from_mapping(config["strategy"]),
        risk_manager=risk_manager,
    )
    scores = scorer.score(history, as_of=target_date)
    if scores.empty:
        return DailyRunResult(
            trade_date=target_date,
            fetched_rows=fetched_rows,
            scored_rows=0,
            candidate_rows=0,
            message=(
                "price data saved, but there is not enough history to score stocks yet; "
                f"strategy requires at least {scorer.config.min_history_days} rows per symbol"
            ),
            fallback_date=fallback_date,
            fallback_reason=fallback_reason,
        )

    scores = risk_manager.apply_candidate_controls(scores)
    saved_scores = save_candidate_scores(engine, scores)
    candidates = int(scores["is_candidate"].sum())
    return DailyRunResult(
        trade_date=target_date,
        fetched_rows=fetched_rows,
        scored_rows=saved_scores,
        candidate_rows=candidates,
        fallback_date=fallback_date,
        fallback_reason=fallback_reason,
    )
