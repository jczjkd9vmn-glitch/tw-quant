"""SQLite persistence helpers."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import Engine, create_engine, delete, insert, select

from tw_quant.data.exceptions import DataQualityError
from tw_quant.data.models import candidate_scores, daily_prices, metadata


PRICE_COLUMNS = [
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


def create_db_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine and ensure a relative SQLite folder exists."""

    if database_url.startswith("sqlite:///"):
        db_path = database_url.replace("sqlite:///", "", 1)
        path = Path(db_path)
        if not path.is_absolute() and path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(database_url, future=True)


def init_db(engine: Engine) -> None:
    metadata.create_all(engine)


def save_daily_prices(engine: Engine, prices: pd.DataFrame) -> int:
    """Replace daily price rows by trade_date/symbol/market."""

    frame = _prepare_prices(prices)
    records = frame[PRICE_COLUMNS + ["fetched_at"]].to_dict(orient="records")
    with engine.begin() as conn:
        for trade_date, symbol, market in _unique_keys(frame, ["trade_date", "symbol", "market"]):
            conn.execute(
                delete(daily_prices).where(
                    daily_prices.c.trade_date == trade_date,
                    daily_prices.c.symbol == symbol,
                    daily_prices.c.market == market,
                )
            )
        if records:
            conn.execute(insert(daily_prices), records)
    return len(records)


def load_price_history(
    engine: Engine,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    stmt = select(daily_prices).order_by(daily_prices.c.trade_date, daily_prices.c.symbol)
    if start_date:
        stmt = stmt.where(daily_prices.c.trade_date >= pd.to_datetime(start_date).date())
    if end_date:
        stmt = stmt.where(daily_prices.c.trade_date <= pd.to_datetime(end_date).date())

    with engine.connect() as conn:
        frame = pd.read_sql(stmt, conn)
    if not frame.empty:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


def load_existing_price_dates(engine: Engine) -> set:
    stmt = select(daily_prices.c.trade_date).distinct()
    with engine.connect() as conn:
        rows = conn.execute(stmt).scalars().all()
    return {pd.to_datetime(row).date() for row in rows}


def load_latest_price_date(engine: Engine) -> date | None:
    stmt = select(daily_prices.c.trade_date).order_by(daily_prices.c.trade_date.desc()).limit(1)
    with engine.connect() as conn:
        row = conn.execute(stmt).scalar_one_or_none()
    if row is None:
        return None
    return pd.to_datetime(row).date()


def save_candidate_scores(engine: Engine, scores: pd.DataFrame) -> int:
    if scores.empty:
        return 0

    frame = scores.copy()
    required = [
        "trade_date",
        "symbol",
        "name",
        "close",
        "total_score",
        "trend_score",
        "momentum_score",
        "fundamental_score",
        "chip_score",
        "risk_score",
        "buy_reasons",
        "stop_loss",
        "suggested_position_pct",
        "is_candidate",
        "risk_pass",
        "risk_reasons",
        "data_quality_status",
    ]
    _require_columns(frame, required)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    frame["created_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    frame["is_candidate"] = frame["is_candidate"].astype(int)
    frame["risk_pass"] = frame["risk_pass"].astype(int)

    records = frame[required + ["created_at"]].to_dict(orient="records")
    with engine.begin() as conn:
        for trade_date, symbol in _unique_keys(frame, ["trade_date", "symbol"]):
            conn.execute(
                delete(candidate_scores).where(
                    candidate_scores.c.trade_date == trade_date,
                    candidate_scores.c.symbol == symbol,
                )
            )
        if records:
            conn.execute(insert(candidate_scores), records)
    return len(records)


def load_candidate_scores(engine: Engine, trade_date: str | None = None) -> pd.DataFrame:
    stmt = select(candidate_scores).order_by(
        candidate_scores.c.trade_date.desc(), candidate_scores.c.total_score.desc()
    )
    if trade_date:
        stmt = stmt.where(candidate_scores.c.trade_date == pd.to_datetime(trade_date).date())
    with engine.connect() as conn:
        frame = pd.read_sql(stmt, conn)
    if not frame.empty:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


def _prepare_prices(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    _require_columns(frame, PRICE_COLUMNS)
    if frame.empty:
        raise DataQualityError("daily price frame is empty")

    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    for column in ["open", "high", "low", "close", "volume", "turnover"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["fetched_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    frame["symbol"] = frame["symbol"].astype(str).str.strip()
    frame["name"] = frame["name"].fillna("").astype(str).str.strip()
    frame["market"] = frame["market"].fillna("TSE").astype(str).str.strip()
    frame["source"] = frame["source"].fillna("unknown").astype(str).str.strip()
    return frame


def _require_columns(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise DataQualityError(f"missing required columns: {', '.join(missing)}")


def _unique_keys(frame: pd.DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(row) for row in frame[columns].drop_duplicates().itertuples(index=False, name=None)]
