"""SQLAlchemy table definitions."""

from __future__ import annotations

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)


metadata = MetaData()


daily_prices = Table(
    "daily_prices",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("trade_date", Date, nullable=False),
    Column("symbol", String(16), nullable=False),
    Column("name", String(64), nullable=False, default=""),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", Float, nullable=False),
    Column("turnover", Float, nullable=True),
    Column("market", String(16), nullable=False, default="TSE"),
    Column("source", String(64), nullable=False, default="unknown"),
    Column("fetched_at", DateTime, nullable=False),
    UniqueConstraint("trade_date", "symbol", "market", name="uq_daily_price"),
)
Index("ix_daily_prices_symbol_date", daily_prices.c.symbol, daily_prices.c.trade_date)


candidate_scores = Table(
    "candidate_scores",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("trade_date", Date, nullable=False),
    Column("symbol", String(16), nullable=False),
    Column("name", String(64), nullable=False, default=""),
    Column("close", Float, nullable=False),
    Column("total_score", Float, nullable=False),
    Column("trend_score", Float, nullable=False),
    Column("momentum_score", Float, nullable=False),
    Column("fundamental_score", Float, nullable=False),
    Column("chip_score", Float, nullable=False),
    Column("risk_score", Float, nullable=False),
    Column("buy_reasons", Text, nullable=False),
    Column("stop_loss", Float, nullable=False),
    Column("suggested_position_pct", Float, nullable=False),
    Column("is_candidate", Integer, nullable=False, default=0),
    Column("risk_pass", Integer, nullable=False, default=0),
    Column("risk_reasons", Text, nullable=False, default=""),
    Column("data_quality_status", String(32), nullable=False, default="OK"),
    Column("created_at", DateTime, nullable=False),
    UniqueConstraint("trade_date", "symbol", name="uq_candidate_score"),
)
Index("ix_candidate_scores_date_score", candidate_scores.c.trade_date, candidate_scores.c.total_score)


simulated_orders = Table(
    "simulated_orders",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("created_at", DateTime, nullable=False),
    Column("trade_date", Date, nullable=False),
    Column("symbol", String(16), nullable=False),
    Column("side", String(8), nullable=False),
    Column("quantity", Float, nullable=False),
    Column("price", Float, nullable=False),
    Column("reason", Text, nullable=False),
    Column("risk_reasons", Text, nullable=False),
    Column("status", String(16), nullable=False),
)


backtest_runs = Table(
    "backtest_runs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("created_at", DateTime, nullable=False),
    Column("start_date", Date, nullable=False),
    Column("end_date", Date, nullable=False),
    Column("initial_cash", Float, nullable=False),
    Column("ending_equity", Float, nullable=False),
    Column("total_return", Float, nullable=False),
    Column("max_drawdown", Float, nullable=False),
    Column("sharpe", Float, nullable=False),
    Column("win_rate", Float, nullable=False),
    Column("trades", Integer, nullable=False),
    Column("notes", Text, nullable=False, default=""),
)
