"""Base types for market intelligence providers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Protocol


@dataclass
class MarketContext:
    symbol: str
    date: str
    close: float | None = None
    volume: float | None = None
    volume_change_ratio: float | None = None
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    dividend_yield: float | None = None
    revenue_growth_yoy: float | None = None
    eps_growth_yoy: float | None = None
    latest_news_titles: list[str] = field(default_factory=list)
    matched_news_keywords: list[str] = field(default_factory=list)
    news_sentiment_score: float = 0.0
    fundamental_score: float = 50.0
    valuation_score: float = 50.0
    momentum_score: float = 50.0
    final_market_score: float = 50.0
    confidence_score: float = 50.0
    risk_score: float = 50.0
    risk_flags: list[str] = field(default_factory=list)
    final_comment: str = "資料不足，僅能依技術面判斷"
    data_source: str = "mock"
    warning_message: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class MarketIntelProvider(Protocol):
    """Provider protocol. Providers must never raise for ordinary source outages."""

    source_name: str

    def fetch(self, symbols: list[str], as_of: str | None = None) -> list[MarketContext]:
        ...
