"""Deterministic fallback market intelligence provider."""

from __future__ import annotations

from tw_quant.market_intel.providers.base import MarketContext


class MockMarketIntelProvider:
    """Neutral provider used when no external source is configured."""

    source_name = "mock"

    def fetch(self, symbols: list[str], as_of: str | None = None) -> list[MarketContext]:
        date_text = as_of or ""
        return [
            MarketContext(
                symbol=symbol,
                date=date_text,
                data_source=self.source_name,
                warning_message="未設定市場資料來源，使用中性 mock 資料",
            )
            for symbol in symbols
        ]
