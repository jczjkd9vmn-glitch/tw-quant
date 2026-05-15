"""Best-effort TPEX official provider placeholder.

The first implementation keeps OTC sources isolated and non-blocking. It can be
expanded without touching the trading workflow.
"""

from __future__ import annotations

from datetime import date

from tw_quant.data_sources.base import ProviderResult, empty_result
from tw_quant.data_sources.twse_provider import (
    ATTENTION_DISPOSITION_COLUMNS,
    INSTITUTIONAL_COLUMNS,
    MARGIN_SHORT_COLUMNS,
)


class TPEXProvider:
    source_name = "tpex"

    def fetch_institutional(self, as_of: str | date | None = None) -> ProviderResult:
        return empty_result("tpex_institutional", INSTITUTIONAL_COLUMNS, "TPEX provider not configured; using fallback")

    def fetch_margin_short(self, as_of: str | date | None = None) -> ProviderResult:
        return empty_result("tpex_margin_short", MARGIN_SHORT_COLUMNS, "TPEX provider not configured; using fallback")

    def fetch_attention_disposition(self, as_of: str | date | None = None) -> ProviderResult:
        return empty_result(
            "tpex_attention_disposition",
            ATTENTION_DISPOSITION_COLUMNS,
            "TPEX provider not configured; using fallback",
        )
