"""Base interfaces for enrichment providers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class EnrichmentConfig:
    provider: str = "rule_based"
    allow_external_ai: bool = False
    advisory_only: bool = True
    max_symbols_per_run: int = 30


class BaseEnricher:
    def enrich(self, frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError
