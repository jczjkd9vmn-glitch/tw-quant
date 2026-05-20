"""Optional AI enrichment wrapper.

External AI calls are disabled by default. This class intentionally falls back
to rule-based output unless a future config explicitly enables a provider.
"""

from __future__ import annotations

import pandas as pd

from tw_quant.enrichment.base import BaseEnricher
from tw_quant.enrichment.rule_based_enricher import RuleBasedEnricher


class AIEnricher(BaseEnricher):
    def __init__(self, *, allow_external_ai: bool = False, fallback_to_rule_based: bool = True) -> None:
        self.allow_external_ai = allow_external_ai
        self.fallback_to_rule_based = fallback_to_rule_based
        self.rule_based = RuleBasedEnricher()

    def enrich(self, frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        # External providers are deliberately not called in this version.
        result = self.rule_based.enrich(frame, trade_date)
        if result.empty:
            return result
        result["enrichment_provider"] = "rule_based"
        result["ai_used"] = False
        if self.allow_external_ai:
            result["ai_warning"] = result["ai_warning"].fillna("").astype(str)
            result["ai_warning"] = result["ai_warning"].where(
                result["ai_warning"].str.strip() != "",
                "外部 AI 尚未接入；已使用 rule-based fallback",
            )
        return result
