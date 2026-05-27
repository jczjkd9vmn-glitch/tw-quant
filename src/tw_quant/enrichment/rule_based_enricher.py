"""Rule-based enrichment that never calls external AI services."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

import pandas as pd

from tw_quant.enrichment.base import BaseEnricher
from tw_quant.enrichment.evidence import SourceEvidence, evidence_json


ENRICHMENT_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "enrichment_status",
    "enrichment_provider",
    "ai_used",
    "source_evidence_count",
    "missing_data_flags",
    "enriched_industry",
    "enriched_industry_source",
    "industry_main",
    "industry_sub",
    "sector_strength_mode",
    "relative_strength_5d",
    "relative_strength_20d",
    "valuation_context",
    "valuation_risk_level",
    "margin_credit_context",
    "margin_risk_level",
    "margin_price_divergence",
    "sector_context",
    "risk_explanation",
    "opportunity_explanation",
    "data_quality_explanation",
    "manual_review_focus",
    "ai_summary",
    "ai_warning",
    "source_evidence_json",
]


@dataclass(frozen=True)
class ValuationContext:
    text: str
    risk_level: str


@dataclass(frozen=True)
class MarginContext:
    text: str
    risk_level: str
    divergence: bool


class RuleBasedEnricher(BaseEnricher):
    """Create concise Traditional Chinese risk context from existing fields."""

    forbidden_terms = ("必買", "必賣", "保證獲利")

    def enrich(self, frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=ENRICHMENT_COLUMNS)

        all_rows = frame.copy()
        outputs: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            evidence = self._evidence(row, trade_date)
            valuation = self.valuation_context(row, all_rows)
            margin = self.margin_credit_context(row)
            missing = self._missing_flags(row)
            status = "OK" if not missing else "PARTIAL"
            if len(missing) >= 3:
                status = "INSUFFICIENT_DATA"
            risk_explanation = self._risk_explanation(row, valuation, margin, missing)
            opportunity = self._opportunity_explanation(row)
            data_quality = "資料完整度可供輔助判斷" if not missing else "資料不足：" + "、".join(missing)
            focus = self._manual_focus(row, valuation, margin, missing)
            summary = self._sanitize(
                f"{risk_explanation}；{opportunity}。此為紙上交易輔助說明，需人工確認。"
            )
            outputs.append(
                {
                    "trade_date": trade_date,
                    "stock_id": str(row.get("stock_id", "")).strip(),
                    "stock_name": str(row.get("stock_name", "")),
                    "enrichment_status": status,
                    "enrichment_provider": "rule_based",
                    "ai_used": False,
                    "source_evidence_count": len(evidence),
                    "missing_data_flags": "；".join(missing),
                    "enriched_industry": self._industry(row),
                    "enriched_industry_source": self._text(row.get("industry_source")) or self._text(row.get("source")) or "local_csv_or_report",
                    "industry_main": self._text(row.get("industry_main")) or self._text(row.get("industry")) or "未知產業",
                    "industry_sub": self._text(row.get("industry_sub")) or self._text(row.get("sub_industry")) or "",
                    "sector_strength_mode": self._text(row.get("sector_strength_mode")) or "",
                    "relative_strength_5d": row.get("relative_strength_5d", ""),
                    "relative_strength_20d": row.get("relative_strength_20d", ""),
                    "valuation_context": valuation.text,
                    "valuation_risk_level": valuation.risk_level,
                    "margin_credit_context": margin.text,
                    "margin_risk_level": margin.risk_level,
                    "margin_price_divergence": margin.divergence,
                    "sector_context": self._sector_context(row),
                    "risk_explanation": risk_explanation,
                    "opportunity_explanation": opportunity,
                    "data_quality_explanation": data_quality,
                    "manual_review_focus": focus,
                    "ai_summary": summary,
                    "ai_warning": "" if status == "OK" else "資料不足時不做強結論；本說明僅供人工確認",
                    "source_evidence_json": evidence_json(evidence),
                }
            )
        return pd.DataFrame(outputs, columns=ENRICHMENT_COLUMNS)

    def valuation_context(self, row: pd.Series, universe: pd.DataFrame) -> ValuationContext:
        pe = self._float(row.get("pe_ratio"))
        pb = self._float(row.get("pb_ratio"))
        dividend = self._float(row.get("dividend_yield"))
        industry = self._industry(row)
        if pe is None:
            return ValuationContext("估值資料不足，無法判斷 PE 是否偏高", "UNKNOWN")
        if pe < 0:
            return ValuationContext(f"PE={pe:.2f}，可能反映虧損或 EPS 為負，PE 參考性偏低", "HIGH")

        peers = universe.copy()
        peer_column = "industry_main" if "industry_main" in peers.columns else "industry"
        if peer_column in peers.columns and industry not in {"全市場", "未知產業"}:
            peers = peers[peers[peer_column].fillna("").astype(str) == industry]
        peer_pe = pd.to_numeric(peers.get("pe_ratio", pd.Series(dtype=float)), errors="coerce")
        median = float(peer_pe[peer_pe > 0].median()) if (peer_pe > 0).any() else None
        fallback_note = ""
        if median is None:
            all_pe = pd.to_numeric(universe.get("pe_ratio", pd.Series(dtype=float)), errors="coerce")
            median = float(all_pe[all_pe > 0].median()) if (all_pe > 0).any() else None
            fallback_note = "；無同產業資料，使用全市場中位數 fallback"
        if median is None or median <= 0:
            return ValuationContext(f"PE={pe:.2f}，缺少同業或市場中位數，估值脈絡不足", "UNKNOWN")
        ratio = pe / median
        if ratio > 1.5:
            level = "HIGH"
        elif ratio > 1.2:
            level = "MEDIUM"
        else:
            level = "LOW"
        extras = []
        if pb is not None:
            extras.append(f"PB={pb:.2f}")
        if dividend is not None:
            extras.append(f"殖利率={dividend:.2f}%")
        risk_note = ""
        if level == "HIGH":
            risk_note = "；估值明顯高於比較基準，若成長未持續，評價可能受壓縮"
        elif level == "MEDIUM":
            risk_note = "；估值高於比較基準，需搭配營收與獲利成長檢查"
        return ValuationContext(
            f"PE={pe:.2f}，{industry}比較基準中位數約 {median:.2f}，約為 {ratio:.2f} 倍{fallback_note}"
            + ("；" + "，".join(extras) if extras else "")
            + risk_note,
            level,
        )

    def margin_credit_context(self, row: pd.Series) -> MarginContext:
        margin_5d = self._float(row.get("margin_change_5d"))
        margin_20d = self._float(row.get("margin_change_20d"))
        price_5d = self._float(row.get("price_return_5d"))
        price_20d = self._float(row.get("price_return_20d"))
        institutional = self._float(row.get("institutional_net_buy_5d"))
        attention = self._bool(row.get("is_attention_stock")) or self._bool(row.get("is_disposition_stock"))
        if margin_5d is None and margin_20d is None:
            return MarginContext("信用交易資料不足，融資籌碼脈絡採中性", "UNKNOWN", False)

        divergence_5d = margin_5d is not None and margin_5d > 0 and (price_5d is None or price_5d <= 0)
        divergence_20d = margin_20d is not None and margin_20d > 0 and (price_20d is None or price_20d <= 0)
        level = "LOW"
        if divergence_20d:
            level = "HIGH"
        elif divergence_5d:
            level = "MEDIUM"
        if institutional is not None and institutional < 0 and level in {"MEDIUM", "HIGH"}:
            level = "HIGH"
        if attention and level != "UNKNOWN":
            level = "HIGH" if level == "MEDIUM" else level
        parts = []
        if margin_5d is not None:
            parts.append(f"5 日融資變化 {margin_5d:,.0f}")
        if price_5d is not None:
            parts.append(f"5 日股價報酬 {price_5d:.2%}")
        if margin_20d is not None:
            parts.append(f"20 日融資變化 {margin_20d:,.0f}")
        if institutional is not None:
            parts.append(f"5 日法人買賣超 {institutional:,.0f}")
        if divergence_20d:
            note = "；近 20 日融資增加但股價沒有同步上漲，槓桿資金增加但價格反應有限，需檢查法人是否承接"
        elif divergence_5d:
            note = "；近 5 日融資增加但股價沒有明顯上漲，可能只是短期異常，需搭配法人與量能確認"
        else:
            note = "；未見明顯融資/價格背離，不代表沒有籌碼風險"
        return MarginContext("，".join(parts) + note, level, bool(divergence_5d or divergence_20d))

    def _evidence(self, row: pd.Series, trade_date: str) -> list[SourceEvidence]:
        items: list[SourceEvidence] = []
        for field in [
            "candidate_grade",
            "decision",
            "pe_ratio",
            "pb_ratio",
            "dividend_yield",
            "margin_change_5d",
            "price_return_5d",
            "liquidity_score",
            "sector_strength_score",
            "event_risk_level",
            "risk_flags",
            "valuation_context",
            "margin_credit_context",
            "sector_context",
            "liquidity_warning",
            "sector_strength_warning",
        ]:
            value = row.get(field)
            if not self._blank(value):
                items.append(
                    SourceEvidence(
                        source_name=self._text(row.get("source")) or "reports",
                        source_type="csv",
                        source_date=trade_date,
                        field_name=field,
                        field_value=value,
                        confidence=0.8,
                    )
                )
        return items

    def _missing_flags(self, row: pd.Series) -> list[str]:
        missing = []
        for column, label in [
            ("pe_ratio", "估值資料不足"),
            ("financial_score", "財報資料不足"),
            ("margin_change_5d", "融資融券資料不足"),
        ]:
            if self._blank(row.get(column)):
                missing.append(label)
        if self._industry(row) == "未知產業":
            missing.append("產業分類資料不足")
        return missing

    def _risk_explanation(self, row: pd.Series, valuation: ValuationContext, margin: MarginContext, missing: list[str]) -> str:
        flags = self._text(row.get("investment_risk_flags")) or self._investment_only_flags(row.get("risk_flags"))
        data_flags = self._text(row.get("data_quality_flags"))
        risks = []
        if flags:
            risks.append(f"投資風險標籤：{flags}")
        if valuation.risk_level in {"MEDIUM", "HIGH"}:
            risks.append(f"估值風險 {valuation.risk_level}：{valuation.text}")
        if margin.risk_level in {"MEDIUM", "HIGH"}:
            risks.append(f"融資籌碼風險 {margin.risk_level}：{margin.text}")
        liquidity = self._float(row.get("liquidity_score"))
        avg_turnover = self._float(row.get("avg_turnover_20d"))
        if liquidity is not None and liquidity < 50:
            if avg_turnover is not None:
                risks.append(f"流動性偏低：20 日平均成交金額約 {avg_turnover:,.0f} 元，短線進出可能有滑價風險")
            else:
                risks.append("流動性偏低：短線進出可能有滑價風險")
        if data_flags or missing:
            risks.append("資料不足另列檢查，不視為投資風險")
        return "；".join(risks) if risks else "未見重大資料警訊"

    def _opportunity_explanation(self, row: pd.Series) -> str:
        score = self._float(row.get("final_market_score")) or self._float(row.get("multi_factor_score"))
        grade = self._text(row.get("candidate_grade"))
        if score is not None and score >= 70:
            return f"分數偏高（{score:.1f}），可列入觀察，但不是買賣建議"
        if grade:
            return f"候選分級 {grade}，僅供排序與人工檢查"
        return "目前只能依既有分數與風險標籤輔助觀察"

    def _manual_focus(self, row: pd.Series, valuation: ValuationContext, margin: MarginContext, missing: list[str]) -> str:
        focuses = []
        if valuation.risk_level in {"MEDIUM", "HIGH", "UNKNOWN"}:
            focuses.append("確認估值是否合理")
        if margin.risk_level in {"MEDIUM", "HIGH"}:
            focuses.append("檢查融資與法人籌碼是否背離")
        if self._bool(row.get("event_blocked")) or self._text(row.get("event_risk_level")).upper() == "HIGH":
            focuses.append("確認重大事件風險")
        if missing:
            focuses.append("補查資料缺口")
        return "；".join(focuses) if focuses else "檢查停損、流動性與事件風險"

    def _sector_context(self, row: pd.Series) -> str:
        industry = self._industry(row)
        mode = self._text(row.get("sector_strength_mode")) or ("industry_relative" if industry != "未知產業" else "unknown")
        score = self._float(row.get("sector_strength_score"))
        rs5 = self._float(row.get("relative_strength_5d"))
        rs20 = self._float(row.get("relative_strength_20d"))
        if score is None:
            return f"{industry}；產業相對強弱資料不足"
        basis = "同產業" if mode == "industry_relative" else "全市場平均" if mode == "market_relative_fallback" else "未知基準"
        if mode == "market_relative_fallback":
            fallback_note = "；目前缺少正式產業分類，因此只能視為全市場相對強弱，不能直接推論為同產業強勢"
        elif mode == "industry_relative":
            fallback_note = "；使用產業分類進行同產業相對比較"
        else:
            fallback_note = "；比較基準不足，需人工確認"
        parts = [f"{industry}；相對 {basis} 強弱分數 {score:.1f}"]
        if rs5 is not None:
            parts.append(f"5 日相對強弱 {rs5:.2%}")
        if rs20 is not None:
            parts.append(f"20 日相對強弱 {rs20:.2%}")
        return "，".join(parts) + fallback_note

    def _industry(self, row: pd.Series) -> str:
        return (
            self._text(row.get("industry_main"))
            or self._text(row.get("industry"))
            or self._text(row.get("enriched_industry"))
            or "未知產業"
        )

    def _sanitize(self, text: str) -> str:
        result = text
        for term in self.forbidden_terms:
            result = result.replace(term, "需人工確認")
        return result

    def _investment_only_flags(self, value: Any) -> str:
        flags = []
        for part in self._text(value).replace("|", "；").split("；"):
            text = part.strip()
            if text and not any(keyword in text for keyword in ["資料不足", "缺少產業分類", "採中性"]):
                flags.append(text)
        return "；".join(dict.fromkeys(flags))

    @staticmethod
    def _float(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return None if pd.isna(parsed) else parsed

    @staticmethod
    def _text(value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()

    @staticmethod
    def _blank(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, float) and pd.isna(value):
            return True
        text = str(value).strip()
        return text == "" or text.lower() in {"nan", "none", "-"}

    @staticmethod
    def _bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}
