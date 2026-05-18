"""Best-effort market intelligence provider backed by local official-data CSVs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.events.material_events import load_material_events, score_material_events
from tw_quant.market_intel.providers.base import MarketContext
from tw_quant.market_intel.providers.mock_provider import MockMarketIntelProvider
from tw_quant.market_intel.scoring import build_market_context
from tw_quant.scoring.official_factors import load_attention_disposition, score_attention_disposition


class RealMarketIntelProvider:
    """Use available official/fallback event data before falling back to mock.

    This provider only produces report/scoring context. It must not place orders
    or directly change trading behavior.
    """

    source_name = "real"

    def __init__(
        self,
        data_dir: str | Path = "data",
        allow_mock: bool = True,
        block_disposition_stock: bool = True,
        block_attention_stock: bool = False,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.allow_mock = allow_mock
        self.block_disposition_stock = block_disposition_stock
        self.block_attention_stock = block_attention_stock

    def fetch(self, symbols: list[str], as_of: str | None = None) -> list[MarketContext]:
        events = load_material_events(self.data_dir / "material_events.csv")
        attention = load_attention_disposition(self.data_dir / "attention_disposition.csv")
        has_any_source = not events.empty or not attention.empty
        if not has_any_source:
            if self.allow_mock:
                return MockMarketIntelProvider().fetch(symbols, as_of=as_of)
            return [
                build_market_context(
                    symbol=str(symbol),
                    date=as_of or "",
                    data_source="csv_fallback",
                    warning_message="市場事件資料不足，採中性處理",
                    system_comment="市場事件資料不足，僅作中性處理",
                )
                for symbol in symbols
            ]

        contexts: list[MarketContext] = []
        for symbol in symbols:
            contexts.append(self._build_context(str(symbol), as_of or "", events, attention))
        return contexts

    def _build_context(
        self,
        symbol: str,
        as_of: str,
        events: pd.DataFrame,
        attention: pd.DataFrame,
    ) -> MarketContext:
        event_score = score_material_events(symbol, events)
        attention_score = score_attention_disposition(
            symbol,
            attention,
            {
                "block_disposition_stock": self.block_disposition_stock,
                "block_attention_stock": self.block_attention_stock,
            },
        )
        titles = _event_titles(symbol, events)
        risk_flags: list[str] = []
        warning_parts: list[str] = []
        comment_parts: list[str] = []
        source_parts: list[str] = []

        if not events.empty:
            source_parts.append("material_events")
        if not attention.empty:
            source_parts.append("attention_disposition")

        if bool(attention_score.get("is_attention_stock")):
            reason = str(attention_score.get("attention_reason") or "").strip()
            risk_flags.append("注意股")
            titles.append(f"注意股：{reason}" if reason else "注意股")
            comment_parts.append("注意股，短線波動風險偏高")
        if bool(attention_score.get("is_disposition_stock")):
            reason = str(attention_score.get("disposition_reason") or "").strip()
            risk_flags.append("處置股")
            titles.append(f"處置股：{reason}" if reason else "處置股")
            comment_parts.append("處置股，預設阻擋新進場")
        if str(event_score.get("event_risk_level", "")).upper() == "HIGH":
            risk_flags.append("重大負面事件")

        if not titles:
            warning_parts.append("市場事件資料不足，採中性處理")
            comment_parts.append("依官方重大訊息與注意處置資料，未發現重大負面事件")

        event_risk_score = min(
            _number(event_score.get("event_risk_score"), 50.0),
            _number(attention_score.get("event_risk_score"), 50.0),
        )
        source = "mixed" if len(source_parts) > 1 else "best_effort"
        if source_parts and all(path.exists() for path in [self.data_dir / "material_events.csv", self.data_dir / "attention_disposition.csv"]):
            source = "mixed"

        return build_market_context(
            symbol=symbol,
            date=as_of,
            latest_news_titles=titles,
            event_risk_score=event_risk_score,
            risk_flags=risk_flags,
            data_source=source,
            warning_message="；".join(warning_parts),
            system_comment="；".join(comment_parts),
        )


def _event_titles(symbol: str, events: pd.DataFrame) -> list[str]:
    if events.empty:
        return []
    frame = events[events["stock_id"].astype(str).str.strip() == str(symbol).strip()].copy()
    if frame.empty:
        return []
    frame = frame.sort_values("event_date").tail(3)
    titles = []
    for _, row in frame.iterrows():
        title = str(row.get("title", "") or "").strip()
        summary = str(row.get("summary", "") or "").strip()
        text = title or summary
        if text:
            titles.append(text)
    return titles


def _number(value: object, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return parsed
