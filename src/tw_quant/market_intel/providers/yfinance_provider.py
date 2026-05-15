"""Optional yfinance provider.

yfinance is not a required dependency. If it is unavailable or a request fails,
the provider returns neutral contexts with warnings instead of raising.
"""

from __future__ import annotations

from tw_quant.market_intel.providers.base import MarketContext
from tw_quant.market_intel.scoring import build_market_context


class YFinanceMarketIntelProvider:
    source_name = "yfinance"

    def fetch(self, symbols: list[str], as_of: str | None = None) -> list[MarketContext]:
        try:
            import yfinance as yf  # type: ignore
        except Exception as exc:
            return [_warning_context(symbol, as_of, f"yfinance 不可用：{exc}") for symbol in symbols]

        contexts: list[MarketContext] = []
        for symbol in symbols:
            ticker_symbol = _tw_symbol(symbol)
            try:
                ticker = yf.Ticker(ticker_symbol)
                info = getattr(ticker, "info", {}) or {}
                history = ticker.history(period="3mo")
                close = _latest(history, "Close")
                volume = _latest(history, "Volume")
                volume_change_ratio = _volume_change_ratio(history)
                contexts.append(
                    build_market_context(
                        symbol=symbol,
                        date=as_of or "",
                        close=close,
                        volume=volume,
                        volume_change_ratio=volume_change_ratio,
                        pe_ratio=info.get("trailingPE"),
                        pb_ratio=info.get("priceToBook"),
                        dividend_yield=_yield_percent(info.get("dividendYield")),
                        data_source=self.source_name,
                    )
                )
            except Exception as exc:
                contexts.append(_warning_context(symbol, as_of, f"yfinance 抓取失敗：{exc}"))
        return contexts


def _tw_symbol(symbol: str) -> str:
    text = str(symbol).strip()
    return text if "." in text else f"{text}.TW"


def _latest(frame, column: str) -> float | None:
    if frame is None or frame.empty or column not in frame.columns:
        return None
    value = frame[column].dropna().tail(1)
    if value.empty:
        return None
    return float(value.iloc[0])


def _volume_change_ratio(frame) -> float | None:
    if frame is None or frame.empty or "Volume" not in frame.columns or len(frame) < 6:
        return None
    recent = frame["Volume"].dropna().tail(5)
    previous = frame["Volume"].dropna().iloc[-10:-5]
    if recent.empty or previous.empty or previous.mean() == 0:
        return None
    return float(recent.mean() / previous.mean())


def _yield_percent(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value) * 100
    except (TypeError, ValueError):
        return None


def _warning_context(symbol: str, as_of: str | None, warning: str) -> MarketContext:
    return build_market_context(
        symbol=symbol,
        date=as_of or "",
        data_source="yfinance",
        warning_message=warning,
    )
