"""Feature engineering for stock scoring."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_feature_frame(prices: pd.DataFrame) -> pd.DataFrame:
    """Build deterministic technical, liquidity, and risk features."""

    if prices.empty:
        return prices.copy()

    frame = prices.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    frame = frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    grouped = frame.groupby("symbol", group_keys=False)
    frame["history_count"] = grouped.cumcount() + 1
    frame["daily_return"] = grouped["close"].pct_change()
    frame["return_5"] = grouped["close"].pct_change(5)
    frame["return_20"] = grouped["close"].pct_change(20)
    frame["ma5"] = grouped["close"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    frame["ma20"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    frame["ma60"] = grouped["close"].transform(lambda s: s.rolling(60, min_periods=20).mean())
    frame["ma20_prev10"] = frame.groupby("symbol")["ma20"].shift(10)
    frame["ma20_slope_10"] = (frame["ma20"] / frame["ma20_prev10"]) - 1
    frame["high_20"] = grouped["high"].transform(lambda s: s.rolling(20, min_periods=10).max())
    frame["low_20"] = grouped["low"].transform(lambda s: s.rolling(20, min_periods=10).min())
    frame["volatility_20"] = grouped["daily_return"].transform(
        lambda s: s.rolling(20, min_periods=10).std()
    )
    frame["volume_ma20"] = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    frame["volume_ratio"] = frame["volume"] / frame["volume_ma20"]
    frame["liquidity_value"] = frame["close"] * frame["volume"]
    frame["prev_close"] = grouped["close"].shift()

    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - frame["prev_close"]).abs(),
            (frame["low"] - frame["prev_close"]).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame["true_range"] = true_range
    frame["atr14"] = frame.groupby("symbol")["true_range"].transform(
        lambda s: s.rolling(14, min_periods=5).mean()
    )

    frame["drawdown_20"] = (frame["close"] / frame["high_20"]) - 1
    frame["near_20d_high"] = np.where(frame["high_20"] > 0, frame["close"] / frame["high_20"], np.nan)
    return frame
