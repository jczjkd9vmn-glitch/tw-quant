"""Rule-based stock scoring."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from tw_quant.risk.controls import RiskManager
from tw_quant.strategy.features import build_feature_frame


DEFAULT_WEIGHTS = {
    "trend": 0.30,
    "momentum": 0.25,
    "fundamental": 0.15,
    "chip": 0.15,
    "risk": 0.15,
}


@dataclass(frozen=True)
class ScoringConfig:
    minimum_total_score: float = 70
    min_history_days: int = 40
    max_candidates: int = 20
    weights: dict[str, float] = field(default_factory=lambda: DEFAULT_WEIGHTS.copy())

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> "ScoringConfig":
        weights = DEFAULT_WEIGHTS.copy()
        weights.update(values.get("weights", {}))
        total_weight = sum(float(v) for v in weights.values())
        if total_weight <= 0:
            weights = DEFAULT_WEIGHTS.copy()
            total_weight = sum(weights.values())
        weights = {key: float(value) / total_weight for key, value in weights.items()}
        return cls(
            minimum_total_score=float(values.get("minimum_total_score", cls.minimum_total_score)),
            min_history_days=int(values.get("min_history_days", cls.min_history_days)),
            max_candidates=int(values.get("max_candidates", cls.max_candidates)),
            weights=weights,
        )


class StockScorer:
    def __init__(self, config: ScoringConfig | None = None, risk_manager: RiskManager | None = None):
        self.config = config or ScoringConfig()
        self.risk_manager = risk_manager or RiskManager()

    def score(self, prices: pd.DataFrame, as_of: str | pd.Timestamp | None = None) -> pd.DataFrame:
        features = build_feature_frame(prices)
        if features.empty:
            return pd.DataFrame()

        if as_of is not None:
            as_of_date = pd.to_datetime(as_of)
            features = features[features["trade_date"] <= as_of_date]
        if features.empty:
            return pd.DataFrame()

        latest_date = features["trade_date"].max()
        latest = features[features["trade_date"] == latest_date].copy()
        latest = latest[latest["history_count"] >= self.config.min_history_days]
        if latest.empty:
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for _, row in latest.iterrows():
            trend_score, trend_reasons = _score_trend(row)
            momentum_score, momentum_reasons = _score_momentum(row)
            fundamental_score, fundamental_reasons = _score_fundamental(row)
            chip_score, chip_reasons = _score_chip(row)
            risk_score, risk_reasons = _score_risk(row)

            total_score = (
                trend_score * self.config.weights["trend"]
                + momentum_score * self.config.weights["momentum"]
                + fundamental_score * self.config.weights["fundamental"]
                + chip_score * self.config.weights["chip"]
                + risk_score * self.config.weights["risk"]
            )
            row_with_score = row.copy()
            row_with_score["total_score"] = total_score
            stop_loss = self.risk_manager.calculate_stop_loss(row_with_score)
            position_pct = self.risk_manager.suggest_position_pct(row_with_score, stop_loss)
            all_reasons = trend_reasons + momentum_reasons + fundamental_reasons + chip_reasons
            if not all_reasons:
                all_reasons = ["量化條件不足，未形成明確買進理由"]

            rows.append(
                {
                    "trade_date": row["trade_date"],
                    "symbol": str(row["symbol"]),
                    "name": str(row.get("name", "")),
                    "close": float(row["close"]),
                    "total_score": round(float(total_score), 2),
                    "trend_score": round(trend_score, 2),
                    "momentum_score": round(momentum_score, 2),
                    "fundamental_score": round(fundamental_score, 2),
                    "chip_score": round(chip_score, 2),
                    "risk_score": round(risk_score, 2),
                    "buy_reasons": "；".join(all_reasons),
                    "stop_loss": stop_loss,
                    "suggested_position_pct": position_pct,
                    "is_candidate": False,
                    "risk_pass": False,
                    "risk_reasons": "尚未執行風控",
                    "data_quality_status": "OK",
                    "atr14": row.get("atr14"),
                    "volatility_20": row.get("volatility_20"),
                    "liquidity_value": row.get("liquidity_value"),
                    "return_5": row.get("return_5"),
                    "return_20": row.get("return_20"),
                }
            )

        scored = pd.DataFrame(rows).sort_values(
            ["total_score", "risk_score", "liquidity_value"], ascending=[False, False, False]
        )
        candidate_mask = scored["total_score"] >= self.config.minimum_total_score
        top_symbols = set(scored[candidate_mask].head(self.config.max_candidates)["symbol"])
        scored["is_candidate"] = scored["symbol"].isin(top_symbols)
        return scored.reset_index(drop=True)


def _score_trend(row: pd.Series) -> tuple[float, list[str]]:
    score = 45.0
    reasons: list[str] = []
    close = _value(row, "close")
    ma20 = _value(row, "ma20")
    ma60 = _value(row, "ma60")
    near_high = _value(row, "near_20d_high")
    slope = _value(row, "ma20_slope_10")

    if close and ma20 and close > ma20:
        score += 18
        reasons.append("收盤價高於 20 日均線")
    if close and ma60 and close > ma60:
        score += 14
        reasons.append("收盤價高於 60 日均線")
    if ma20 and ma60 and ma20 > ma60:
        score += 12
        reasons.append("20 日均線高於 60 日均線")
    if slope and slope > 0:
        score += min(10, slope * 200)
        reasons.append("20 日均線斜率為正")
    if near_high and near_high >= 0.95:
        score += 8
        reasons.append("收盤價接近 20 日高點")
    return _bounded(score), reasons


def _score_momentum(row: pd.Series) -> tuple[float, list[str]]:
    score = 45.0
    reasons: list[str] = []
    return_5 = _value(row, "return_5")
    return_20 = _value(row, "return_20")
    volume_ratio = _value(row, "volume_ratio")

    if return_20 is not None:
        score += np.clip(return_20 * 180, -25, 25)
        if return_20 > 0.03:
            reasons.append("20 日報酬為正且具動能")
    if return_5 is not None:
        score += np.clip(return_5 * 120, -18, 18)
        if return_5 > 0.01:
            reasons.append("5 日短線動能為正")
    if volume_ratio is not None and volume_ratio > 1.2:
        score += min(12, (volume_ratio - 1) * 10)
        reasons.append("成交量高於 20 日均量")
    return _bounded(score), reasons


def _score_fundamental(row: pd.Series) -> tuple[float, list[str]]:
    score = 50.0
    reasons: list[str] = []
    used = 0

    pe_ratio = _value(row, "pe_ratio")
    if pe_ratio is not None and pe_ratio > 0:
        used += 1
        if pe_ratio <= 15:
            score += 14
            reasons.append("本益比低於 15")
        elif pe_ratio <= 25:
            score += 6
        else:
            score -= 8

    pb_ratio = _value(row, "pb_ratio")
    if pb_ratio is not None and pb_ratio > 0:
        used += 1
        if pb_ratio <= 2:
            score += 8
            reasons.append("股價淨值比低於 2")
        elif pb_ratio > 5:
            score -= 8

    dividend_yield = _value(row, "dividend_yield")
    if dividend_yield is not None:
        used += 1
        if dividend_yield >= 3:
            score += 10
            reasons.append("殖利率高於 3%")

    revenue_yoy = _value(row, "revenue_yoy")
    if revenue_yoy is not None:
        used += 1
        if revenue_yoy > 0:
            score += min(15, revenue_yoy / 2)
            reasons.append("營收年增率為正")
        else:
            score -= min(15, abs(revenue_yoy) / 2)

    if used == 0:
        reasons.append("基本面資料不足，採中性分數")
    return _bounded(score), reasons


def _score_chip(row: pd.Series) -> tuple[float, list[str]]:
    score = 50.0
    reasons: list[str] = []
    used = 0
    for column, label in [
        ("foreign_net_buy", "外資買超"),
        ("investment_trust_net_buy", "投信買超"),
        ("dealer_net_buy", "自營商買超"),
    ]:
        value = _value(row, column)
        if value is None:
            continue
        used += 1
        if value > 0:
            score += 8
            reasons.append(label)
        elif value < 0:
            score -= 6

    margin_change = _value(row, "margin_balance_change")
    if margin_change is not None:
        used += 1
        if margin_change < 0:
            score += 5
            reasons.append("融資餘額下降")
        elif margin_change > 0:
            score -= 4

    if used == 0:
        reasons.append("籌碼資料不足，採中性分數")
    return _bounded(score), reasons


def _score_risk(row: pd.Series) -> tuple[float, list[str]]:
    score = 65.0
    reasons: list[str] = []
    volatility = _value(row, "volatility_20")
    drawdown = _value(row, "drawdown_20")
    liquidity = _value(row, "liquidity_value")
    close = _value(row, "close")
    atr14 = _value(row, "atr14")

    if volatility is not None:
        if volatility <= 0.03:
            score += 12
            reasons.append("20 日波動低於 3%")
        elif volatility > 0.08:
            score -= 25
            reasons.append("20 日波動偏高")

    if drawdown is not None:
        if drawdown >= -0.08:
            score += 10
            reasons.append("距 20 日高點回落小於 8%")
        elif drawdown < -0.2:
            score -= 20

    if liquidity is not None:
        if liquidity >= 50_000_000:
            score += 8
            reasons.append("成交金額流動性充足")
        elif liquidity < 5_000_000:
            score -= 18

    if close and atr14:
        atr_pct = atr14 / close
        if atr_pct <= 0.04:
            score += 5
        elif atr_pct > 0.1:
            score -= 12
    return _bounded(score), reasons


def _value(row: pd.Series, key: str) -> float | None:
    if key not in row:
        return None
    value = row[key]
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(parsed) or np.isinf(parsed):
        return None
    return parsed


def _bounded(value: float) -> float:
    return float(np.clip(value, 0, 100))
