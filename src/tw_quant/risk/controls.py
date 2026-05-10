"""Risk management and data-quality gates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from tw_quant.data.exceptions import DataQualityError


@dataclass(frozen=True)
class RiskConfig:
    initial_equity: float = 1_000_000
    max_position_pct: float = 0.10
    max_portfolio_exposure_pct: float = 0.60
    max_trade_risk_pct: float = 0.01
    default_stop_loss_pct: float = 0.08
    min_liquidity_value: float = 5_000_000
    max_volatility_20: float = 0.08

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> "RiskConfig":
        return cls(
            initial_equity=float(values.get("initial_equity", cls.initial_equity)),
            max_position_pct=float(values.get("max_position_pct", cls.max_position_pct)),
            max_portfolio_exposure_pct=float(
                values.get("max_portfolio_exposure_pct", cls.max_portfolio_exposure_pct)
            ),
            max_trade_risk_pct=float(values.get("max_trade_risk_pct", cls.max_trade_risk_pct)),
            default_stop_loss_pct=float(
                values.get("default_stop_loss_pct", cls.default_stop_loss_pct)
            ),
            min_liquidity_value=float(values.get("min_liquidity_value", cls.min_liquidity_value)),
            max_volatility_20=float(values.get("max_volatility_20", cls.max_volatility_20)),
        )


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reasons: list[str]
    suggested_position_pct: float
    stop_loss: float


class RiskManager:
    def __init__(self, config: RiskConfig | None = None):
        self.config = config or RiskConfig()

    def validate_price_data(self, prices: pd.DataFrame) -> None:
        required = ["trade_date", "symbol", "open", "high", "low", "close", "volume"]
        missing = [column for column in required if column not in prices.columns]
        if missing:
            raise DataQualityError(f"missing price columns: {', '.join(missing)}")
        if prices.empty:
            raise DataQualityError("price data is empty")

        frame = prices.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        for column in ["open", "high", "low", "close", "volume"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        if frame[["trade_date", "symbol", "open", "high", "low", "close", "volume"]].isna().any().any():
            raise DataQualityError("price data contains null or non-numeric values")
        if frame.duplicated(["trade_date", "symbol"]).any():
            raise DataQualityError("price data contains duplicate trade_date/symbol rows")
        if (frame[["open", "high", "low", "close"]] <= 0).any().any():
            raise DataQualityError("price data contains non-positive OHLC values")
        if (frame["volume"] < 0).any():
            raise DataQualityError("price data contains negative volume")
        if (frame["high"] < frame[["open", "close", "low"]].max(axis=1)).any():
            raise DataQualityError("price data has high lower than open/close/low")
        if (frame["low"] > frame[["open", "close", "high"]].min(axis=1)).any():
            raise DataQualityError("price data has low higher than open/close/high")

    def calculate_stop_loss(self, row: pd.Series | dict[str, Any]) -> float:
        close = float(row.get("close", 0))
        atr14 = _safe_float(row.get("atr14"))
        if close <= 0:
            return 0.0
        default_stop = close * (1 - self.config.default_stop_loss_pct)
        atr_stop = close - (2 * atr14) if atr14 and atr14 > 0 else default_stop
        stop = max(default_stop, atr_stop)
        if stop >= close:
            stop = default_stop
        return round(stop, 2)

    def suggest_position_pct(self, row: pd.Series | dict[str, Any], stop_loss: float) -> float:
        close = float(row.get("close", 0))
        total_score = _safe_float(row.get("total_score")) or 50.0
        if close <= 0 or stop_loss <= 0 or stop_loss >= close:
            return 0.0

        risk_per_share_pct = (close - stop_loss) / close
        if risk_per_share_pct <= 0:
            return 0.0

        score_scale = min(max(total_score / 100, 0.3), 1.0)
        raw_position = (self.config.max_trade_risk_pct * score_scale) / risk_per_share_pct
        return round(max(0.0, min(raw_position, self.config.max_position_pct)), 4)

    def evaluate_candidate(
        self,
        row: pd.Series | dict[str, Any],
        current_exposure_pct: float = 0.0,
    ) -> RiskDecision:
        reasons: list[str] = []
        close = float(row.get("close", 0))
        stop_loss = float(row.get("stop_loss") or self.calculate_stop_loss(row))
        suggested_position_pct = float(
            row.get("suggested_position_pct") or self.suggest_position_pct(row, stop_loss)
        )

        if str(row.get("data_quality_status", "OK")) != "OK":
            reasons.append("資料品質不是 OK，禁止交易")
        if close <= 0:
            reasons.append("收盤價無效")
        if stop_loss <= 0 or stop_loss >= close:
            reasons.append("停損價無效")
        if suggested_position_pct <= 0:
            reasons.append("建議部位為 0")
        if suggested_position_pct > self.config.max_position_pct:
            reasons.append("單一持股部位超過上限")
        if current_exposure_pct + suggested_position_pct > self.config.max_portfolio_exposure_pct:
            reasons.append("投資組合總曝險超過上限")

        liquidity_value = _safe_float(row.get("liquidity_value"))
        if liquidity_value is not None and liquidity_value < self.config.min_liquidity_value:
            reasons.append("流動性不足")

        volatility = _safe_float(row.get("volatility_20"))
        if volatility is not None and volatility > self.config.max_volatility_20:
            reasons.append("20 日波動過高")

        if not reasons:
            reasons.append("通過資料品質、停損、部位、流動性與波動檢查")
        return RiskDecision(
            allowed=len(reasons) == 1 and reasons[0].startswith("通過"),
            reasons=reasons,
            suggested_position_pct=round(suggested_position_pct, 4),
            stop_loss=round(stop_loss, 2),
        )

    def apply_candidate_controls(self, scores: pd.DataFrame) -> pd.DataFrame:
        frame = scores.copy()
        if frame.empty:
            return frame

        risk_pass: list[bool] = []
        risk_reasons: list[str] = []
        position_pct: list[float] = []
        stop_losses: list[float] = []
        current_exposure = 0.0

        for _, row in frame.iterrows():
            decision = self.evaluate_candidate(row, current_exposure_pct=current_exposure)
            allowed = bool(row.get("is_candidate", False)) and decision.allowed
            if allowed:
                current_exposure += decision.suggested_position_pct
            risk_pass.append(allowed)
            risk_reasons.append("；".join(decision.reasons))
            position_pct.append(decision.suggested_position_pct if allowed else 0.0)
            stop_losses.append(decision.stop_loss)

        frame["risk_pass"] = risk_pass
        frame["risk_reasons"] = risk_reasons
        frame["suggested_position_pct"] = position_pct
        frame["stop_loss"] = stop_losses
        return frame


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(parsed) or np.isinf(parsed):
        return None
    return parsed
