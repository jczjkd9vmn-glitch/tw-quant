"""Local factor providers derived from SQLite OHLCV data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from tw_quant.config import load_config
from tw_quant.data.database import create_db_engine, load_price_history
from tw_quant.data.trading_calendar import filter_trading_days
from tw_quant.data_sources.base import ProviderResult
from tw_quant.enrichment.industry import load_industry_map
from tw_quant.risk.controls import detect_price_jumps


LIQUIDITY_DERIVED_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "avg_volume_20d",
    "avg_turnover_20d",
    "latest_volume",
    "latest_turnover",
    "turnover_ratio_20d",
    "liquidity_score",
    "slippage_risk_score",
    "liquidity_warning",
]

SECTOR_STRENGTH_DERIVED_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "industry",
    "sub_industry",
    "industry_source",
    "sector_strength_mode",
    "stock_return_5d",
    "stock_return_20d",
    "market_return_5d",
    "market_return_20d",
    "sector_return_5d",
    "sector_return_20d",
    "relative_strength_5d",
    "relative_strength_20d",
    "sector_strength_score",
    "sector_strength_rank",
    "sector_strength_warning",
]


@dataclass(frozen=True)
class LocalFactorConfig:
    liquidity_window: int = 20
    low_turnover_threshold: float = 5_000_000.0
    sector_short_window: int = 5
    sector_long_window: int = 20
    fallback_to_market_relative: bool = True


class LocalDerivedProvider:
    """Builds local factors from the existing daily price SQLite table.

    This provider intentionally does not call external APIs. It only derives
    report/scoring helper fields from OHLCV data already stored by the system.
    """

    def __init__(
        self,
        *,
        engine: Engine | None = None,
        database_url: str | None = None,
        config: dict[str, Any] | None = None,
        config_path: str | Path = "config.yaml",
    ) -> None:
        self.config = config or load_config(config_path)
        self.config_path = Path(config_path)
        self.factor_config = self._parse_factor_config(self.config)
        db_url = database_url or self.config.get("database", {}).get("url", "sqlite:///data/tw_quant.sqlite")
        self.engine = engine or create_db_engine(db_url)

    @staticmethod
    def _parse_factor_config(config: dict[str, Any]) -> LocalFactorConfig:
        local = config.get("local_factors", {})
        liquidity = local.get("liquidity", {})
        sector = local.get("sector_strength", {})
        return LocalFactorConfig(
            liquidity_window=int(liquidity.get("window", 20)),
            low_turnover_threshold=float(liquidity.get("low_turnover_threshold", 5_000_000)),
            sector_short_window=int(sector.get("short_window", 5)),
            sector_long_window=int(sector.get("long_window", 20)),
            fallback_to_market_relative=bool(sector.get("fallback_to_market_relative", True)),
        )

    def fetch_liquidity(self, as_of: str | None = None) -> ProviderResult:
        history = self._load_history(as_of)
        history_warning = str(history.attrs.get("price_quality_warning", ""))
        if history.empty:
            return self._empty_result(
                "liquidity",
                LIQUIDITY_DERIVED_COLUMNS,
                "SQLite 價量資料不足，無法產生流動性分數",
                as_of,
            )

        window = self.factor_config.liquidity_window
        frames: list[dict[str, Any]] = []
        latest_date = history["trade_date"].max()
        for stock_id, group in history.groupby("stock_id", sort=False):
            group = group.sort_values("trade_date").tail(window)
            latest = group.iloc[-1]
            avg_volume = pd.to_numeric(group["volume"], errors="coerce").mean()
            avg_turnover = pd.to_numeric(group["turnover_value"], errors="coerce").mean()
            latest_turnover = self._safe_float(latest.get("turnover_value"))
            latest_volume = self._safe_float(latest.get("volume"))
            ratio = latest_turnover / avg_turnover if avg_turnover and avg_turnover > 0 else np.nan
            score = self._liquidity_score(avg_turnover)
            slippage_score = self._slippage_risk_score(avg_turnover)
            warnings = []
            if len(group) < window:
                warnings.append(f"價量資料少於 {window} 日")
            if avg_turnover and avg_turnover < self.factor_config.low_turnover_threshold:
                warnings.append("流動性偏低，短線進出可能有滑價風險")
            frames.append(
                {
                    "trade_date": self._date_text(latest_date),
                    "stock_id": str(stock_id),
                    "stock_name": latest.get("stock_name", ""),
                    "avg_volume_20d": avg_volume,
                    "avg_turnover_20d": avg_turnover,
                    "latest_volume": latest_volume,
                    "latest_turnover": latest_turnover,
                    "turnover_ratio_20d": ratio,
                    "liquidity_score": score,
                    "slippage_risk_score": slippage_score,
                    "liquidity_warning": "；".join(warnings),
                }
            )

        data = pd.DataFrame(frames, columns=LIQUIDITY_DERIVED_COLUMNS)
        warning = ""
        if data["liquidity_warning"].fillna("").ne("").any():
            warning = "部分股票流動性偏低或價量資料不足"
        if history_warning:
            warning = "；".join(part for part in [warning, history_warning] if part)
        return self._ok_result("liquidity", data, warning, as_of, latest_date)

    def fetch_sector_strength(self, as_of: str | None = None) -> ProviderResult:
        history = self._load_history(as_of)
        history_warning = str(history.attrs.get("price_quality_warning", ""))
        if history.empty:
            return self._empty_result(
                "sector_strength",
                SECTOR_STRENGTH_DERIVED_COLUMNS,
                "SQLite 價格資料不足，無法產生產業 / 相對強弱分數",
                as_of,
            )

        short_window = self.factor_config.sector_short_window
        long_window = self.factor_config.sector_long_window
        latest_date = history["trade_date"].max()
        rows: list[dict[str, Any]] = []
        for stock_id, group in history.groupby("stock_id", sort=False):
            group = group.sort_values("trade_date")
            latest = group.iloc[-1]
            industry = self._clean_text(latest.get("industry"))
            has_stock_industry = bool(industry and industry != "全市場")
            mode = "industry_relative" if has_stock_industry else "market_relative_fallback"
            stock_return_5d = self._period_return(group, short_window)
            stock_return_20d = self._period_return(group, long_window)
            rows.append(
                {
                    "trade_date": self._date_text(latest_date),
                    "stock_id": str(stock_id),
                    "stock_name": latest.get("stock_name", ""),
                    "industry": industry if has_stock_industry else "全市場",
                    "sub_industry": self._clean_text(latest.get("sub_industry")),
                    "industry_source": self._clean_text(latest.get("industry_source")),
                    "sector_strength_mode": mode,
                    "stock_return_5d": stock_return_5d,
                    "stock_return_20d": stock_return_20d,
                }
            )

        data = pd.DataFrame(rows)
        market_return_5d = pd.to_numeric(data["stock_return_5d"], errors="coerce").mean()
        market_return_20d = pd.to_numeric(data["stock_return_20d"], errors="coerce").mean()
        data["market_return_5d"] = market_return_5d
        data["market_return_20d"] = market_return_20d
        data["sector_return_5d"] = market_return_5d
        data["sector_return_20d"] = market_return_20d
        industry_mask = data["sector_strength_mode"] == "industry_relative"
        fallback_mask = ~industry_mask
        if industry_mask.any():
            data.loc[industry_mask, "sector_return_5d"] = data.loc[industry_mask].groupby("industry")[
                "stock_return_5d"
            ].transform("mean")
            data.loc[industry_mask, "sector_return_20d"] = data.loc[industry_mask].groupby("industry")[
                "stock_return_20d"
            ].transform("mean")
        data["relative_strength_5d"] = data["stock_return_5d"] - data["sector_return_5d"]
        data["relative_strength_20d"] = data["stock_return_20d"] - data["sector_return_20d"]
        data["sector_strength_warning"] = ""
        data.loc[fallback_mask, "sector_strength_warning"] = "缺少產業分類，使用全市場相對強弱"

        data["sector_strength_rank"] = (
            data["relative_strength_20d"].rank(method="min", ascending=False, na_option="bottom").astype("Int64")
        )
        data["sector_strength_score"] = data.apply(self._sector_strength_score, axis=1)
        data = data[SECTOR_STRENGTH_DERIVED_COLUMNS]
        warnings = []
        status = "OK"
        if fallback_mask.any():
            fallback_count = int(fallback_mask.sum())
            warnings.append(f"{fallback_count} 檔缺少產業分類，使用全市場相對強弱")
            status = "OK_WITH_FALLBACK"
        if data[["stock_return_5d", "stock_return_20d"]].isna().all(axis=None):
            warnings.append("價格資料不足，產業 / 相對強弱分數採中性")
        if history_warning:
            warnings.append(history_warning)
            if status == "OK":
                status = "OK_WITH_FALLBACK"
        warning = "；".join(warnings)
        return self._ok_result("sector_strength", data, warning, as_of, latest_date, status=status)

    def _load_history(self, as_of: str | None) -> pd.DataFrame:
        history = load_price_history(self.engine, end_date=as_of)
        if history.empty:
            return history
        history = filter_trading_days(history)
        history = history.rename(columns={"symbol": "stock_id", "name": "stock_name"})
        history["trade_date"] = pd.to_datetime(history["trade_date"], errors="coerce")
        history = history.dropna(subset=["trade_date", "stock_id", "close"])
        for column in ["open", "high", "low", "close", "volume", "turnover"]:
            if column in history.columns:
                history[column] = pd.to_numeric(history[column], errors="coerce")
        if "turnover" in history.columns and history["turnover"].notna().any():
            history["turnover_value"] = history["turnover"]
        else:
            history["turnover_value"] = np.nan
        estimated = pd.to_numeric(history["close"], errors="coerce") * pd.to_numeric(history["volume"], errors="coerce")
        history["turnover_value"] = history["turnover_value"].where(history["turnover_value"].notna(), estimated)
        jumps = detect_price_jumps(
            history.rename(columns={"stock_id": "symbol"})[["trade_date", "symbol", "close"]],
            max_abs_daily_return=0.50,
        )
        if not jumps.empty:
            jump_keys = set(zip(jumps["symbol"].astype(str), jumps["trade_date"].astype(str)))
            keys = list(zip(history["stock_id"].astype(str), history["trade_date"].dt.strftime("%Y-%m-%d")))
            history = history[[key not in jump_keys for key in keys]].copy()
            history.attrs["price_quality_warning"] = f"排除 {len(jumps)} 筆跨日價格跳動異常資料"
        history = self._merge_industry_map(history)
        return history.sort_values(["stock_id", "trade_date"])

    def _merge_industry_map(self, history: pd.DataFrame) -> pd.DataFrame:
        data_dir = self.config_path.resolve().parent / "data"
        industry_map = load_industry_map(data_dir=data_dir, config_path=self.config_path)
        if industry_map.empty or "stock_id" not in industry_map.columns or "industry" not in industry_map.columns:
            return history
        lookup = industry_map.drop_duplicates("stock_id", keep="last").set_index("stock_id")
        result = history.copy()
        result["stock_id"] = result["stock_id"].astype(str).str.strip()
        for column, source_column in [
            ("industry", "industry"),
            ("sub_industry", "sub_industry"),
            ("industry_source", "source"),
        ]:
            if source_column not in lookup.columns:
                continue
            mapped = result["stock_id"].map(lookup[source_column])
            if column not in result.columns:
                result[column] = mapped
            else:
                current = result[column].fillna("").astype(str).str.strip()
                result[column] = result[column].where(current != "", mapped)
        return result

    @staticmethod
    def _liquidity_score(avg_turnover: float | None) -> float:
        value = LocalDerivedProvider._safe_float(avg_turnover)
        if not value or np.isnan(value) or value <= 0:
            return 0.0
        if value >= 100_000_000:
            return 92.0
        if value >= 50_000_000:
            return 75.0
        if value >= 20_000_000:
            return 60.0
        if value >= 5_000_000:
            return 50.0
        return 35.0

    @staticmethod
    def _slippage_risk_score(avg_turnover: float | None) -> float:
        value = LocalDerivedProvider._safe_float(avg_turnover)
        if not value or np.isnan(value) or value <= 0:
            return 20.0
        if value >= 100_000_000:
            return 90.0
        if value >= 50_000_000:
            return 80.0
        if value >= 20_000_000:
            return 65.0
        if value >= 5_000_000:
            return 50.0
        return 30.0

    @staticmethod
    def _period_return(group: pd.DataFrame, window: int) -> float:
        if len(group) <= window:
            return np.nan
        latest_close = LocalDerivedProvider._safe_float(group.iloc[-1]["close"])
        base_close = LocalDerivedProvider._safe_float(group.iloc[-(window + 1)]["close"])
        if not latest_close or not base_close or np.isnan(latest_close) or np.isnan(base_close) or base_close == 0:
            return np.nan
        return latest_close / base_close - 1.0

    @staticmethod
    def _sector_strength_score(row: pd.Series) -> float:
        score = 50.0
        rs20 = LocalDerivedProvider._safe_float(row.get("relative_strength_20d"))
        rs5 = LocalDerivedProvider._safe_float(row.get("relative_strength_5d"))
        rank = LocalDerivedProvider._safe_float(row.get("sector_strength_rank"))
        if not np.isnan(rs20):
            if rs20 > 0.05:
                score += 20
            elif rs20 > 0:
                score += 10
            elif rs20 < -0.05:
                score -= 15
            elif rs20 < 0:
                score -= 8
        if not np.isnan(rs5):
            if rs5 > 0.02:
                score += 8
            elif rs5 > 0:
                score += 4
            elif rs5 < -0.02:
                score -= 6
        if not np.isnan(rank) and rank <= 20:
            score += 8
        return float(max(0, min(100, score)))

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            if value is None or value == "":
                return np.nan
            return float(value)
        except (TypeError, ValueError):
            return np.nan

    @staticmethod
    def _clean_text(value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except TypeError:
            pass
        text = str(value).strip()
        if text.lower() in {"nan", "none", "nat", "<na>"}:
            return ""
        return text

    @staticmethod
    def _date_text(value: Any) -> str:
        if pd.isna(value):
            return ""
        return pd.to_datetime(value).strftime("%Y-%m-%d")

    def _ok_result(
        self,
        source_name: str,
        data: pd.DataFrame,
        warning: str,
        requested_period: str | None,
        latest_date: Any,
        *,
        status: str = "OK",
    ) -> ProviderResult:
        actual_period = self._date_text(latest_date)
        return ProviderResult(
            source_name=source_name,
            status=status,
            data=data,
            warning=warning,
            error_message="",
            requested_period=requested_period or actual_period,
            actual_period=actual_period,
            latest_available_period=actual_period,
            source_url_or_name="SQLite local OHLCV data",
            is_real_data=True,
            is_mock=False,
            is_stale=False,
            data_age_days=self._data_age_days(requested_period, actual_period),
            coverage_ratio=1.0 if not data.empty else 0.0,
            affected_symbols_count=int(len(data)),
        )

    def _empty_result(
        self,
        source_name: str,
        columns: list[str],
        warning: str,
        requested_period: str | None,
    ) -> ProviderResult:
        return ProviderResult(
            source_name=source_name,
            status="EMPTY",
            data=pd.DataFrame(columns=columns),
            warning=warning,
            error_message="",
            requested_period=requested_period or "",
            actual_period="",
            latest_available_period="",
            source_url_or_name="SQLite local OHLCV data",
            is_real_data=True,
            is_mock=False,
            is_stale=False,
            data_age_days=None,
            coverage_ratio=0.0,
            affected_symbols_count=0,
        )

    @staticmethod
    def _data_age_days(requested_period: str | None, actual_period: str) -> int | None:
        if not requested_period or not actual_period:
            return None
        try:
            requested = datetime.strptime(str(requested_period).replace("-", ""), "%Y%m%d")
            actual = datetime.strptime(str(actual_period).replace("-", ""), "%Y%m%d")
        except ValueError:
            return None
        return max(0, (requested.date() - actual.date()).days)
