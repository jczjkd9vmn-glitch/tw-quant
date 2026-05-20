"""Market regime scoring for paper-entry guardrails.

The score is advisory and only used to decide whether new paper pending
orders may be created. It never changes exits or real orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from tw_quant.config import load_config
from tw_quant.data.database import create_db_engine, init_db, load_price_history


REGIME_COLUMNS = [
    "trade_date",
    "market_regime_score",
    "source",
    "twse_above_20ma",
    "twse_above_60ma",
    "tpex_above_20ma",
    "tpex_above_60ma",
    "market_return_5d",
    "market_return_20d",
    "market_above_20ma_ratio",
    "market_above_60ma_ratio",
    "warning",
]


@dataclass(frozen=True)
class MarketRegimeResult:
    trade_date: pd.Timestamp | None
    market_regime_score: float
    source: str
    output_path: Path | None = None
    warning: str = ""
    frame: pd.DataFrame | None = None


def evaluate_market_regime(
    engine: Engine | None = None,
    config: dict | None = None,
    config_path: str | Path = "config.yaml",
    trade_date: str | pd.Timestamp | None = None,
    reports_dir: str | Path | None = None,
) -> MarketRegimeResult:
    """Calculate market regime score from index data or local equal-weight fallback."""

    active_config = config or load_config(config_path)
    if engine is None:
        engine = create_db_engine(active_config["database"]["url"])
        init_db(engine)

    history = load_price_history(engine)
    if history.empty:
        return _result(None, 50.0, "EMPTY", "無可用價量資料，市場環境採中性分數", reports_dir)

    frame = history.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.dropna(subset=["trade_date"])
    if trade_date:
        target = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(target):
            frame = frame[frame["trade_date"] <= target].copy()
    if frame.empty:
        return _result(None, 50.0, "EMPTY", "指定日期前沒有可用價量資料，市場環境採中性分數", reports_dir)

    latest_date = frame["trade_date"].max()
    index_frame = _index_frame(frame)
    if not index_frame.empty:
        result = _score_index_regime(index_frame, latest_date)
        if result is not None:
            return _write_result(result, reports_dir)

    fallback_allowed = bool(
        active_config.get("market_regime", {}).get("fallback_to_equal_weight_market", True)
    )
    if not fallback_allowed:
        return _result(latest_date, 50.0, "EMPTY", "缺少指數資料且未啟用全市場 fallback", reports_dir)
    result = _score_equal_weight_regime(frame, latest_date)
    return _write_result(result, reports_dir)


def _score_index_regime(frame: pd.DataFrame, latest_date: pd.Timestamp) -> MarketRegimeResult | None:
    rows = []
    for market_name, keywords in {
        "twse": ["加權", "TAIEX", "TWII", "發行量加權"],
        "tpex": ["櫃買", "OTC", "TPEX"],
    }.items():
        subset = frame[
            frame["symbol"].astype(str).str.contains("|".join(keywords), case=False, na=False)
            | frame["name"].astype(str).str.contains("|".join(keywords), case=False, na=False)
        ].copy()
        if subset.empty:
            continue
        subset = subset.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
        if len(subset) < 20:
            continue
        close = pd.to_numeric(subset["close"], errors="coerce")
        latest_close = close.iloc[-1]
        rows.append(
            {
                "market": market_name,
                "above20": bool(latest_close >= close.rolling(20).mean().iloc[-1]),
                "above60": bool(len(close) >= 60 and latest_close >= close.rolling(60).mean().iloc[-1]),
                "return5": _period_return(close, 5),
                "return20": _period_return(close, 20),
            }
        )
    if not rows:
        return None
    score = 50.0
    for row in rows:
        score += 8 if row["above20"] else -8
        score += 8 if row["above60"] else -8
        score += 8 if row["return5"] > 0 else -6
        score += 10 if row["return20"] > 0 else -8
    score = _bound(score / max(len(rows), 1) + 25)
    twse = next((row for row in rows if row["market"] == "twse"), {})
    tpex = next((row for row in rows if row["market"] == "tpex"), {})
    output = pd.DataFrame(
        [
            {
                "trade_date": latest_date.strftime("%Y-%m-%d"),
                "market_regime_score": score,
                "source": "index",
                "twse_above_20ma": twse.get("above20"),
                "twse_above_60ma": twse.get("above60"),
                "tpex_above_20ma": tpex.get("above20"),
                "tpex_above_60ma": tpex.get("above60"),
                "market_return_5d": _mean([row["return5"] for row in rows]),
                "market_return_20d": _mean([row["return20"] for row in rows]),
                "market_above_20ma_ratio": "",
                "market_above_60ma_ratio": "",
                "warning": "",
            }
        ],
        columns=REGIME_COLUMNS,
    )
    return MarketRegimeResult(latest_date, score, "index", warning="", frame=output)


def _score_equal_weight_regime(frame: pd.DataFrame, latest_date: pd.Timestamp) -> MarketRegimeResult:
    prices = frame.copy()
    prices["symbol"] = prices["symbol"].astype(str).str.strip()
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["close"]).sort_values(["symbol", "trade_date"])
    metrics = []
    for _, group in prices.groupby("symbol"):
        group = group.sort_values("trade_date")
        close = group["close"].reset_index(drop=True)
        if len(close) < 6:
            continue
        latest_close = close.iloc[-1]
        metrics.append(
            {
                "return5": _period_return(close, 5),
                "return20": _period_return(close, 20),
                "above20": bool(len(close) >= 20 and latest_close >= close.rolling(20).mean().iloc[-1]),
                "above60": bool(len(close) >= 60 and latest_close >= close.rolling(60).mean().iloc[-1]),
            }
        )
    if not metrics:
        return _result(latest_date, 50.0, "equal_weight_market", "價量資料不足，市場環境採中性分數", None)

    market_return_5d = _mean([row["return5"] for row in metrics])
    market_return_20d = _mean([row["return20"] for row in metrics])
    above20 = _mean([1.0 if row["above20"] else 0.0 for row in metrics])
    above60 = _mean([1.0 if row["above60"] else 0.0 for row in metrics])
    score = 50.0
    score += 15 if market_return_5d > 0 else -10
    score += 20 if market_return_20d > 0 else -15
    score += 12 if above20 >= 0.55 else -12 if above20 < 0.45 else 0
    score += 10 if above60 >= 0.55 else -10 if above60 < 0.45 else 0
    score = _bound(score)
    warning = "缺少加權 / 櫃買指數資料，使用全市場等權報酬 fallback"
    output = pd.DataFrame(
        [
            {
                "trade_date": latest_date.strftime("%Y-%m-%d"),
                "market_regime_score": score,
                "source": "equal_weight_market",
                "twse_above_20ma": "",
                "twse_above_60ma": "",
                "tpex_above_20ma": "",
                "tpex_above_60ma": "",
                "market_return_5d": market_return_5d,
                "market_return_20d": market_return_20d,
                "market_above_20ma_ratio": above20,
                "market_above_60ma_ratio": above60,
                "warning": warning,
            }
        ],
        columns=REGIME_COLUMNS,
    )
    return MarketRegimeResult(latest_date, score, "equal_weight_market", warning=warning, frame=output)


def _index_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    text = frame["symbol"].astype(str) + " " + frame["name"].astype(str)
    mask = text.str.contains("加權|櫃買|TAIEX|TWII|TPEX|OTC|發行量加權", case=False, na=False)
    return frame[mask].copy()


def _write_result(result: MarketRegimeResult, reports_dir: str | Path | None) -> MarketRegimeResult:
    if reports_dir is None or result.frame is None or result.trade_date is None:
        return result
    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"market_regime_{result.trade_date.strftime('%Y%m%d')}.csv"
    result.frame.to_csv(path, index=False, encoding="utf-8-sig")
    return MarketRegimeResult(
        result.trade_date,
        result.market_regime_score,
        result.source,
        output_path=path,
        warning=result.warning,
        frame=result.frame,
    )


def _result(
    trade_date: pd.Timestamp | None,
    score: float,
    source: str,
    warning: str,
    reports_dir: str | Path | None,
) -> MarketRegimeResult:
    frame = pd.DataFrame(
        [
            {
                "trade_date": trade_date.strftime("%Y-%m-%d") if trade_date is not None else "",
                "market_regime_score": _bound(score),
                "source": source,
                "warning": warning,
            }
        ],
        columns=REGIME_COLUMNS,
    )
    return _write_result(MarketRegimeResult(trade_date, _bound(score), source, warning=warning, frame=frame), reports_dir)


def _period_return(close: pd.Series, days: int) -> float:
    clean = pd.to_numeric(close, errors="coerce").dropna().reset_index(drop=True)
    if len(clean) <= days or clean.iloc[-days - 1] == 0:
        return 0.0
    return float(clean.iloc[-1] / clean.iloc[-days - 1] - 1.0)


def _mean(values: list[float]) -> float:
    clean = [float(value) for value in values if pd.notna(value)]
    return round(sum(clean) / len(clean), 6) if clean else 0.0


def _bound(value: float) -> float:
    return round(max(0.0, min(100.0, float(value))), 2)
