"""Official material-event style risk classification."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


EVENT_COLUMNS = [
    "event_date",
    "stock_id",
    "stock_name",
    "title",
    "summary",
    "event_type",
    "event_sentiment",
    "event_risk_level",
]

POSITIVE_KEYWORDS = ["接單", "營收創高", "獲利成長", "法說展望佳", "擴產", "合約"]
NEGATIVE_KEYWORDS = ["虧損", "訴訟", "停工", "違約", "下修", "處分", "資安事件", "財報重編", "內控缺失"]
HIGH_RISK_KEYWORDS = ["訴訟", "停工", "違約", "資安事件", "財報重編", "內控缺失"]


def score_material_events_for_symbols(
    symbols: list[str],
    events_path: str | Path = "data/material_events.csv",
) -> pd.DataFrame:
    events = load_material_events(events_path)
    return pd.DataFrame([score_material_events(symbol, events) for symbol in symbols])


def score_material_events(stock_id: str, events: pd.DataFrame) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if events.empty:
        return _neutral(symbol)
    frame = events[events["stock_id"].astype(str).str.strip() == symbol].copy()
    if frame.empty:
        return _neutral(symbol)

    frame = frame.sort_values("event_date")
    latest = frame.iloc[-1]
    text = f"{latest.get('title', '')} {latest.get('summary', '')}"
    sentiment, risk_level, blocked, reason = classify_event_text(text)
    score = 50.0
    if sentiment == "positive":
        score += 5
    elif risk_level == "HIGH":
        score -= 35
    elif sentiment == "negative":
        score -= 18

    return {
        "stock_id": symbol,
        "event_score": _bounded(score),
        "event_risk_level": risk_level,
        "event_reason": reason,
        "event_blocked": bool(blocked),
    }


def classify_event_text(text: str) -> tuple[str, str, bool, str]:
    normalized = str(text)
    positive_hits = [keyword for keyword in POSITIVE_KEYWORDS if keyword in normalized]
    negative_hits = [keyword for keyword in NEGATIVE_KEYWORDS if keyword in normalized]
    high_hits = [keyword for keyword in HIGH_RISK_KEYWORDS if keyword in normalized]
    if high_hits:
        return "negative", "HIGH", True, "高風險重大訊息：" + "、".join(high_hits)
    if negative_hits:
        return "negative", "MEDIUM", False, "重大利空訊息：" + "、".join(negative_hits)
    if positive_hits:
        return "positive", "LOW", False, "重大利多訊息：" + "、".join(positive_hits)
    return "neutral", "NONE", False, "近期無重大事件風險"


def load_material_events(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame(columns=EVENT_COLUMNS)
    frame = pd.read_csv(csv_path, dtype={"stock_id": str})
    for column in EVENT_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
    return frame[EVENT_COLUMNS].copy()


def _neutral(stock_id: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "event_score": 50.0,
        "event_risk_level": "NONE",
        "event_reason": "近期無重大事件風險",
        "event_blocked": False,
    }


def _bounded(value: float) -> float:
    return max(0.0, min(100.0, round(float(value), 2)))
