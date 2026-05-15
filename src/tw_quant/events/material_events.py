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
    "event_keywords",
    "event_warning",
]

POSITIVE_KEYWORDS = [
    "營收創高",
    "獲利成長",
    "接單增加",
    "擴產",
    "股利增加",
    "毛利率改善",
    "客戶需求強",
]
NEGATIVE_KEYWORDS = [
    "檢調",
    "虧損",
    "下修",
    "砍單",
    "調降財測",
    "財報不如預期",
    "毛利率下滑",
    "庫存過高",
    "停止交易",
    "處分",
    "訴訟",
    "減資",
    "資安事件",
    "內控缺失",
]
HIGH_RISK_KEYWORDS = ["檢調", "停止交易", "處分", "訴訟", "減資", "財報不如預期", "資安事件", "內控缺失"]


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
    text = f"{latest.get('title', '')} {latest.get('summary', '')} {latest.get('event_keywords', '')}"
    sentiment, risk_level, blocked, reason, keywords = classify_event_text(text)
    score = 50.0
    if sentiment == "positive":
        score += 8
    elif risk_level == "HIGH":
        score -= 35
    elif sentiment == "negative":
        score -= 18
    return {
        "stock_id": symbol,
        "event_score": _bounded(score),
        "event_risk_score": _bounded(score),
        "event_risk_level": risk_level,
        "event_reason": reason,
        "event_keywords": "；".join(keywords),
        "event_warning": reason if risk_level in {"HIGH", "MEDIUM"} else "",
        "event_blocked": bool(blocked),
    }


def classify_event_text(text: str) -> tuple[str, str, bool, str, list[str]]:
    normalized = str(text)
    positive_hits = [keyword for keyword in POSITIVE_KEYWORDS if keyword in normalized]
    negative_hits = [keyword for keyword in NEGATIVE_KEYWORDS if keyword in normalized]
    high_hits = [keyword for keyword in HIGH_RISK_KEYWORDS if keyword in normalized]
    if high_hits:
        return "negative", "HIGH", True, "重大負面事件：" + "；".join(high_hits), high_hits
    if negative_hits:
        return "negative", "MEDIUM", False, "負面重大訊息：" + "；".join(negative_hits), negative_hits
    if positive_hits:
        return "positive", "LOW", False, "正面重大訊息：" + "；".join(positive_hits), positive_hits
    return "neutral", "NONE", False, "無重大事件資料", []


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
        "event_risk_score": 50.0,
        "event_risk_level": "NONE",
        "event_reason": "無重大事件資料",
        "event_keywords": "",
        "event_warning": "",
        "event_blocked": False,
    }


def _bounded(value: float) -> float:
    return max(0.0, min(100.0, round(float(value), 2)))
