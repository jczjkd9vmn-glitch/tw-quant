"""Official-data auxiliary factor scoring.

These scores are report and risk-warning inputs. They do not create trades by
themselves.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


CREDIT_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "margin_balance",
    "margin_change",
    "short_balance",
    "short_change",
    "securities_lending_sell_volume",
    "securities_lending_balance",
]

ATTENTION_DISPOSITION_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "is_attention_stock",
    "attention_reason",
    "is_disposition_stock",
    "disposition_start_date",
    "disposition_end_date",
    "disposition_reason",
]

SECTOR_STRENGTH_COLUMNS = [
    "trade_date",
    "stock_id",
    "industry",
    "stock_return_5d",
    "stock_return_20d",
    "market_return_5d",
    "market_return_20d",
    "sector_return_5d",
    "sector_return_20d",
    "relative_strength_5d",
    "relative_strength_20d",
    "sector_strength_rank",
]

LIQUIDITY_COLUMNS = [
    "trade_date",
    "stock_id",
    "avg_volume_20d",
    "avg_turnover_20d",
    "intraday_trading_ratio",
]


def score_credit_for_symbols(
    symbols: list[str],
    credit_path: str | Path = "data/margin_short.csv",
    context: pd.DataFrame | None = None,
) -> pd.DataFrame:
    credit = load_credit(credit_path)
    context_lookup = _context_lookup(context)
    return pd.DataFrame([score_credit(symbol, credit, context_lookup.get(str(symbol))) for symbol in symbols])


def score_credit(stock_id: str, credit: pd.DataFrame, context: pd.Series | None = None) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if credit.empty:
        return _neutral_credit(symbol, "信用交易與借券資料不足，採中性分數")
    frame = credit[credit["stock_id"].astype(str).str.strip() == symbol].sort_values("trade_date").copy()
    if frame.empty:
        return _neutral_credit(symbol, "信用交易與借券資料不足，採中性分數")

    latest = frame.iloc[-1]
    margin_balance = _number(latest.get("margin_balance"))
    margin_change = _number(latest.get("margin_change"))
    short_balance = _number(latest.get("short_balance"))
    short_change = _number(latest.get("short_change"))
    lending_sell = _number(latest.get("securities_lending_sell_volume"))
    lending_balance = _number(latest.get("securities_lending_balance"))
    institutional_total = _context_number(context, "total_institutional_net_buy", "institutional_total_net_buy")
    return_5 = _context_number(context, "return_5")

    score = 50.0
    warnings: list[str] = []
    flags: list[str] = []
    if margin_change is not None:
        if margin_change > 0 and (return_5 is None or return_5 <= 0):
            score -= 10
            flags.append("融資連增但股價不漲")
        if margin_change >= max(1000.0, (margin_balance or 0.0) * 0.15):
            if institutional_total is not None and institutional_total < 0:
                score -= 18
                flags.append("融資暴增但法人賣超")
            else:
                score -= 8
                warnings.append("融資明顯增加")
        elif margin_change > 0 and institutional_total is not None and institutional_total > 0 and (return_5 or 0) > 0:
            score += 8
            warnings.append("股價上漲且法人買超，融資小增")
    if lending_sell is not None and lending_sell >= max(1000.0, (lending_balance or 0.0) * 0.2):
        score -= 18
        flags.append("借券賣出暴增")
    if margin_balance is not None and margin_balance >= 50_000:
        score -= 8
        flags.append("融資使用偏高")

    return {
        "stock_id": symbol,
        "margin_balance": margin_balance,
        "margin_change": margin_change,
        "short_balance": short_balance,
        "short_change": short_change,
        "securities_lending_sell_volume": lending_sell,
        "securities_lending_balance": lending_balance,
        "credit_score": _bounded(score),
        "margin_usage_warning": "；".join(warnings),
        "short_selling_warning": "；".join(flag for flag in flags if "借券" in flag),
        "credit_reason": "；".join(warnings + flags) or "信用交易結構中性",
        "credit_risk_flags": "；".join(flags),
    }


def score_attention_disposition_for_symbols(
    symbols: list[str],
    attention_path: str | Path = "data/attention_disposition.csv",
    config: dict | None = None,
) -> pd.DataFrame:
    events = load_attention_disposition(attention_path)
    active_config = config or {}
    return pd.DataFrame([score_attention_disposition(symbol, events, active_config) for symbol in symbols])


def score_attention_disposition(stock_id: str, events: pd.DataFrame, config: dict | None = None) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if events.empty:
        return _neutral_attention(symbol)
    frame = events[events["stock_id"].astype(str).str.strip() == symbol].sort_values("trade_date").copy()
    if frame.empty:
        return _neutral_attention(symbol)

    latest = frame.iloc[-1]
    is_attention = _to_bool(latest.get("is_attention_stock"))
    is_disposition = _to_bool(latest.get("is_disposition_stock"))
    block_disposition = bool((config or {}).get("block_disposition_stock", True))
    block_attention = bool((config or {}).get("block_attention_stock", False))
    blocked = (is_disposition and block_disposition) or (is_attention and block_attention)
    flags: list[str] = []
    if is_attention:
        flags.append("注意股")
    if is_disposition:
        flags.append("處置股")
    risk_level = "HIGH" if blocked else "MEDIUM" if flags else "NONE"
    score = 20.0 if blocked else 35.0 if flags else 50.0
    reason = "；".join(flags) if flags else "無注意股或處置股資料"
    return {
        "stock_id": symbol,
        "is_attention_stock": is_attention,
        "attention_reason": str(latest.get("attention_reason", "") or ""),
        "is_disposition_stock": is_disposition,
        "disposition_start_date": _date_or_blank(latest.get("disposition_start_date")),
        "disposition_end_date": _date_or_blank(latest.get("disposition_end_date")),
        "disposition_reason": str(latest.get("disposition_reason", "") or ""),
        "event_risk_score": score,
        "event_risk_level": risk_level,
        "event_blocked": blocked,
        "attention_disposition_reason": reason,
        "event_risk_flags": "；".join(flags),
    }


def score_sector_strength_for_symbols(
    symbols: list[str],
    sector_path: str | Path = "data/sector_strength.csv",
) -> pd.DataFrame:
    sector = load_sector_strength(sector_path)
    return pd.DataFrame([score_sector_strength(symbol, sector) for symbol in symbols])


def score_sector_strength(stock_id: str, sector: pd.DataFrame) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if sector.empty:
        return _neutral_sector(symbol, "產業相對強弱資料不足，採中性分數")
    frame = sector[sector["stock_id"].astype(str).str.strip() == symbol].sort_values("trade_date").copy()
    if frame.empty:
        return _neutral_sector(symbol, "產業相對強弱資料不足，採中性分數")
    latest = frame.iloc[-1]
    rs5 = _number(latest.get("relative_strength_5d"))
    rs20 = _number(latest.get("relative_strength_20d"))
    rank = _number(latest.get("sector_strength_rank"))
    sector20 = _number(latest.get("sector_return_20d"))
    stock20 = _number(latest.get("stock_return_20d"))

    score = 50.0
    reasons: list[str] = []
    if rs20 is not None and rs20 > 0:
        score += 15
        reasons.append("個股 20 日強於大盤")
    if rs5 is not None and rs5 > 0:
        score += 5
        reasons.append("短線相對強勢")
    if rank is not None and (rank <= 20 or rank <= 0.2):
        score += 10
        reasons.append("所屬產業排名前 20%")
    if sector20 is not None and stock20 is not None:
        if sector20 > 0 and stock20 < sector20:
            reasons.append("產業強但個股較弱，列入觀察")
        if sector20 < 0 and stock20 > 0:
            score -= 5
            reasons.append("個股強但產業偏弱，降低信心")
    return {
        "stock_id": symbol,
        "industry": str(latest.get("industry", "") or ""),
        "stock_return_5d": _number(latest.get("stock_return_5d")),
        "stock_return_20d": stock20,
        "market_return_5d": _number(latest.get("market_return_5d")),
        "market_return_20d": _number(latest.get("market_return_20d")),
        "sector_return_5d": _number(latest.get("sector_return_5d")),
        "sector_return_20d": sector20,
        "relative_strength_5d": rs5,
        "relative_strength_20d": rs20,
        "sector_strength_rank": rank,
        "sector_strength_score": _bounded(score),
        "sector_strength_reason": "；".join(reasons) or "產業相對強弱中性",
    }


def score_liquidity_for_symbols(
    symbols: list[str],
    liquidity_path: str | Path = "data/liquidity.csv",
) -> pd.DataFrame:
    liquidity = load_liquidity(liquidity_path)
    return pd.DataFrame([score_liquidity(symbol, liquidity) for symbol in symbols])


def score_liquidity(stock_id: str, liquidity: pd.DataFrame) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if liquidity.empty:
        return _neutral_liquidity(symbol, "流動性資料不足，採中性分數")
    frame = liquidity[liquidity["stock_id"].astype(str).str.strip() == symbol].sort_values("trade_date").copy()
    if frame.empty:
        return _neutral_liquidity(symbol, "流動性資料不足，採中性分數")
    latest = frame.iloc[-1]
    avg_volume = _number(latest.get("avg_volume_20d"))
    avg_turnover = _number(latest.get("avg_turnover_20d"))
    intraday_ratio = _number(latest.get("intraday_trading_ratio"))
    score = 50.0
    warning = ""
    flags: list[str] = []
    if avg_turnover is not None:
        if avg_turnover < 20_000_000:
            score = 20.0
            warning = "20 日均成交金額低於 2000 萬，不建議進場"
            flags.append("流動性不足")
        elif avg_turnover < 50_000_000:
            score = 40.0
            warning = "20 日均成交金額低於 5000 萬，降低優先度"
            flags.append("流動性偏低")
        else:
            score += 12
    if intraday_ratio is not None and intraday_ratio > 2.5:
        score -= 8
        flags.append("成交量異常放大")
    return {
        "stock_id": symbol,
        "avg_volume_20d": avg_volume,
        "avg_turnover_20d": avg_turnover,
        "intraday_trading_ratio": intraday_ratio,
        "liquidity_score": _bounded(score),
        "liquidity_warning": warning,
        "slippage_risk_score": _bounded(score),
        "liquidity_risk_flags": "；".join(flags),
    }


def load_credit(path: str | Path) -> pd.DataFrame:
    return _load_csv(path, CREDIT_COLUMNS)


def load_attention_disposition(path: str | Path) -> pd.DataFrame:
    frame = _load_csv(path, ATTENTION_DISPOSITION_COLUMNS)
    for column in ["is_attention_stock", "is_disposition_stock"]:
        frame[column] = frame[column].apply(_to_bool)
    return frame


def load_sector_strength(path: str | Path) -> pd.DataFrame:
    return _load_csv(path, SECTOR_STRENGTH_COLUMNS)


def load_liquidity(path: str | Path) -> pd.DataFrame:
    return _load_csv(path, LIQUIDITY_COLUMNS)


def _load_csv(path: str | Path, columns: list[str]) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(csv_path, dtype={"stock_id": str})
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    if "stock_id" in frame.columns:
        frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    for column in frame.columns:
        if column not in {"stock_id", "stock_name", "industry", "attention_reason", "disposition_reason"}:
            if "date" in column:
                frame[column] = frame[column]
            elif column.startswith("is_"):
                frame[column] = frame[column]
            else:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[columns].copy()


def _neutral_credit(stock_id: str, reason: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "margin_balance": None,
        "margin_change": None,
        "short_balance": None,
        "short_change": None,
        "securities_lending_sell_volume": None,
        "securities_lending_balance": None,
        "credit_score": 50.0,
        "margin_usage_warning": reason,
        "short_selling_warning": "",
        "credit_reason": reason,
        "credit_risk_flags": "",
    }


def _neutral_attention(stock_id: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "is_attention_stock": False,
        "attention_reason": "",
        "is_disposition_stock": False,
        "disposition_start_date": "",
        "disposition_end_date": "",
        "disposition_reason": "",
        "event_risk_score": 50.0,
        "event_risk_level": "NONE",
        "event_blocked": False,
        "attention_disposition_reason": "無注意股或處置股資料",
        "event_risk_flags": "",
    }


def _neutral_sector(stock_id: str, reason: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "industry": "",
        "stock_return_5d": None,
        "stock_return_20d": None,
        "market_return_5d": None,
        "market_return_20d": None,
        "sector_return_5d": None,
        "sector_return_20d": None,
        "relative_strength_5d": None,
        "relative_strength_20d": None,
        "sector_strength_rank": None,
        "sector_strength_score": 50.0,
        "sector_strength_reason": reason,
    }


def _neutral_liquidity(stock_id: str, reason: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "avg_volume_20d": None,
        "avg_turnover_20d": None,
        "intraday_trading_ratio": None,
        "liquidity_score": 50.0,
        "liquidity_warning": reason,
        "slippage_risk_score": 50.0,
        "liquidity_risk_flags": "",
    }


def _context_lookup(frame: pd.DataFrame | None) -> dict[str, pd.Series]:
    if frame is None or frame.empty or "stock_id" not in frame.columns:
        return {}
    copy = frame.copy()
    copy["stock_id"] = copy["stock_id"].astype(str).str.strip()
    return {stock_id: row for stock_id, row in copy.drop_duplicates("stock_id").set_index("stock_id").iterrows()}


def _context_number(row: pd.Series | None, *columns: str) -> float | None:
    if row is None:
        return None
    for column in columns:
        if column in row.index:
            value = _number(row.get(column))
            if value is not None:
                return value
    return None


def _number(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}


def _date_or_blank(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _bounded(value: float) -> float:
    return max(0.0, min(100.0, round(float(value), 2)))
