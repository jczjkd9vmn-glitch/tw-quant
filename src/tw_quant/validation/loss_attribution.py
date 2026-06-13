"""Loss attribution report for paper trades and open positions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd


LOSS_ATTRIBUTION_COLUMNS = [
    "candidate_grade",
    "decision",
    "entry_date",
    "exit_date",
    "stock_id",
    "stock_name",
    "entry_price",
    "exit_price",
    "realized_pnl_pct",
    "unrealized_pnl_pct",
    "exit_reason",
    "holding_days",
    "liquidity_score",
    "sector_strength_score",
    "confidence_score",
    "market_regime_score",
    "gap_pct",
    "max_favorable_excursion",
    "max_adverse_excursion",
    "loss_bucket",
    "likely_loss_reason",
]


@dataclass(frozen=True)
class LossAttributionResult:
    trade_date: pd.Timestamp | None
    attribution: pd.DataFrame
    output_path: Path | None
    warning: str = ""


def generate_loss_attribution(
    reports_dir: str | Path = "reports",
    trade_date: str | None = None,
) -> LossAttributionResult:
    report_dir = Path(reports_dir)
    trades = _read_csv(report_dir / "paper_trades.csv")
    selected_date = _resolve_trade_date(report_dir, trade_date)
    if trades.empty:
        frame = pd.DataFrame(columns=LOSS_ATTRIBUTION_COLUMNS)
        path = _write_attribution(report_dir, selected_date, frame)
        return LossAttributionResult(selected_date, frame, path, "no paper trades found")

    candidates = _read_latest(report_dir, "candidates_*.csv", trade_date)
    decisions = _read_latest(report_dir, "trading_decisions_*.csv", trade_date)
    regime = _read_latest(report_dir, "market_regime_*.csv", trade_date)
    market_score = _first_number(regime, "market_regime_score", 50.0)
    context = _context_lookup(candidates, decisions)
    rows = []
    for _, trade in trades.iterrows():
        stock_id = str(trade.get("stock_id", "")).strip()
        merged = {**context.get(stock_id, {}), **trade.to_dict()}
        rows.append(_attribution_row(merged, market_score))
    frame = pd.DataFrame(rows, columns=LOSS_ATTRIBUTION_COLUMNS)
    path = _write_attribution(report_dir, selected_date, frame)
    return LossAttributionResult(selected_date, frame, path)


def _attribution_row(row: dict[str, object], market_score: float) -> dict[str, object]:
    realized = _num(row.get("realized_pnl_pct_after_cost"), None)
    if realized is None:
        realized = _num(row.get("realized_pnl_pct"), None)
    unrealized = _num(row.get("unrealized_pnl_pct"), None)
    return_pct = realized if realized is not None else unrealized if unrealized is not None else 0.0
    liquidity = _num(row.get("liquidity_score"), 50.0)
    sector = _num(row.get("sector_strength_score"), 50.0)
    confidence = _num(row.get("confidence_score"), 50.0)
    exit_reason = _text(row.get("exit_reason"))
    status = _text(row.get("status")).upper()
    return {
        "candidate_grade": _text(row.get("candidate_grade")) or "-",
        "decision": _text(row.get("decision")) or ("HOLD" if status == "OPEN" else "-"),
        "entry_date": _entry_date(row),
        "exit_date": _text(row.get("exit_date")),
        "stock_id": _text(row.get("stock_id")),
        "stock_name": _text(row.get("stock_name")),
        "entry_price": _num(row.get("entry_price"), None),
        "exit_price": _num(row.get("exit_price"), None),
        "realized_pnl_pct": realized,
        "unrealized_pnl_pct": unrealized,
        "exit_reason": exit_reason,
        "holding_days": _num(row.get("holding_days"), 0),
        "liquidity_score": liquidity,
        "sector_strength_score": sector,
        "confidence_score": confidence,
        "market_regime_score": market_score,
        "gap_pct": _gap_pct(row),
        "max_favorable_excursion": _mfe(row, return_pct),
        "max_adverse_excursion": _mae(row, return_pct),
        "loss_bucket": _loss_bucket(return_pct, status),
        "likely_loss_reason": _likely_loss_reason(
            return_pct=return_pct,
            exit_reason=exit_reason,
            liquidity=liquidity,
            sector=sector,
            confidence=confidence,
            market_score=market_score,
            grade=_text(row.get("candidate_grade")),
        ),
    }


def _context_lookup(candidates: pd.DataFrame, decisions: pd.DataFrame) -> dict[str, dict[str, object]]:
    output: dict[str, dict[str, object]] = {}
    for frame in [candidates, decisions]:
        if frame.empty or "stock_id" not in frame.columns:
            continue
        for _, row in frame.iterrows():
            stock_id = str(row.get("stock_id", "")).strip()
            if not stock_id:
                continue
            output.setdefault(stock_id, {}).update(
                {
                    key: row.get(key)
                    for key in [
                        "candidate_grade",
                        "decision",
                        "liquidity_score",
                        "sector_strength_score",
                        "confidence_score",
                    ]
                    if key in row.index
                }
            )
    return output


def _entry_date(row: dict[str, object]) -> str:
    for column in ["actual_entry_date", "trade_date", "signal_date"]:
        text = _text(row.get(column))
        if text:
            return text
    return ""


def _gap_pct(row: dict[str, object]) -> float | None:
    signal = _num(row.get("signal_close"), None)
    entry = _num(row.get("entry_price"), None)
    if signal is None or entry is None or signal == 0:
        return None
    return round(entry / signal - 1.0, 6)


def _mfe(row: dict[str, object], return_pct: float) -> float:
    value = _num(row.get("highest_pnl_pct_since_entry"), None)
    if value is not None:
        return round(value, 6)
    return round(max(return_pct, 0.0), 6)


def _mae(row: dict[str, object], return_pct: float) -> float:
    stop_hit = _text(row.get("exit_reason")).lower() in {"stop_loss", "stop_loss_hit"}
    if stop_hit:
        return round(min(return_pct, -0.08), 6)
    return round(min(return_pct, 0.0), 6)


def _loss_bucket(return_pct: float, status: str) -> str:
    if return_pct >= 0:
        return "profitable_or_flat"
    if return_pct <= -0.08:
        return "large_loss"
    if status == "OPEN":
        return "unrealized_loss"
    return "small_loss"


def _likely_loss_reason(
    *,
    return_pct: float,
    exit_reason: str,
    liquidity: float,
    sector: float,
    confidence: float,
    market_score: float,
    grade: str,
) -> str:
    if return_pct >= 0:
        return "目前非虧損交易"
    reasons = []
    if exit_reason.lower() in {"stop_loss", "stop_loss_hit"}:
        reasons.append("停損出場")
    if market_score < 60:
        reasons.append("市場環境偏弱")
    if liquidity < 50:
        reasons.append("流動性偏低")
    if sector < 45:
        reasons.append("產業 / 相對弱勢")
    if confidence < 60:
        reasons.append("資料可信度偏低")
    if str(grade).upper() in {"C", "D"}:
        reasons.append("候選分級偏弱")
    return "；".join(reasons) if reasons else "價格走勢不如預期，需人工複核"


def _write_attribution(report_dir: Path, trade_date: pd.Timestamp | None, frame: pd.DataFrame) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    target_date = trade_date or pd.Timestamp.today()
    path = report_dir / f"loss_attribution_{target_date.strftime('%Y%m%d')}.csv"
    frame.to_csv(path, index=False, encoding="utf-8")
    return path


def _resolve_trade_date(report_dir: Path, trade_date: str | None) -> pd.Timestamp | None:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    for pattern in ["daily_summary_*.csv", "paper_summary_*.csv", "paper_trades.csv"]:
        if pattern == "paper_trades.csv":
            frame = _read_csv(report_dir / pattern)
            if not frame.empty:
                value = frame.get("trade_date", pd.Series([None])).dropna()
                if not value.empty:
                    return pd.to_datetime(value.iloc[-1], errors="coerce")
            continue
        files = sorted(report_dir.glob(pattern), key=lambda path: _date_from_path(path) or pd.Timestamp.min)
        if files:
            return _date_from_path(files[-1])
    return None


def _read_latest(report_dir: Path, pattern: str, trade_date: str | None) -> pd.DataFrame:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            target = report_dir / pattern.replace("*", parsed.strftime("%Y%m%d"))
            if target.exists():
                return _read_csv(target)
    files = sorted(report_dir.glob(pattern), key=lambda path: _date_from_path(path) or pd.Timestamp.min)
    return _read_csv(files[-1]) if files else pd.DataFrame()


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _first_number(frame: pd.DataFrame, column: str, default: float) -> float:
    if frame.empty or column not in frame.columns:
        return float(default)
    parsed = _num(frame.iloc[0].get(column), default)
    return float(default) if parsed is None else float(parsed)


def _num(value: object, default: float | None) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return parsed


def _text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()
