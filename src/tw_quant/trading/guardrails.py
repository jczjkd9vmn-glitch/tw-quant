"""Paper-entry guardrails.

These checks only decide whether to create new paper pending orders. They do
not modify exits, existing holdings, broker state, or real orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from tw_quant.market_regime import MarketRegimeResult


GRADE_ORDER = {"D": 0, "C": 1, "B": 2, "A": 3}


@dataclass(frozen=True)
class GuardrailContext:
    enabled: bool
    min_grade_for_new_entry: str
    max_total_drawdown_pct: float
    max_daily_loss_pct: float
    max_consecutive_stop_loss: int
    pause_new_entries_days: int
    max_open_positions: int
    max_daily_new_positions: int
    min_market_regime_score: float
    market_regime_score: float
    total_drawdown_pct: float
    daily_loss_pct: float
    consecutive_stop_loss: int
    open_positions: int
    global_reasons: list[str]

    @property
    def new_entries_allowed(self) -> bool:
        return self.enabled is False or not self.global_reasons

    @property
    def guardrail_status(self) -> str:
        if not self.enabled:
            return "DISABLED"
        return "BLOCKED" if self.global_reasons else "OK"

    @property
    def pause_reason(self) -> str:
        return "；".join(self.global_reasons)


@dataclass(frozen=True)
class GuardrailDecision:
    allowed: bool
    reason: str
    status: str
    market_regime_score: float
    new_entries_allowed: bool


def build_guardrail_context(
    reports_dir: str | Path,
    capital: float,
    config: dict,
    market_regime: MarketRegimeResult | None = None,
) -> GuardrailContext:
    guard_config = config.get("paper_trading_guardrails", {}) if isinstance(config, dict) else {}
    regime_config = config.get("market_regime", {}) if isinstance(config, dict) else {}
    enabled = bool(guard_config.get("enabled", True))
    min_grade = str(guard_config.get("min_grade_for_new_entry", "A")).strip().upper() or "A"
    report_dir = Path(reports_dir)
    trades = _read_csv(report_dir / "paper_trades.csv")
    summary = _read_latest_summary(report_dir)
    market_score = (
        float(market_regime.market_regime_score)
        if market_regime is not None
        else float(regime_config.get("min_score_for_new_entries", 60))
    )
    max_drawdown = float(guard_config.get("max_total_drawdown_pct", 0.05))
    max_daily_loss = float(guard_config.get("max_daily_loss_pct", 0.02))
    max_stop_loss = int(guard_config.get("max_consecutive_stop_loss", 3))
    pause_days = int(guard_config.get("pause_new_entries_days", 5))
    max_open = int(guard_config.get("max_open_positions", 8))
    max_daily_new = int(guard_config.get("max_daily_new_positions", 2))
    min_regime = float(regime_config.get("min_score_for_new_entries", 60))

    open_positions = _open_position_count(trades)
    total_equity = _summary_number(summary, "total_equity_after_cost", capital)
    total_drawdown_pct = max(0.0, (float(capital) - total_equity) / float(capital)) if capital else 0.0
    realized_today = _summary_number(summary, "realized_pnl_after_cost_today", 0.0)
    daily_loss_pct = max(0.0, -realized_today / float(capital)) if capital else 0.0
    consecutive_stop_loss = _consecutive_stop_loss_count(trades)

    reasons: list[str] = []
    if bool(regime_config.get("enabled", True)) and market_score < min_regime:
        reasons.append(f"market_regime_score {market_score:.2f} 低於新增持倉門檻 {min_regime:.2f}")
    if total_drawdown_pct > max_drawdown:
        reasons.append(f"總回撤 {total_drawdown_pct:.2%} 超過 {max_drawdown:.2%}")
    if daily_loss_pct > max_daily_loss:
        reasons.append(f"單日虧損 {daily_loss_pct:.2%} 超過 {max_daily_loss:.2%}")
    if consecutive_stop_loss >= max_stop_loss:
        reasons.append(f"連續停損 {consecutive_stop_loss} 筆，暫停新增持倉 {pause_days} 個交易日")
    if open_positions >= max_open:
        reasons.append(f"目前持倉 {open_positions} 檔已達上限 {max_open} 檔")

    return GuardrailContext(
        enabled=enabled,
        min_grade_for_new_entry=min_grade,
        max_total_drawdown_pct=max_drawdown,
        max_daily_loss_pct=max_daily_loss,
        max_consecutive_stop_loss=max_stop_loss,
        pause_new_entries_days=pause_days,
        max_open_positions=max_open,
        max_daily_new_positions=max_daily_new,
        min_market_regime_score=min_regime,
        market_regime_score=round(market_score, 2),
        total_drawdown_pct=round(total_drawdown_pct, 6),
        daily_loss_pct=round(daily_loss_pct, 6),
        consecutive_stop_loss=consecutive_stop_loss,
        open_positions=open_positions,
        global_reasons=reasons,
    )


def evaluate_candidate_entry(
    row: pd.Series,
    context: GuardrailContext,
    created_today: int,
    duplicate_reason: str = "",
) -> GuardrailDecision:
    if not context.enabled:
        return GuardrailDecision(True, "paper guardrails disabled", "DISABLED", context.market_regime_score, True)
    if duplicate_reason:
        return _blocked(duplicate_reason, context)
    if context.global_reasons:
        return _blocked(context.pause_reason, context)
    if created_today >= context.max_daily_new_positions:
        return _blocked(f"今日新增 pending order 已達上限 {context.max_daily_new_positions} 筆", context)
    if _to_bool(row.get("event_blocked")):
        return _blocked("高風險事件或處置條件阻擋新增進場", context)

    grade = _candidate_grade(row)
    if grade and _grade_value(grade) < _grade_value(context.min_grade_for_new_entry):
        return _blocked(f"候選分級 {grade} 低於新增紙上交易門檻 {context.min_grade_for_new_entry}", context)
    if not grade:
        return GuardrailDecision(
            True,
            "legacy report missing candidate_grade; grade guardrail skipped",
            context.guardrail_status,
            context.market_regime_score,
            context.new_entries_allowed,
        )
    return GuardrailDecision(
        True,
        "符合 paper trading guardrails",
        context.guardrail_status,
        context.market_regime_score,
        context.new_entries_allowed,
    )


def _blocked(reason: str, context: GuardrailContext) -> GuardrailDecision:
    return GuardrailDecision(
        False,
        reason,
        "BLOCKED",
        context.market_regime_score,
        False,
    )


def _candidate_grade(row: pd.Series) -> str:
    value = str(row.get("candidate_grade", "") or "").strip().upper()
    if value in GRADE_ORDER:
        return value
    if "candidate_grade" in row.index:
        return ""
    return ""


def _grade_value(value: str) -> int:
    return GRADE_ORDER.get(str(value).strip().upper(), -1)


def _open_position_count(trades: pd.DataFrame) -> int:
    if trades.empty or "status" not in trades.columns:
        return 0
    return int((trades["status"].fillna("").astype(str).str.upper() == "OPEN").sum())


def _consecutive_stop_loss_count(trades: pd.DataFrame) -> int:
    if trades.empty or "status" not in trades.columns or "exit_reason" not in trades.columns:
        return 0
    frame = trades[trades["status"].fillna("").astype(str).str.upper() == "CLOSED"].copy()
    if frame.empty:
        return 0
    sort_column = "exit_date" if "exit_date" in frame.columns else "trade_date"
    if sort_column in frame.columns:
        frame["_sort_date"] = pd.to_datetime(frame[sort_column], errors="coerce")
        frame = frame.sort_values("_sort_date")
    count = 0
    for reason in reversed(frame["exit_reason"].fillna("").astype(str).str.lower().tolist()):
        if reason in {"stop_loss", "stop_loss_hit", "停損"}:
            count += 1
        else:
            break
    return count


def _read_latest_summary(report_dir: Path) -> dict[str, object]:
    files = sorted(report_dir.glob("paper_summary_*.csv"))
    if not files:
        files = sorted(report_dir.glob("daily_summary_*.csv"))
    if not files:
        return {}
    frame = _read_csv(files[-1])
    return frame.iloc[0].to_dict() if not frame.empty else {}


def _summary_number(summary: dict[str, object], column: str, default: float) -> float:
    if not summary:
        return float(default)
    try:
        value = float(summary.get(column, default))
    except (TypeError, ValueError):
        return float(default)
    if pd.isna(value):
        return float(default)
    return value


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}
