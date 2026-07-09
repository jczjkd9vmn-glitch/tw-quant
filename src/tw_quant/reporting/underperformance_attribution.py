"""Report-only attribution for why paper trading underperforms benchmark.

The diagnostics here intentionally read existing local CSV outputs only. They
do not modify strategy, exits, orders, reference data, or broker settings.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

from tw_quant.reporting.benchmark import select_benchmark_snapshot


UNDERPERFORMANCE_ATTRIBUTION_COLUMNS = [
    "trade_date",
    "attribution_type",
    "diagnostic_item",
    "stock_id",
    "stock_name",
    "industry",
    "window",
    "position_weight",
    "cash_ratio",
    "strategy_value",
    "benchmark_value",
    "alpha",
    "pnl_after_cost",
    "pnl_contribution_pct",
    "drawdown_contribution",
    "top3_contribution_pct",
    "diagnostic_status",
    "conclusion",
    "data_quality_warning",
    "notes",
]


@dataclass(frozen=True)
class UnderperformanceAttributionResult:
    trade_date: pd.Timestamp | None
    frame: pd.DataFrame
    output_path: Path
    status: str = "OK"
    warning: str = ""


def generate_underperformance_attribution(
    reports_dir: str | Path = "reports",
    trade_date: str | None = None,
) -> UnderperformanceAttributionResult:
    """Generate conservative underperformance diagnostics from report CSVs."""

    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    selected_date = _resolve_trade_date(report_dir, trade_date)
    date_label = (selected_date or pd.Timestamp.today()).strftime("%Y%m%d")

    benchmark = select_benchmark_snapshot(report_dir, selected_date)
    trades = _read_csv(report_dir / "paper_trades.csv")
    portfolio = _read_latest(report_dir, "paper_portfolio_*.csv", trade_date)
    paper_summary = _read_latest(report_dir, "paper_summary_*.csv", trade_date)
    performance = _read_latest(report_dir, "performance_diagnostics_*.csv", trade_date)
    sector_strength = _read_sector_strength(report_dir)
    loss_attribution = _read_latest(report_dir, "loss_attribution_*.csv", trade_date)

    rows: list[dict[str, object]] = []
    rows.extend(_stock_selection_alpha(selected_date, trades, portfolio, sector_strength, benchmark))
    rows.extend(_entry_timing_alpha(selected_date, trades, sector_strength, benchmark))
    rows.extend(_exit_timing_diagnostic(selected_date, trades, loss_attribution))
    rows.extend(_drawdown_contribution(selected_date, trades, loss_attribution))
    rows.extend(_position_concentration(selected_date, trades, portfolio, paper_summary))
    rows.extend(_sector_allocation_alpha(selected_date, trades, portfolio, sector_strength, benchmark))
    rows.extend(_missed_benchmark_rally(selected_date, paper_summary, benchmark, performance))
    rows.extend(_cash_drag(selected_date, paper_summary, benchmark))

    if not rows:
        rows.append(
            _row(
                selected_date,
                "underperformance_attribution",
                "DATA_INSUFFICIENT",
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少 paper trading 或 benchmark 資料，無法歸因。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        )

    frame = pd.DataFrame(rows, columns=UNDERPERFORMANCE_ATTRIBUTION_COLUMNS)
    output_path = report_dir / f"underperformance_attribution_{date_label}.csv"
    frame.to_csv(output_path, index=False, encoding="utf-8")

    warnings = [
        str(value) for value in frame.get("data_quality_warning", pd.Series(dtype=str)).dropna().unique() if str(value)
    ]
    status = "OK"
    if not frame.empty and (frame["diagnostic_status"].astype(str) == "DATA_INSUFFICIENT").all():
        status = "DATA_INSUFFICIENT"
    elif warnings:
        status = "OK_WITH_WARNINGS"
    return UnderperformanceAttributionResult(
        trade_date=selected_date,
        frame=frame,
        output_path=output_path,
        status=status,
        warning="; ".join(dict.fromkeys(warnings)),
    )


def _stock_selection_alpha(
    trade_date: pd.Timestamp | None,
    trades: pd.DataFrame,
    portfolio: pd.DataFrame,
    sector_strength: pd.DataFrame,
    benchmark: dict[str, object],
) -> list[dict[str, object]]:
    positions = _open_positions(trades, portfolio)
    benchmark_20d = _benchmark_return(benchmark, "20d")
    if positions.empty or sector_strength.empty:
        return [
            _row(
                trade_date,
                "stock_selection_alpha",
                "持有股票平均報酬 vs benchmark",
                window="20d",
                benchmark_value=benchmark_20d,
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少持倉或 sector_strength 資料，無法判斷選股 alpha。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        ]

    merged = _join_sector(positions, sector_strength)
    weights = _position_weights(merged)
    stock_returns = pd.to_numeric(merged.get("stock_return_20d"), errors="coerce")
    valid = stock_returns.notna()
    if not valid.any():
        return [
            _row(
                trade_date,
                "stock_selection_alpha",
                "持有股票平均報酬 vs benchmark",
                window="20d",
                benchmark_value=benchmark_20d,
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少持有股票 20 日報酬，無法判斷選股 alpha。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="需要 stock_return_20d；此區只做診斷。",
            )
        ]

    strategy_return = float((stock_returns[valid] * weights[valid]).sum())
    alpha = _sub_or_none(strategy_return, benchmark_20d)
    conclusion = (
        "持有股票近期平均報酬落後 benchmark。"
        if alpha is not None and alpha < 0
        else "持有股票近期平均報酬未落後 benchmark。"
    )
    return [
        _row(
            trade_date,
            "stock_selection_alpha",
            "持有股票平均報酬 vs benchmark",
            window="20d",
            strategy_value=strategy_return,
            benchmark_value=benchmark_20d,
            alpha=alpha,
            diagnostic_status="OBSERVATION_ONLY",
            conclusion=conclusion,
            data_quality_warning="OBSERVATION_ONLY",
            notes="使用最新 sector_strength stock_return_20d 作 proxy，不等同完整逐筆持有期間回測；此區只做診斷，不修改策略。",
        )
    ]


def _entry_timing_alpha(
    trade_date: pd.Timestamp | None,
    trades: pd.DataFrame,
    sector_strength: pd.DataFrame,
    benchmark: dict[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    normalized = _normalize_trades(trades)
    for window in ["1d", "5d", "20d"]:
        benchmark_value = _benchmark_return(benchmark, window)
        values, source_note = _entry_window_returns(normalized, sector_strength, window)
        if values.empty:
            rows.append(
                _row(
                    trade_date,
                    "entry_timing_alpha",
                    f"進場後 {window} 表現",
                    window=window,
                    benchmark_value=benchmark_value,
                    diagnostic_status="DATA_INSUFFICIENT",
                    conclusion=f"缺少進場後 {window} forward return，不能判斷進場時機。",
                    data_quality_warning="DATA_INSUFFICIENT",
                    notes="需要逐筆進場後 forward return；此區只做診斷。",
                )
            )
            continue
        strategy_value = float(values.mean())
        alpha = _sub_or_none(strategy_value, benchmark_value)
        conclusion = "進場後表現落後 benchmark。" if alpha is not None and alpha < 0 else "進場後表現未落後 benchmark。"
        rows.append(
            _row(
                trade_date,
                "entry_timing_alpha",
                f"進場後 {window} 表現",
                window=window,
                strategy_value=strategy_value,
                benchmark_value=benchmark_value,
                alpha=alpha,
                diagnostic_status="OBSERVATION_ONLY" if source_note else "OK",
                conclusion=conclusion,
                data_quality_warning="OBSERVATION_ONLY" if source_note else "",
                notes=(source_note or "使用逐筆 forward return。") + " 此區只做診斷，不修改策略。",
            )
        )
    return rows


def _exit_timing_diagnostic(
    trade_date: pd.Timestamp | None,
    trades: pd.DataFrame,
    loss_attribution: pd.DataFrame,
) -> list[dict[str, object]]:
    frame = loss_attribution if not loss_attribution.empty else trades
    frame = _normalize_trades(frame)
    if frame.empty or "exit_reason" not in frame.columns:
        return [
            _row(
                trade_date,
                "exit_timing_diagnostic",
                "停損 / 停利後續走勢",
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少出場原因，無法判斷停損後反彈或停利後續漲。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改出場規則。",
            )
        ]

    rows: list[dict[str, object]] = []
    exit_reason = frame["exit_reason"].astype(str).str.lower()
    stop_loss = frame[exit_reason.str.contains("stop_loss|停損", na=False)]
    take_profit = frame[exit_reason.str.contains("take_profit|trailing|停利", na=False)]
    for label, subset, question in [
        ("停損後是否反彈", stop_loss, "缺少停損後 forward return，不能正式判斷是否反彈。"),
        ("停利後是否續漲", take_profit, "缺少停利後 forward return，不能正式判斷是否續漲。"),
    ]:
        post_exit = _first_numeric_series(subset, ["post_exit_return_5d", "forward_return_after_exit_5d"])
        if post_exit.empty:
            rows.append(
                _row(
                    trade_date,
                    "exit_timing_diagnostic",
                    label,
                    diagnostic_status="DATA_INSUFFICIENT",
                    conclusion=question,
                    data_quality_warning="DATA_INSUFFICIENT",
                    notes=f"樣本數={len(subset)}；現有 CSV 尚無出場後 5 日報酬；此區只做診斷，不修改出場規則。",
                )
            )
            continue
        mean_return = float(post_exit.mean())
        conclusion = (
            f"{label} proxy 為正，需進一步人工檢查。" if mean_return > 0 else f"{label} proxy 未顯示明顯正報酬。"
        )
        rows.append(
            _row(
                trade_date,
                "exit_timing_diagnostic",
                label,
                strategy_value=mean_return,
                diagnostic_status="OBSERVATION_ONLY",
                conclusion=conclusion,
                data_quality_warning="OBSERVATION_ONLY",
                notes="使用出場後 forward return 欄位；此區只做診斷，不修改出場規則。",
            )
        )
    return rows


def _drawdown_contribution(
    trade_date: pd.Timestamp | None,
    trades: pd.DataFrame,
    loss_attribution: pd.DataFrame,
) -> list[dict[str, object]]:
    frame = loss_attribution.copy() if not loss_attribution.empty else _normalize_trades(trades)
    if frame.empty:
        return [
            _row(
                trade_date,
                "drawdown_contribution",
                "最大回撤股票",
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少交易或 loss attribution 資料，無法判斷回撤來源。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        ]
    frame = _ensure_stock_id(frame)
    if "max_adverse_excursion" in frame.columns:
        frame["_drawdown"] = pd.to_numeric(frame["max_adverse_excursion"], errors="coerce")
    else:
        frame["_drawdown"] = pd.to_numeric(
            frame.get("realized_pnl_pct_after_cost", frame.get("realized_pnl_pct")), errors="coerce"
        )
    frame = frame.dropna(subset=["_drawdown"]).sort_values("_drawdown", ascending=True)
    if frame.empty:
        return [
            _row(
                trade_date,
                "drawdown_contribution",
                "最大回撤股票",
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少最大不利幅度或交易報酬欄位。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        ]
    rows = []
    for _, item in frame.head(5).iterrows():
        drawdown = _num(item.get("_drawdown"))
        rows.append(
            _row(
                trade_date,
                "drawdown_contribution",
                "最大回撤股票",
                stock_id=item.get("stock_id"),
                stock_name=item.get("stock_name"),
                drawdown_contribution=drawdown,
                pnl_after_cost=_first_number(item, ["realized_pnl_after_cost", "unrealized_pnl", "realized_pnl"]),
                diagnostic_status="OBSERVATION_ONLY",
                conclusion="少數股票造成較大回撤，需檢查選股與部位控管。",
                data_quality_warning="OBSERVATION_ONLY",
                notes="以 max_adverse_excursion 或交易報酬 proxy 回撤貢獻；此區只做診斷。",
            )
        )
    return rows


def _position_concentration(
    trade_date: pd.Timestamp | None,
    trades: pd.DataFrame,
    portfolio: pd.DataFrame,
    paper_summary: pd.DataFrame,
) -> list[dict[str, object]]:
    frame = portfolio if not portfolio.empty else trades
    frame = _normalize_trades(frame)
    if frame.empty:
        return [
            _row(
                trade_date,
                "position_concentration",
                "前 3 大持倉對總損益貢獻",
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少持倉資料，無法判斷集中度。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        ]
    frame["_pnl"] = frame.apply(
        lambda row: _first_number(row, ["realized_pnl_after_cost", "unrealized_pnl", "realized_pnl"]) or 0.0, axis=1
    )
    frame["_abs_pnl"] = frame["_pnl"].abs()
    total_abs = float(frame["_abs_pnl"].sum())
    if total_abs <= 0:
        return [
            _row(
                trade_date,
                "position_concentration",
                "前 3 大持倉對總損益貢獻",
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="損益絕對值不足，無法判斷集中度。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        ]
    top3 = frame.sort_values("_abs_pnl", ascending=False).head(3).copy()
    top3_contribution = float(top3["_abs_pnl"].sum() / total_abs)
    conclusion = (
        "損益集中在少數股票，回撤可能受個股影響較大。" if top3_contribution >= 0.6 else "損益集中度未明顯過高。"
    )
    rows = [
        _row(
            trade_date,
            "position_concentration",
            "前 3 大持倉對總損益貢獻",
            top3_contribution_pct=top3_contribution,
            diagnostic_status="OBSERVATION_ONLY",
            conclusion=conclusion,
            data_quality_warning="OBSERVATION_ONLY",
            notes="以 paper trading 損益絕對值估算集中度；此區只做診斷。",
        )
    ]
    for _, item in top3.iterrows():
        rows.append(
            _row(
                trade_date,
                "position_concentration",
                "損益貢獻個股",
                stock_id=item.get("stock_id"),
                stock_name=item.get("stock_name"),
                pnl_after_cost=item.get("_pnl"),
                pnl_contribution_pct=float(item.get("_abs_pnl", 0.0) / total_abs),
                diagnostic_status="OBSERVATION_ONLY",
                conclusion="前 3 大損益貢獻明細。",
                data_quality_warning="OBSERVATION_ONLY",
                notes="此區只做診斷，不修改策略。",
            )
        )
    return rows


def _sector_allocation_alpha(
    trade_date: pd.Timestamp | None,
    trades: pd.DataFrame,
    portfolio: pd.DataFrame,
    sector_strength: pd.DataFrame,
    benchmark: dict[str, object],
) -> list[dict[str, object]]:
    positions = _open_positions(trades, portfolio)
    benchmark_20d = _benchmark_return(benchmark, "20d")
    if positions.empty or sector_strength.empty:
        return [
            _row(
                trade_date,
                "sector_allocation_alpha",
                "持倉產業 vs benchmark",
                window="20d",
                benchmark_value=benchmark_20d,
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少持倉或產業相對強弱資料，無法判斷產業配置 alpha。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        ]
    merged = _join_sector(positions, sector_strength)
    if "industry" not in merged.columns:
        return []
    weights = _position_weights(merged)
    merged["_weight"] = weights
    rows: list[dict[str, object]] = []
    for industry, group in merged.groupby("industry", dropna=False):
        sector_return = pd.to_numeric(group.get("sector_return_20d"), errors="coerce")
        valid = sector_return.notna()
        if not valid.any():
            continue
        weight = float(group.loc[valid, "_weight"].sum())
        value = float((sector_return[valid] * group.loc[valid, "_weight"]).sum() / weight) if weight > 0 else None
        alpha = _sub_or_none(value, benchmark_20d)
        rows.append(
            _row(
                trade_date,
                "sector_allocation_alpha",
                "持倉產業 vs benchmark",
                industry=industry,
                window="20d",
                position_weight=weight,
                strategy_value=value,
                benchmark_value=benchmark_20d,
                alpha=alpha,
                diagnostic_status="OBSERVATION_ONLY",
                conclusion="持倉產業近期落後 benchmark。"
                if alpha is not None and alpha < 0
                else "持倉產業近期未落後 benchmark。",
                data_quality_warning="OBSERVATION_ONLY",
                notes="使用最新 sector_return_20d 作產業配置 proxy；此區只做診斷。",
            )
        )
    return rows or [
        _row(
            trade_date,
            "sector_allocation_alpha",
            "持倉產業 vs benchmark",
            window="20d",
            benchmark_value=benchmark_20d,
            diagnostic_status="DATA_INSUFFICIENT",
            conclusion="缺少 sector_return_20d，無法判斷產業配置 alpha。",
            data_quality_warning="DATA_INSUFFICIENT",
            notes="此區只做診斷，不修改策略。",
        )
    ]


def _missed_benchmark_rally(
    trade_date: pd.Timestamp | None,
    paper_summary: pd.DataFrame,
    benchmark: dict[str, object],
    performance: pd.DataFrame,
) -> list[dict[str, object]]:
    benchmark_20d = _benchmark_return(benchmark, "20d")
    invested_ratio, cash_ratio = _capital_ratios(paper_summary)
    conclusion_status = _first_text_from_frame(performance, "conclusion_status")
    if benchmark_20d is None or invested_ratio is None:
        return [
            _row(
                trade_date,
                "missed_benchmark_rally",
                "大盤上漲期間持倉不足",
                benchmark_value=benchmark_20d,
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少 benchmark 或資金配置資料，無法判斷是否錯過大盤上漲。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        ]
    missed = benchmark_20d > 0.03 and invested_ratio < 0.5
    conclusion = (
        "大盤上漲期間持倉比例偏低，可能錯過 benchmark rally。" if missed else "未看到明顯因持倉不足錯過大盤上漲的訊號。"
    )
    if conclusion_status == "UNDERPERFORMING" and missed:
        conclusion += " 這可能是目前 UNDERPERFORMING 的一項原因。"
    return [
        _row(
            trade_date,
            "missed_benchmark_rally",
            "大盤上漲期間持倉不足",
            window="20d",
            position_weight=invested_ratio,
            cash_ratio=cash_ratio,
            benchmark_value=benchmark_20d,
            diagnostic_status="OBSERVATION_ONLY",
            conclusion=conclusion,
            data_quality_warning="OBSERVATION_ONLY",
            notes="以 invested_value / total_equity 與 benchmark_20d 粗估；此區只做診斷，不修改策略。",
        )
    ]


def _cash_drag(
    trade_date: pd.Timestamp | None,
    paper_summary: pd.DataFrame,
    benchmark: dict[str, object],
) -> list[dict[str, object]]:
    benchmark_20d = _benchmark_return(benchmark, "20d")
    _, cash_ratio = _capital_ratios(paper_summary)
    if benchmark_20d is None or cash_ratio is None:
        return [
            _row(
                trade_date,
                "cash_drag",
                "現金拖累",
                window="20d",
                benchmark_value=benchmark_20d,
                diagnostic_status="DATA_INSUFFICIENT",
                conclusion="缺少 benchmark 或現金比例資料，無法估算 cash drag。",
                data_quality_warning="DATA_INSUFFICIENT",
                notes="此區只做診斷，不修改策略。",
            )
        ]
    drag = cash_ratio * benchmark_20d if benchmark_20d > 0 else 0.0
    conclusion = "現金比例過高可能拖累上漲行情參與度。" if drag > 0.02 else "cash drag 目前不是主要拖累。"
    return [
        _row(
            trade_date,
            "cash_drag",
            "現金拖累",
            window="20d",
            cash_ratio=cash_ratio,
            benchmark_value=benchmark_20d,
            alpha=-drag,
            diagnostic_status="OBSERVATION_ONLY",
            conclusion=conclusion,
            data_quality_warning="OBSERVATION_ONLY",
            notes="cash_drag 約等於 cash_ratio * benchmark_return_20d；此區只做診斷，不修改策略。",
        )
    ]


def _row(
    trade_date: pd.Timestamp | None,
    attribution_type: str,
    diagnostic_item: str,
    **values: object,
) -> dict[str, object]:
    row = {column: "" for column in UNDERPERFORMANCE_ATTRIBUTION_COLUMNS}
    row.update(
        {
            "trade_date": _date_text(trade_date),
            "attribution_type": attribution_type,
            "diagnostic_item": diagnostic_item,
            **values,
        }
    )
    return row


def _open_positions(trades: pd.DataFrame, portfolio: pd.DataFrame) -> pd.DataFrame:
    source = portfolio if not portfolio.empty else trades
    frame = _normalize_trades(source)
    if frame.empty or "status" not in frame.columns:
        return pd.DataFrame()
    return frame[frame["status"].astype(str).str.upper() == "OPEN"].copy()


def _normalize_trades(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    output = _ensure_stock_id(frame.copy())
    return output


def _ensure_stock_id(frame: pd.DataFrame) -> pd.DataFrame:
    if "stock_id" in frame.columns:
        frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    return frame


def _join_sector(frame: pd.DataFrame, sector_strength: pd.DataFrame) -> pd.DataFrame:
    if (
        frame.empty
        or sector_strength.empty
        or "stock_id" not in frame.columns
        or "stock_id" not in sector_strength.columns
    ):
        return frame.copy()
    sector = sector_strength.copy()
    sector["stock_id"] = sector["stock_id"].astype(str).str.strip()
    keep = [
        column
        for column in [
            "stock_id",
            "industry",
            "sub_industry",
            "stock_return_5d",
            "stock_return_20d",
            "market_return_5d",
            "market_return_20d",
            "sector_return_5d",
            "sector_return_20d",
        ]
        if column in sector.columns
    ]
    return frame.merge(sector[keep].drop_duplicates("stock_id"), on="stock_id", how="left")


def _position_weights(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    value_column = "market_value" if "market_value" in frame.columns else "position_value"
    if value_column not in frame.columns:
        return pd.Series([1.0 / len(frame)] * len(frame), index=frame.index)
    values = pd.to_numeric(frame[value_column], errors="coerce").fillna(0.0)
    total = float(values.sum())
    if total <= 0:
        return pd.Series([1.0 / len(frame)] * len(frame), index=frame.index)
    return values / total


def _entry_window_returns(trades: pd.DataFrame, sector_strength: pd.DataFrame, window: str) -> tuple[pd.Series, str]:
    if trades.empty:
        return pd.Series(dtype=float), ""
    direct_column = f"forward_return_{window}"
    if direct_column in trades.columns:
        values = pd.to_numeric(trades[direct_column], errors="coerce").dropna()
        return values, ""
    if window in {"5d", "20d"} and not sector_strength.empty:
        merged = _join_sector(trades, sector_strength)
        column = f"stock_return_{window}"
        if column in merged.columns:
            values = pd.to_numeric(merged[column], errors="coerce").dropna()
            return values, f"缺少逐筆 forward_return_{window}，使用最新 sector_strength {column} 作觀察 proxy。"
    return pd.Series(dtype=float), ""


def _capital_ratios(paper_summary: pd.DataFrame) -> tuple[float | None, float | None]:
    if paper_summary.empty:
        return None, None
    row = paper_summary.iloc[0]
    total = _first_number(row, ["total_equity_after_cost", "total_equity", "total_capital"])
    invested = _first_number(row, ["market_value", "invested_value"])
    cash = _first_number(row, ["cash"])
    if total is None or total <= 0:
        return None, None
    invested_ratio = invested / total if invested is not None else None
    cash_ratio = cash / total if cash is not None else (1.0 - invested_ratio if invested_ratio is not None else None)
    return invested_ratio, cash_ratio


def _benchmark_return(benchmark: dict[str, object], window: str) -> float | None:
    returns = benchmark.get("returns", {}) if isinstance(benchmark.get("returns"), dict) else {}
    value = returns.get(window)
    if not bool(benchmark.get("benchmark_is_official", False)):
        return _num(value)
    if not bool(benchmark.get(f"can_judge_alpha_{window}", False)):
        return None
    return _num(value)


def _read_sector_strength(report_dir: Path) -> pd.DataFrame:
    for path in [report_dir / "sector_strength.csv", report_dir.parent / "data" / "sector_strength.csv"]:
        frame = _read_csv(path)
        if not frame.empty and "stock_id" in frame.columns:
            return _ensure_stock_id(frame)
    return pd.DataFrame()


def _read_latest(report_dir: Path, pattern: str, trade_date: str | None) -> pd.DataFrame:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            target = report_dir / pattern.replace("*", parsed.strftime("%Y%m%d"))
            if target.exists():
                return _read_csv(target)
    latest = _latest_file(report_dir, pattern)
    return _read_csv(latest) if latest is not None else pd.DataFrame()


def _latest_file(report_dir: Path, pattern: str) -> Path | None:
    files = sorted(
        report_dir.glob(pattern),
        key=lambda path: (_date_from_path(path) or pd.Timestamp.min, path.stat().st_mtime),
        reverse=True,
    )
    return files[0] if files else None


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _resolve_trade_date(report_dir: Path, trade_date: str | None) -> pd.Timestamp | None:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    for pattern in [
        "performance_diagnostics_*.csv",
        "benchmark_diagnostics_*.csv",
        "pnl_chart_data_*.csv",
        "paper_summary_*.csv",
        "daily_summary_*.csv",
    ]:
        latest = _latest_file(report_dir, pattern)
        if latest is not None:
            parsed = _date_from_path(latest)
            if parsed is not None:
                return parsed
    return None


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed


def _first_numeric_series(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    for column in columns:
        if column in frame.columns:
            values = pd.to_numeric(frame[column], errors="coerce").dropna()
            if not values.empty:
                return values
    return pd.Series(dtype=float)


def _first_number(row: pd.Series | dict[str, object], columns: list[str]) -> float | None:
    for column in columns:
        value = _num(row.get(column))
        if value is not None:
            return value
    return None


def _first_text_from_frame(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return ""
    value = frame.iloc[0].get(column)
    return "" if pd.isna(value) else str(value)


def _sub_or_none(left: object, right: object) -> float | None:
    left_number = _num(left)
    right_number = _num(right)
    if left_number is None or right_number is None:
        return None
    return round(left_number - right_number, 6)


def _num(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _date_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")
