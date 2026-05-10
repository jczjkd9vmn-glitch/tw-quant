from __future__ import annotations

"""Streamlit dashboard for daily candidates and backtest results."""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tw_quant.backtest.engine import BacktestConfig, BacktestEngine
from tw_quant.config import load_config
from tw_quant.data.database import create_db_engine, init_db, load_candidate_scores, load_price_history
from tw_quant.reporting.performance import load_paper_performance
from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.strategy.scoring import ScoringConfig, StockScorer


ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    st.set_page_config(page_title="台股量化 Dashboard", layout="wide")
    st.title("台股量化 Dashboard")

    config = load_config(ROOT / "config.yaml")
    engine = create_db_engine(config["database"]["url"])
    init_db(engine)

    scores = load_candidate_scores(engine)
    prices = load_price_history(engine)

    if scores.empty:
        st.warning("尚未有評分資料。請先執行 scripts/run_daily.py。")
    else:
        latest_date = scores["trade_date"].max()
        latest_scores = scores[scores["trade_date"] == latest_date].copy()
        candidates = latest_scores[latest_scores["is_candidate"] == 1]
        risk_pass_candidates = candidates[candidates["risk_pass"] == 1]

        st.subheader(f"今日候選股票：{latest_date.date()}")
        score_cols = st.columns(3)
        score_cols[0].metric("scored_rows", f"{len(latest_scores)}")
        score_cols[1].metric("candidate_rows", f"{len(candidates)}")
        score_cols[2].metric("risk_pass_rows", f"{len(risk_pass_candidates)}")
        st.dataframe(
            candidates[
                [
                    "symbol",
                    "name",
                    "close",
                    "total_score",
                    "trend_score",
                    "momentum_score",
                    "fundamental_score",
                    "chip_score",
                    "risk_score",
                    "stop_loss",
                    "suggested_position_pct",
                    "buy_reasons",
                    "risk_reasons",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

        st.subheader("每檔股票評分")
        fig = px.bar(
            latest_scores.head(30),
            x="symbol",
            y=["trend_score", "momentum_score", "fundamental_score", "chip_score", "risk_score"],
            barmode="group",
            title="前 30 名股票五構面評分",
        )
        st.plotly_chart(fig, width="stretch")

    st.subheader("紙上交易績效")
    performance = load_paper_performance(
        ROOT / "reports",
        capital=float(config["risk"]["initial_equity"]),
    )
    if performance.warning:
        st.warning(performance.warning)
    else:
        perf_cols = st.columns(5)
        perf_cols[0].metric("open_positions", f"{int(performance.metrics['open_positions'])}")
        perf_cols[1].metric("closed_positions", f"{int(performance.metrics['closed_positions'])}")
        perf_cols[2].metric("win_rate", f"{performance.metrics['win_rate']:.2%}")
        perf_cols[3].metric("total_return_pct", f"{performance.metrics['total_return_pct']:.2%}")
        perf_cols[4].metric("max_drawdown", f"{performance.metrics['max_drawdown']:.2%}")

        if not performance.summary.empty:
            curve = performance.summary.melt(
                id_vars=["trade_date"],
                value_vars=["total_equity", "unrealized_pnl", "realized_pnl"],
                var_name="metric",
                value_name="value",
            )
            perf_fig = px.line(curve, x="trade_date", y="value", color="metric", title="紙上交易損益曲線")
            st.plotly_chart(perf_fig, width="stretch")

        st.subheader("目前 OPEN 持倉")
        if performance.open_positions.empty:
            st.info("目前沒有 OPEN 紙上持倉。")
        else:
            st.dataframe(performance.open_positions, width="stretch", hide_index=True)

        st.subheader("CLOSED 交易紀錄")
        if performance.closed_trades.empty:
            st.info("目前沒有 CLOSED 紙上交易。")
        else:
            st.dataframe(performance.closed_trades, width="stretch", hide_index=True)

    st.subheader("回測績效")
    if prices.empty:
        st.info("尚未有價格資料可回測。")
        return

    if not st.button("執行回測"):
        st.info("回測會重新計算歷史訊號；資料量較大時請按下按鈕後等待。")
        return

    risk_manager = RiskManager(RiskConfig.from_mapping(config["risk"]))
    scorer = StockScorer(
        ScoringConfig.from_mapping(config["strategy"]),
        risk_manager=risk_manager,
    )
    backtest = BacktestEngine(
        BacktestConfig.from_mapping(config["backtest"]),
        scorer=scorer,
        risk_manager=risk_manager,
    )
    result = backtest.run(prices)
    metric_cols = st.columns(5)
    metric_cols[0].metric("總報酬", f"{result.metrics['total_return']:.2%}")
    metric_cols[1].metric("最大回撤", f"{result.metrics['max_drawdown']:.2%}")
    metric_cols[2].metric("Sharpe", f"{result.metrics['sharpe']:.2f}")
    metric_cols[3].metric("勝率", f"{result.metrics['win_rate']:.2%}")
    metric_cols[4].metric("交易筆數", f"{int(result.metrics['trades'])}")

    if not result.equity_curve.empty:
        equity_fig = px.line(result.equity_curve, x="trade_date", y="equity", title="權益曲線")
        st.plotly_chart(equity_fig, width="stretch")

    if not result.trades.empty:
        st.dataframe(result.trades, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
