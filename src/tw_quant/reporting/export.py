"""Candidate report export helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from tw_quant.config import load_config
from tw_quant.data.database import load_candidate_scores
from tw_quant.fundamental.revenue import score_revenue_for_symbols
from tw_quant.scoring.multi_factor import apply_multi_factor_scores, write_data_fetch_status


EXPORT_COLUMNS = [
    "rank",
    "trade_date",
    "stock_id",
    "stock_name",
    "close",
    "total_score",
    "original_total_score",
    "multi_factor_score",
    "multi_factor_reason",
    "trend_score",
    "momentum_score",
    "fundamental_score",
    "chip_score",
    "risk_score",
    "revenue_score",
    "revenue_yoy",
    "revenue_mom",
    "accumulated_revenue_yoy",
    "revenue_reason",
    "fundamental_reason",
    "valuation_score",
    "pe_ratio",
    "pb_ratio",
    "dividend_yield",
    "valuation_reason",
    "valuation_warning",
    "financial_score",
    "eps",
    "roe",
    "gross_margin",
    "operating_margin",
    "debt_ratio",
    "financial_reason",
    "financial_warning",
    "event_score",
    "event_reason",
    "event_risk_level",
    "event_blocked",
    "institutional_score",
    "foreign_net_buy",
    "investment_trust_net_buy",
    "dealer_net_buy",
    "institutional_reason",
    "is_candidate",
    "risk_pass",
    "risk_reason",
    "reason",
    "stop_loss_price",
    "suggested_position_pct",
]


@dataclass(frozen=True)
class CandidateExportResult:
    trade_date: pd.Timestamp | None
    candidates: pd.DataFrame
    risk_pass_candidates: pd.DataFrame
    candidates_path: Path | None
    risk_pass_path: Path | None
    data_fetch_status_path: Path | None = None
    data_fetch_status: pd.DataFrame | None = None
    warning: str = ""


def export_latest_candidates(
    engine: Engine,
    output_dir: str | Path = "reports",
    revenue_path: str | Path | None = None,
    valuation_path: str | Path | None = None,
    financials_path: str | Path | None = None,
    events_path: str | Path | None = None,
    institutional_path: str | Path | None = None,
    config: dict | None = None,
) -> CandidateExportResult:
    scores = load_candidate_scores(engine)
    if scores.empty:
        return CandidateExportResult(
            trade_date=None,
            candidates=pd.DataFrame(columns=EXPORT_COLUMNS),
            risk_pass_candidates=pd.DataFrame(columns=EXPORT_COLUMNS),
            candidates_path=None,
            risk_pass_path=None,
            data_fetch_status_path=None,
            data_fetch_status=pd.DataFrame(),
            warning="no scoring data found",
        )

    latest_date = scores["trade_date"].max()
    latest_scores = scores[scores["trade_date"] == latest_date].copy()
    candidates = latest_scores[latest_scores["is_candidate"].astype(int) == 1].copy()
    if candidates.empty:
        return CandidateExportResult(
            trade_date=latest_date,
            candidates=pd.DataFrame(columns=EXPORT_COLUMNS),
            risk_pass_candidates=pd.DataFrame(columns=EXPORT_COLUMNS),
            candidates_path=None,
            risk_pass_path=None,
            data_fetch_status_path=None,
            data_fetch_status=pd.DataFrame(),
            warning=f"no candidate stocks found for {latest_date.date()}",
        )

    active_config = config or load_config()
    candidates, data_fetch_status = _format_candidates(
        candidates,
        revenue_path=revenue_path,
        valuation_path=valuation_path,
        financials_path=financials_path,
        events_path=events_path,
        institutional_path=institutional_path,
        config=active_config,
    )
    risk_pass_candidates = candidates[candidates["risk_pass"].astype(int) == 1].copy()

    report_dir = Path(output_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    date_label = latest_date.strftime("%Y%m%d")
    candidates_path = report_dir / f"candidates_{date_label}.csv"
    risk_pass_path = report_dir / f"risk_pass_candidates_{date_label}.csv"
    data_fetch_status_path = write_data_fetch_status(report_dir, latest_date, data_fetch_status)
    candidates.to_csv(candidates_path, index=False, encoding="utf-8-sig")
    risk_pass_candidates.to_csv(risk_pass_path, index=False, encoding="utf-8-sig")

    return CandidateExportResult(
        trade_date=latest_date,
        candidates=candidates,
        risk_pass_candidates=risk_pass_candidates,
        candidates_path=candidates_path,
        risk_pass_path=risk_pass_path,
        data_fetch_status_path=data_fetch_status_path,
        data_fetch_status=data_fetch_status,
    )


def _format_candidates(
    scores: pd.DataFrame,
    revenue_path: str | Path | None = None,
    valuation_path: str | Path | None = None,
    financials_path: str | Path | None = None,
    events_path: str | Path | None = None,
    institutional_path: str | Path | None = None,
    config: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = scores.sort_values(["total_score", "risk_score"], ascending=[False, False]).reset_index(
        drop=True
    )
    frame["rank"] = frame.index + 1
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y-%m-%d")
    frame["stock_id"] = frame["symbol"].astype(str)
    frame["stock_name"] = frame["name"].astype(str)
    frame["risk_reason"] = frame["risk_reasons"].astype(str)
    frame["reason"] = frame["buy_reasons"].astype(str)
    frame["stop_loss_price"] = frame["stop_loss"]
    frame["is_candidate"] = frame["is_candidate"].astype(int)
    frame["risk_pass"] = frame["risk_pass"].astype(int)
    revenue_scores = score_revenue_for_symbols(
        frame["stock_id"].astype(str).tolist(),
        revenue_path or Path(__file__).resolve().parents[3] / "data" / "monthly_revenue.csv",
    )
    frame = frame.merge(revenue_scores, on="stock_id", how="left", suffixes=("", "_revenue"))
    frame["fundamental_score"] = pd.to_numeric(frame["fundamental_score_revenue"], errors="coerce").fillna(50.0)
    frame["fundamental_reason"] = frame["fundamental_reason"].fillna("基本面資料不足，採中性分數")
    for column in ["revenue_yoy", "revenue_mom", "accumulated_revenue_yoy"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.drop(columns=["fundamental_score_revenue"], errors="ignore")
    multi_factor = apply_multi_factor_scores(
        frame,
        config=(config or {}).get("multi_factor", {}),
        revenue_path=revenue_path,
        valuation_path=valuation_path,
        financials_path=financials_path,
        events_path=events_path,
        institutional_path=institutional_path,
    )
    frame = multi_factor.candidates
    for column in EXPORT_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return frame[EXPORT_COLUMNS].copy(), multi_factor.data_fetch_status
