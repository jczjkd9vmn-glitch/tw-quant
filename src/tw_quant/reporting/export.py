"""Candidate report export helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from tw_quant.data.database import load_candidate_scores


EXPORT_COLUMNS = [
    "rank",
    "trade_date",
    "stock_id",
    "stock_name",
    "close",
    "total_score",
    "trend_score",
    "momentum_score",
    "fundamental_score",
    "chip_score",
    "risk_score",
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
    warning: str = ""


def export_latest_candidates(
    engine: Engine,
    output_dir: str | Path = "reports",
) -> CandidateExportResult:
    scores = load_candidate_scores(engine)
    if scores.empty:
        return CandidateExportResult(
            trade_date=None,
            candidates=pd.DataFrame(columns=EXPORT_COLUMNS),
            risk_pass_candidates=pd.DataFrame(columns=EXPORT_COLUMNS),
            candidates_path=None,
            risk_pass_path=None,
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
            warning=f"no candidate stocks found for {latest_date.date()}",
        )

    candidates = _format_candidates(candidates)
    risk_pass_candidates = candidates[candidates["risk_pass"].astype(int) == 1].copy()

    report_dir = Path(output_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    date_label = latest_date.strftime("%Y%m%d")
    candidates_path = report_dir / f"candidates_{date_label}.csv"
    risk_pass_path = report_dir / f"risk_pass_candidates_{date_label}.csv"
    candidates.to_csv(candidates_path, index=False, encoding="utf-8-sig")
    risk_pass_candidates.to_csv(risk_pass_path, index=False, encoding="utf-8-sig")

    return CandidateExportResult(
        trade_date=latest_date,
        candidates=candidates,
        risk_pass_candidates=risk_pass_candidates,
        candidates_path=candidates_path,
        risk_pass_path=risk_pass_path,
    )


def _format_candidates(scores: pd.DataFrame) -> pd.DataFrame:
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
    return frame[EXPORT_COLUMNS].copy()
