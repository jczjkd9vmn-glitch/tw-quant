"""Candidate A/B/C/D grading for advisory reports."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class CandidateGrade:
    candidate_grade: str
    grade_reason: str
    grade_risk_flags: str
    requires_manual_review: bool


def grade_candidate(row: pd.Series) -> CandidateGrade:
    """Grade one candidate without changing ranking, risk_pass, or trading state."""

    risk_pass = _to_bool(row.get("risk_pass"))
    total_score = _num(row.get("total_score"), 0.0)
    multi_score = _num(row.get("multi_factor_score"), 50.0)
    market_score = _num(row.get("final_market_score"), 50.0)
    confidence = _num(row.get("confidence_score"), 50.0)
    liquidity = _num(row.get("liquidity_score"), 50.0)
    sector = _num(row.get("sector_strength_score"), 50.0)
    event_level = str(row.get("event_risk_level", "") or "").upper()
    risk_flags = _risk_flag_parts(row)
    reasons: list[str] = []

    if not risk_pass:
        reasons.append("未通過既有風控")
    if _to_bool(row.get("is_disposition_stock")):
        reasons.append("處置股")
        risk_flags.append("處置股")
    if event_level == "HIGH" or _to_bool(row.get("event_blocked")):
        reasons.append("高風險事件")
        risk_flags.append("高風險事件")
    if liquidity < 40:
        reasons.append("流動性太低")
        risk_flags.append("流動性太低")
    if (
        not risk_pass
        or _to_bool(row.get("is_disposition_stock"))
        or event_level == "HIGH"
        or _to_bool(row.get("event_blocked"))
        or liquidity < 40
    ):
        return CandidateGrade("D", _join(reasons or ["風險條件不適合列入優先觀察"]), _join(risk_flags), True)

    data_issues = _data_issue_count(row, risk_flags)
    if _to_bool(row.get("is_attention_stock")):
        risk_flags.append("注意股")
        reasons.append("注意股需人工確認")
        return CandidateGrade("C", _join(reasons), _join(risk_flags), True)
    if confidence < 60:
        reasons.append("資料可信度偏低")
        risk_flags.append("資料可信度偏低")
    if liquidity < 50:
        reasons.append("流動性偏低")
        risk_flags.append("流動性偏低")
    if sector < 45:
        reasons.append("產業 / 相對強弱偏弱")
        risk_flags.append("產業弱勢")

    if (
        total_score >= 80
        and multi_score >= 70
        and market_score >= 65
        and confidence >= 70
        and liquidity >= 60
        and sector >= 50
        and data_issues == 0
        and not risk_flags
    ):
        return CandidateGrade("A", "技術、多因子、市場情報與資料可信度同步偏強", "", True)

    if total_score >= 75 and risk_pass and confidence >= 60 and liquidity >= 50 and event_level != "HIGH":
        if reasons:
            return CandidateGrade("B", "技術條件偏強，但" + _join(reasons), _join(risk_flags), True)
        return CandidateGrade("B", "技術條件偏強，仍需人工確認市場與流動性", _join(risk_flags), True)

    if reasons or data_issues > 0:
        if data_issues > 0:
            reasons.append("部分資料不足")
            risk_flags.append("資料不足")
        return CandidateGrade("C", _join(reasons), _join(risk_flags), True)

    return CandidateGrade("C", "分數條件普通，列入觀察即可", _join(risk_flags), True)


def apply_candidate_grades(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        output = frame.copy()
        for column in ["candidate_grade", "grade_reason", "grade_risk_flags", "requires_manual_review"]:
            output[column] = []
        return output
    output = frame.copy()
    grades = output.apply(grade_candidate, axis=1)
    output["candidate_grade"] = [grade.candidate_grade for grade in grades]
    output["grade_reason"] = [grade.grade_reason for grade in grades]
    output["grade_risk_flags"] = [grade.grade_risk_flags for grade in grades]
    output["requires_manual_review"] = [grade.requires_manual_review for grade in grades]
    return output


def _risk_flag_parts(row: pd.Series) -> list[str]:
    text = str(row.get("risk_flags", "") or "").strip()
    if not text or text == "nan":
        return []
    return [part.strip() for part in text.replace("|", "；").split("；") if part.strip()]


def _data_issue_count(row: pd.Series, risk_flags: list[str]) -> int:
    issues = 0
    issue_text = "；".join(risk_flags)
    if "資料不足" in issue_text:
        issues += 1
    for column in ["market_intel_warning", "financial_warning", "valuation_warning", "liquidity_warning"]:
        value = str(row.get(column, "") or "").strip()
        if value and value != "nan" and "資料不足" in value:
            issues += 1
    return issues


def _join(parts: list[str]) -> str:
    clean = [part for part in dict.fromkeys(parts) if part]
    return "；".join(clean)


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}


def _num(value: object, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return parsed
