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
    review_level: str
    review_reason: str


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
    risk_flags = _investment_risk_parts(row)
    data_flags = _data_quality_parts(row)
    reasons: list[str] = []

    if not risk_pass:
        reasons.append("未通過既有風控")
        risk_flags.append("未通過既有風控")
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
        return _make_grade("D", reasons or ["風險條件不適合列入優先觀察"], risk_flags, data_flags)

    data_issues = _data_issue_count(row, data_flags)
    if _to_bool(row.get("is_attention_stock")):
        risk_flags.append("注意股")
        reasons.append("注意股需人工確認")
        return _make_grade("C", reasons, risk_flags, data_flags)
    if confidence < 60:
        reasons.append("資料可信度偏低")
        data_flags.append("資料可信度偏低")
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
        return _make_grade("A", ["技術、多因子、市場情報與資料可信度同步偏強"], risk_flags, data_flags)

    if total_score >= 75 and risk_pass and confidence >= 60 and liquidity >= 50 and event_level != "HIGH":
        if reasons:
            return _make_grade("B", ["技術條件偏強，但" + _join(reasons)], risk_flags, data_flags)
        return _make_grade("B", ["技術條件偏強，仍需人工確認市場與流動性"], risk_flags, data_flags)

    if reasons or data_issues > 0:
        if data_issues > 0:
            reasons.append("部分資料不足")
            data_flags.append("資料不足")
        return _make_grade("C", reasons, risk_flags, data_flags)

    return _make_grade("C", ["分數條件普通，列入觀察即可"], risk_flags, data_flags)


def apply_candidate_grades(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        output = frame.copy()
        for column in [
            "candidate_grade",
            "grade_reason",
            "grade_risk_flags",
            "requires_manual_review",
            "review_level",
            "review_reason",
        ]:
            output[column] = []
        return output
    output = frame.copy()
    grades = output.apply(grade_candidate, axis=1)
    output["candidate_grade"] = [grade.candidate_grade for grade in grades]
    output["grade_reason"] = [grade.grade_reason for grade in grades]
    output["grade_risk_flags"] = [grade.grade_risk_flags for grade in grades]
    output["requires_manual_review"] = [grade.requires_manual_review for grade in grades]
    output["review_level"] = [grade.review_level for grade in grades]
    output["review_reason"] = [grade.review_reason for grade in grades]
    return output


def _make_grade(
    letter: str,
    reasons: list[str],
    risk_flags: list[str],
    data_flags: list[str],
) -> CandidateGrade:
    clean_risks = _dedupe(risk_flags)
    clean_data = _dedupe(data_flags)
    review_level = "RISK_REVIEW" if clean_risks else "DATA_REVIEW" if clean_data else "STANDARD_REVIEW"
    return CandidateGrade(
        letter,
        _join(reasons),
        _join(clean_risks),
        True,
        review_level,
        _review_reason(clean_risks, clean_data),
    )


def _investment_risk_parts(row: pd.Series) -> list[str]:
    if _text(row.get("investment_risk_flags")):
        return _risk_flag_parts(row.get("investment_risk_flags"))
    return [part for part in _risk_flag_parts(row.get("risk_flags")) if not _is_data_issue(part)]


def _data_quality_parts(row: pd.Series) -> list[str]:
    if _text(row.get("data_quality_flags")):
        return _risk_flag_parts(row.get("data_quality_flags"))
    return [part for part in _risk_flag_parts(row.get("risk_flags")) if _is_data_issue(part)]


def _risk_flag_parts(value: object) -> list[str]:
    text = str(value or "").strip()
    if not text or text == "nan":
        return []
    return [part.strip() for part in text.replace("|", "；").split("；") if part.strip()]


def _data_issue_count(row: pd.Series, data_flags: list[str]) -> int:
    issues = len(_dedupe(data_flags))
    for column in ["market_intel_warning", "financial_warning", "valuation_warning", "liquidity_warning"]:
        value = str(row.get(column, "") or "").strip()
        if value and value != "nan" and "資料不足" in value:
            issues += 1
    return issues


def _join(parts: list[str]) -> str:
    return "；".join(_dedupe(parts))


def _dedupe(parts: list[str]) -> list[str]:
    return [part for part in dict.fromkeys(parts) if part]


def _review_reason(risk_flags: list[str], data_flags: list[str]) -> str:
    parts = []
    if risk_flags:
        parts.append("投資風險：" + _join(risk_flags))
    if data_flags:
        parts.append("資料不足：" + _join(data_flags))
    return "；".join(parts) if parts else "一般人工確認"


def _is_data_issue(value: str) -> bool:
    return any(keyword in str(value) for keyword in ["資料不足", "資料可信度偏低", "缺少產業分類", "採中性"])


def _text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


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
