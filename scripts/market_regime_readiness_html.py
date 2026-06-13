from __future__ import annotations

from html import escape, unescape
from pathlib import Path
import re
from typing import Iterable

import pandas as pd


SECTION_ID = "market-regime-threshold-optimization"


def patch_generated_market_regime_readiness_html(report_dir: Path, docs_dir: Path | None = None) -> None:
    optimization = _read_latest_csv(report_dir, "market_regime_threshold_optimization_*.csv")
    if optimization.empty:
        return

    candidate_forward_returns = _read_latest_csv(report_dir, "candidate_forward_returns_*.csv")
    section = _render_readiness_section(optimization, candidate_forward_returns)
    for html_path in _target_html_paths(report_dir, docs_dir):
        if html_path.exists():
            _replace_section(html_path, section)


def _target_html_paths(report_dir: Path, docs_dir: Path | None) -> Iterable[Path]:
    yield report_dir / "index.html"
    if docs_dir is not None:
        yield docs_dir / "index.html"


def _read_latest_csv(report_dir: Path, pattern: str) -> pd.DataFrame:
    matches = sorted(report_dir.glob(pattern))
    if not matches:
        return pd.DataFrame()
    fallback = pd.DataFrame()
    for path in reversed(matches):
        frame = pd.read_csv(path)
        if fallback.empty:
            fallback = frame
        if "readiness_status" in frame.columns:
            return frame
    return fallback


def _replace_section(html_path: Path, section: str) -> None:
    html = html_path.read_text(encoding="utf-8")
    pattern = re.compile(
        rf'<section[^>]*id="{re.escape(SECTION_ID)}"[^>]*>.*?</section>',
        re.DOTALL,
    )
    if pattern.search(html):
        html = pattern.sub(section, html, count=1)
    else:
        html = html.replace("</body>", section + "\n</body>")
    tmp_path = html_path.with_name(html_path.name + ".tmp")
    tmp_path.write_text(html, encoding="utf-8", newline="\n")
    tmp_path.replace(html_path)


def _render_readiness_section(optimization: pd.DataFrame, candidate_forward_returns: pd.DataFrame) -> str:
    row = _threshold_row(optimization)
    readiness_status = str(row.get("readiness_status") or "OBSERVATION_ONLY")
    readiness_reason = str(row.get("readiness_reason") or "")
    can_recommend_threshold_change = _truthy(row.get("can_recommend_threshold_change"))

    cards = [
        _card(_t("&#x76ee;&#x524d;&#x6b63;&#x5f0f;&#x9580;&#x6abb;"), "60"),
        _card("5d label coverage", _pct(row.get("label_5d_coverage"))),
        _card("20d label coverage", _pct(row.get("label_20d_coverage"))),
        _card("Validation sample count", _validation_count(row)),
        _card("Readiness status", _status_label(readiness_status)),
        _card("Readiness reason", readiness_reason or "-"),
        _card(_t("&#x662f;&#x5426;&#x5141;&#x8a31;&#x6b63;&#x5f0f;&#x8abf;&#x6574;&#x9580;&#x6abb;"), _yes_no(can_recommend_threshold_change)),
        _card("Dynamic exposure proxy", _dynamic_exposure_text(optimization)),
    ]
    return (
        f'<section id="{SECTION_ID}" class="market-regime-threshold-section">'
        f"<h2>{_t('Market Regime &#x9580;&#x6abb;&#x6700;&#x4f73;&#x5316;&#x89c0;&#x5bdf;')}</h2>"
        + _observation_notice(readiness_status, readiness_reason, can_recommend_threshold_change)
        + '<div class="cards">'
        + "".join(cards)
        + "</div>"
        + _forward_label_cards(candidate_forward_returns)
        + _blocked_forward_summary(candidate_forward_returns)
        + f"<p class=\"note\">{_t('observation-only &#x9580;&#x6abb;&#x8a3a;&#x65b7;')}</p>"
        + _readiness_table(optimization)
        + "</section>"
    )


def _threshold_row(optimization: pd.DataFrame) -> dict[str, object]:
    matches = optimization[optimization["threshold"].astype(str) == "60"] if "threshold" in optimization else pd.DataFrame()
    if matches.empty:
        matches = optimization
    return matches.iloc[0].to_dict()


def _observation_notice(readiness_status: str, readiness_reason: str, can_recommend_threshold_change: bool) -> str:
    if can_recommend_threshold_change:
        return (
            '<p class="top-notice benchmark-ok"><strong>Readiness gate passed</strong>'
            f"<span>{_t('20d coverage &#x8207; validation sample count &#x5df2;&#x9054;&#x9580;&#x6abb;')}"
            f"{_t('&#xff1b;&#x4ecd;&#x50c5;&#x70ba;&#x5831;&#x8868;&#x89c0;&#x5bdf;&#xff0c;&#x4e0d;&#x6703;&#x81ea;&#x52d5;&#x4fee;&#x6539; config.yaml&#x3002;')}</span></p>"
        )
    if "DATA_INSUFFICIENT_20D" in readiness_status or "20d" in readiness_reason:
        return (
            '<p class="top-notice benchmark-warning">'
            f"<strong>{_t('20d &#x6a23;&#x672c;&#x4e0d;&#x8db3;')}</strong>"
            f"<span>{_t('&#x76ee;&#x524d;&#x50c5;&#x4f9b; observation-only')}"
            f"{_t('&#xff1b;&#x4e0d;&#x53ef;&#x4f5c;&#x70ba;&#x6b63;&#x5f0f;&#x964d;&#x4f4e;&#x9580;&#x6abb;&#x4f9d;&#x64da;&#x3002;')}</span></p>"
        )
    return (
        f"<p class=\"top-notice benchmark-warning\"><strong>{_t('Observation only / proxy &#x8a3a;&#x65b7;')}</strong>"
        f"<span>{_t('Validation &#x6a23;&#x672c;&#x4e0d;&#x8db3;&#xff0c;&#x76ee;&#x524d;&#x50c5;&#x4f9b; observation-only&#x3002;')}</span></p>"
    )


def _readiness_table(optimization: pd.DataFrame) -> str:
    columns = [
        "threshold",
        "label_5d_coverage",
        "label_20d_coverage",
        "validation_eligible_sample_count",
        "validation_blocked_sample_count",
        "readiness_status",
        "readiness_reason",
        "can_recommend_threshold_change",
        "can_recommend_dynamic_exposure",
        "recommendation",
    ]
    visible = [column for column in columns if column in optimization.columns]
    if not visible:
        return ""
    head = "".join(f"<th>{escape(column)}</th>" for column in visible)
    rows = []
    for record in optimization[visible].head(20).to_dict("records"):
        rows.append("<tr>" + "".join(f"<td>{escape(_format_cell(column, record.get(column)))}</td>" for column in visible) + "</tr>")
    return f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


def _card(label: str, value: object) -> str:
    return f'<div class="card"><span>{escape(str(label))}</span><strong>{escape(str(value))}</strong></div>'


def _status_label(status: str) -> str:
    labels = {
        "READY_FOR_5D_OBSERVATION": "READY_FOR_5D_OBSERVATION",
        "READY_FOR_20D_OBSERVATION": "READY_FOR_20D_OBSERVATION",
        "DATA_INSUFFICIENT_20D": _t('20d &#x6a23;&#x672c;&#x4e0d;&#x8db3;'),
        "DATA_INSUFFICIENT_VALIDATION": _t('Validation &#x6a23;&#x672c;&#x4e0d;&#x8db3;'),
        "OBSERVATION_ONLY": "OBSERVATION_ONLY",
    }
    return labels.get(status, status)


def _validation_count(row: dict[str, object]) -> str:
    eligible = _int_text(row.get("validation_eligible_sample_count"))
    blocked = _int_text(row.get("validation_blocked_sample_count"))
    return f"eligible {eligible} / blocked {blocked}"


def _forward_label_cards(frame: pd.DataFrame) -> str:
    total = len(frame)
    valid_5d = _valid_count(frame, "forward_return_5d")
    valid_20d = _valid_count(frame, "forward_return_20d")
    cards = [
        _card("Forward return labels", "available" if total else "missing"),
        _card("5d label coverage", _coverage_text(valid_5d, total)),
        _card("20d label coverage", _coverage_text(valid_20d, total)),
    ]
    return '<div class="cards">' + "".join(cards) + "</div>"


def _blocked_forward_summary(frame: pd.DataFrame) -> str:
    if frame.empty or "blocked_by_market_regime" not in frame:
        return ""
    blocked = frame[frame["blocked_by_market_regime"].apply(_truthy)]
    return (
        f"<h3>{_t('&#x88ab; market regime &#x64cb;&#x4e0b;&#x5019;&#x9078;&#x80a1; forward return &#x6458;&#x8981;')}</h3>"
        f"<p class=\"note\">blocked samples: {len(blocked)}</p>"
    )


def _valid_count(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame:
        return 0
    return int(pd.to_numeric(frame[column], errors="coerce").notna().sum())


def _coverage_text(valid_count: int, total: int) -> str:
    coverage = valid_count / total if total else 0.0
    return f"{valid_count}/{total} ({coverage:.1%})"


def _dynamic_exposure_text(optimization: pd.DataFrame) -> str:
    if "threshold" not in optimization:
        return "-"
    matches = optimization[optimization["threshold"].astype(str) == "DYNAMIC_EXPOSURE"]
    if matches.empty:
        return "-"
    row = matches.iloc[0]
    for column in ("dynamic_estimated_excess_return", "estimated_excess_return", "dynamic_exposure_pct"):
        if column in row and not pd.isna(row[column]):
            return _format_cell(column, row[column])
    return "-"


def _format_cell(column: str, value: object) -> str:
    if column in {"label_5d_coverage", "label_20d_coverage"}:
        return _pct(value)
    if column in {"can_recommend_threshold_change", "can_recommend_dynamic_exposure"}:
        return "True" if _truthy(value) else "False"
    if value is None or pd.isna(value):
        return "-"
    return str(value)


def _pct(value: object) -> str:
    try:
        return f"{float(value):.2%}"
    except (TypeError, ValueError):
        return "-"


def _int_text(value: object) -> str:
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return "0"


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _yes_no(value: bool) -> str:
    return _t("&#x662f;") if value else _t("&#x5426;")


def _t(value: str) -> str:
    return unescape(value)
