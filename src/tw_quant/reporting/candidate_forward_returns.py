"""Observation-only forward return labels for candidate diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from tw_quant.data.database import load_price_history
from tw_quant.reporting.benchmark import select_official_benchmark_history


FORWARD_WINDOWS = (5, 20)

CANDIDATE_FORWARD_RETURN_COLUMNS = [
    "trade_date",
    "symbol",
    "name",
    "candidate_score",
    "market_regime_score",
    "official_threshold",
    "blocked_by_market_regime",
    "close_on_trade_date",
    "close_plus_5d",
    "close_plus_20d",
    "forward_return_5d",
    "forward_return_20d",
    "benchmark_return_5d",
    "benchmark_return_20d",
    "excess_return_5d",
    "excess_return_20d",
    "forward_return_5d_status",
    "forward_return_20d_status",
    "data_sufficiency_status",
    "is_observation_only",
]


@dataclass(frozen=True)
class CandidateForwardReturnsResult:
    trade_date: pd.Timestamp | None
    frame: pd.DataFrame
    output_path: Path
    status: str = "OK"
    warning: str = ""

    @property
    def coverage_5d(self) -> float:
        return _coverage(self.frame, "forward_return_5d")

    @property
    def coverage_20d(self) -> float:
        return _coverage(self.frame, "forward_return_20d")


def generate_candidate_forward_returns(
    engine: Engine,
    reports_dir: str | Path = "reports",
    trade_date: str | pd.Timestamp | None = None,
    *,
    current_threshold: int = 60,
) -> CandidateForwardReturnsResult:
    """Generate candidate forward-return labels from local price/index data.

    The output is a report-only label snapshot. It does not modify strategy
    thresholds, candidate selection, guardrails, exits, or orders.
    """

    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    selected_date = _resolve_trade_date(report_dir, trade_date)
    date_label = (selected_date or pd.Timestamp.today()).strftime("%Y%m%d")
    output_path = report_dir / f"candidate_forward_returns_{date_label}.csv"

    candidates = _candidate_rows(report_dir, selected_date)
    if candidates.empty:
        frame = pd.DataFrame(columns=CANDIDATE_FORWARD_RETURN_COLUMNS)
        frame.to_csv(output_path, index=False, encoding="utf-8")
        return CandidateForwardReturnsResult(
            trade_date=selected_date,
            frame=frame,
            output_path=output_path,
            status="DATA_INSUFFICIENT",
            warning="no candidate reports found",
        )

    prices = _price_history(engine, selected_date)
    benchmark = _benchmark_history(report_dir, selected_date)
    regime_scores = _market_regime_scores(report_dir, selected_date)
    rejected_keys = _market_regime_rejected_keys(report_dir, selected_date)

    rows = []
    for row in candidates.to_dict(orient="records"):
        trade_ts = pd.to_datetime(row.get("trade_date"), errors="coerce")
        if pd.isna(trade_ts):
            continue
        trade_ts = trade_ts.normalize()
        symbol = str(row.get("symbol", "") or "").strip()
        market_score = _first_number(row.get("market_regime_score"), regime_scores.get(trade_ts))
        stock_prices = prices.get(symbol, pd.DataFrame())
        close_on_trade_date, forward_prices, forward_returns = _stock_forward_returns(
            stock_prices,
            trade_ts,
        )
        benchmark_returns = _benchmark_forward_returns(benchmark, trade_ts)
        excess_returns = {
            window: _sub_or_none(forward_returns.get(window), benchmark_returns.get(window))
            for window in FORWARD_WINDOWS
        }
        forward_status = {
            window: "OBSERVATION_ONLY" if forward_returns.get(window) is not None else "DATA_INSUFFICIENT"
            for window in FORWARD_WINDOWS
        }
        data_status = (
            "OBSERVATION_ONLY"
            if all(forward_status[window] == "OBSERVATION_ONLY" for window in FORWARD_WINDOWS)
            else "DATA_INSUFFICIENT"
        )
        date_text = _date_text(trade_ts)
        rows.append(
            {
                "trade_date": date_text,
                "symbol": symbol,
                "name": str(row.get("name", "") or "").strip(),
                "candidate_score": _first_number(row.get("candidate_score"), row.get("total_score")),
                "market_regime_score": market_score,
                "official_threshold": current_threshold,
                "blocked_by_market_regime": bool(
                    (market_score is not None and market_score < current_threshold)
                    or (date_text, symbol) in rejected_keys
                ),
                "close_on_trade_date": close_on_trade_date,
                "close_plus_5d": forward_prices.get(5),
                "close_plus_20d": forward_prices.get(20),
                "forward_return_5d": forward_returns.get(5),
                "forward_return_20d": forward_returns.get(20),
                "benchmark_return_5d": benchmark_returns.get(5),
                "benchmark_return_20d": benchmark_returns.get(20),
                "excess_return_5d": excess_returns.get(5),
                "excess_return_20d": excess_returns.get(20),
                "forward_return_5d_status": forward_status[5],
                "forward_return_20d_status": forward_status[20],
                "data_sufficiency_status": data_status,
                "is_observation_only": True,
            }
        )

    frame = pd.DataFrame(rows, columns=CANDIDATE_FORWARD_RETURN_COLUMNS)
    if not frame.empty:
        frame = frame.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
    frame.to_csv(output_path, index=False, encoding="utf-8")

    status = "OK" if not frame.empty and frame["forward_return_20d"].notna().any() else "DATA_INSUFFICIENT"
    warning = "" if status == "OK" else "forward return labels are not fully available"
    return CandidateForwardReturnsResult(
        trade_date=selected_date,
        frame=frame,
        output_path=output_path,
        status=status,
        warning=warning,
    )


def _candidate_rows(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    frame = _read_all_reports(report_dir, "candidates_*.csv", dtype={"stock_id": str, "symbol": str})
    if frame.empty:
        return pd.DataFrame()
    frame = _with_trade_date(frame)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
    frame = frame.dropna(subset=["trade_date"])
    if selected_date is not None:
        frame = frame[frame["trade_date"] <= selected_date]
    if frame.empty:
        return pd.DataFrame()

    output = frame.copy()
    output["symbol"] = _string_series(output, "symbol").where(
        _string_series(output, "symbol") != "",
        _string_series(output, "stock_id"),
    )
    output["name"] = _string_series(output, "name").where(
        _string_series(output, "name") != "",
        _string_series(output, "stock_name"),
    )
    output["candidate_score"] = pd.to_numeric(_series(output, "candidate_score"), errors="coerce")
    output["candidate_score"] = output["candidate_score"].where(
        output["candidate_score"].notna(),
        pd.to_numeric(_series(output, "total_score"), errors="coerce"),
    )
    keep = [
        "trade_date",
        "symbol",
        "name",
        "candidate_score",
        "total_score",
        "market_regime_score",
        "_source_file",
    ]
    for column in keep:
        if column not in output.columns:
            output[column] = None
    output = output[keep].copy()
    output = output[output["symbol"].astype(str).str.strip() != ""]
    return output.drop_duplicates(["trade_date", "symbol"], keep="last").reset_index(drop=True)


def _price_history(engine: Engine, selected_date: pd.Timestamp | None) -> dict[str, pd.DataFrame]:
    end_date = _date_text(selected_date) if selected_date is not None else None
    try:
        history = load_price_history(engine, end_date=end_date)
    except Exception:
        return {}
    if history.empty or not {"trade_date", "symbol", "close"}.issubset(history.columns):
        return {}
    history = history.copy()
    history["trade_date"] = pd.to_datetime(history["trade_date"], errors="coerce").dt.normalize()
    history["symbol"] = history["symbol"].astype(str).str.strip()
    history["close"] = pd.to_numeric(history["close"], errors="coerce")
    history = history.dropna(subset=["trade_date", "close"])
    history = history[(history["symbol"] != "") & (history["close"] > 0)]
    history = history.sort_values(["symbol", "trade_date"]).drop_duplicates(["symbol", "trade_date"], keep="last")
    return {symbol: group.reset_index(drop=True) for symbol, group in history.groupby("symbol", sort=False)}


def _benchmark_history(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    snapshot = select_official_benchmark_history(report_dir, selected_date)
    frame = snapshot.get("frame", pd.DataFrame()) if isinstance(snapshot, dict) else pd.DataFrame()
    if frame.empty or not {"trade_date", "close"}.issubset(frame.columns):
        return pd.DataFrame(columns=["trade_date", "close"])
    output = frame.copy()
    output["trade_date"] = pd.to_datetime(output["trade_date"], errors="coerce").dt.normalize()
    output["close"] = pd.to_numeric(output["close"], errors="coerce")
    output = output.dropna(subset=["trade_date", "close"])
    output = output[output["close"] > 0]
    return output.sort_values("trade_date").drop_duplicates("trade_date", keep="last").reset_index(drop=True)


def _stock_forward_returns(
    prices: pd.DataFrame,
    trade_date: pd.Timestamp,
) -> tuple[float | None, dict[int, float | None], dict[int, float | None]]:
    if prices.empty or "trade_date" not in prices.columns or "close" not in prices.columns:
        return None, {window: None for window in FORWARD_WINDOWS}, {window: None for window in FORWARD_WINDOWS}
    data = prices.sort_values("trade_date").reset_index(drop=True)
    matches = data.index[data["trade_date"] == trade_date].tolist()
    if not matches:
        return None, {window: None for window in FORWARD_WINDOWS}, {window: None for window in FORWARD_WINDOWS}
    index = matches[-1]
    base = _num(data.loc[index, "close"])
    if base is None or abs(base) < 0.000001:
        return None, {window: None for window in FORWARD_WINDOWS}, {window: None for window in FORWARD_WINDOWS}
    future_prices: dict[int, float | None] = {}
    future_returns: dict[int, float | None] = {}
    for window in FORWARD_WINDOWS:
        future_index = index + window
        future_close = _num(data.loc[future_index, "close"]) if future_index < len(data) else None
        future_prices[window] = future_close
        future_returns[window] = None if future_close is None else float(future_close / base - 1.0)
    return base, future_prices, future_returns


def _benchmark_forward_returns(frame: pd.DataFrame, trade_date: pd.Timestamp) -> dict[int, float | None]:
    if frame.empty or "trade_date" not in frame.columns or "close" not in frame.columns:
        return {window: None for window in FORWARD_WINDOWS}
    data = frame.sort_values("trade_date").reset_index(drop=True)
    matches = data.index[data["trade_date"] == trade_date].tolist()
    if not matches:
        return {window: None for window in FORWARD_WINDOWS}
    index = matches[-1]
    base = _num(data.loc[index, "close"])
    if base is None or abs(base) < 0.000001:
        return {window: None for window in FORWARD_WINDOWS}
    returns = {}
    for window in FORWARD_WINDOWS:
        future_index = index + window
        future_close = _num(data.loc[future_index, "close"]) if future_index < len(data) else None
        returns[window] = None if future_close is None else float(future_close / base - 1.0)
    return returns


def _market_regime_scores(report_dir: Path, selected_date: pd.Timestamp | None) -> dict[pd.Timestamp, float]:
    frames = []
    for pattern, column in [
        ("daily_summary_*.csv", "market_regime_score"),
        ("market_regime_*.csv", "market_regime_score"),
    ]:
        frame = _read_all_reports(report_dir, pattern)
        if frame.empty or column not in frame.columns:
            continue
        frame = _with_trade_date(frame)
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(subset=["trade_date", column])
        if selected_date is not None:
            frame = frame[frame["trade_date"] <= selected_date]
        frames.append(frame[["trade_date", column, "_source_file"]])
    if not frames:
        return {}
    combined = pd.concat(frames, ignore_index=True).sort_values(["trade_date", "_source_file"])
    combined = combined.drop_duplicates("trade_date", keep="last")
    return {row.trade_date: float(row.market_regime_score) for row in combined.itertuples(index=False)}


def _market_regime_rejected_keys(report_dir: Path, selected_date: pd.Timestamp | None) -> set[tuple[str, str]]:
    frame = _read_all_reports(report_dir, "rejected_paper_orders_*.csv", dtype={"stock_id": str, "symbol": str})
    if frame.empty:
        return set()
    frame = _with_trade_date(frame)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
    frame = frame.dropna(subset=["trade_date"])
    if selected_date is not None:
        frame = frame[frame["trade_date"] <= selected_date]
    if frame.empty:
        return set()
    reason = _joined_text(frame, ["rejection_reason", "rejected_reason", "skipped_reason", "warning"])
    market_rejected = reason.str.contains("market_regime|市場環境|新增持倉門檻", case=False, na=False)
    data = frame[market_rejected].copy()
    if data.empty:
        return set()
    data["symbol"] = _string_series(data, "symbol").where(
        _string_series(data, "symbol") != "",
        _string_series(data, "stock_id"),
    )
    return {
        (_date_text(row.trade_date), str(row.symbol).strip())
        for row in data[["trade_date", "symbol"]].dropna().itertuples(index=False)
        if str(row.symbol).strip()
    }


def _read_all_reports(report_dir: Path, pattern: str, dtype: dict[str, object] | None = None) -> pd.DataFrame:
    frames = []
    for path in sorted(report_dir.glob(pattern)):
        try:
            frame = pd.read_csv(path, encoding="utf-8", dtype=dtype)
        except (OSError, pd.errors.EmptyDataError, UnicodeDecodeError):
            continue
        frame = frame.copy()
        frame["_source_file"] = path.name
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def _with_trade_date(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    if "trade_date" not in output.columns:
        output["trade_date"] = output.get("_source_file", "").astype(str).str.extract(r"(\d{8})", expand=False)
    return output


def _resolve_trade_date(report_dir: Path, trade_date: str | pd.Timestamp | None) -> pd.Timestamp | None:
    if trade_date is not None:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        return None if pd.isna(parsed) else parsed.normalize()
    for pattern in ["daily_summary_*.csv", "market_regime_*.csv", "candidates_*.csv"]:
        files = sorted(report_dir.glob(pattern))
        if not files:
            continue
        try:
            frame = pd.read_csv(files[-1], encoding="utf-8", nrows=1)
        except (OSError, pd.errors.EmptyDataError, UnicodeDecodeError):
            continue
        if "trade_date" in frame.columns and not frame.empty:
            parsed = pd.to_datetime(frame.iloc[0].get("trade_date"), errors="coerce")
            if pd.notna(parsed):
                return parsed.normalize()
        parsed = pd.to_datetime(files[-1].stem[-8:], format="%Y%m%d", errors="coerce")
        if pd.notna(parsed):
            return parsed.normalize()
    return None


def _coverage(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    total = len(frame)
    if total <= 0:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").notna().sum() / total)


def _joined_text(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    parts = []
    for column in columns:
        if column in frame.columns:
            value = frame[column]
            if isinstance(value, pd.DataFrame):
                value = value.iloc[:, 0]
            parts.append(value.fillna("").astype(str))
    if not parts:
        return pd.Series("", index=frame.index)
    result = parts[0]
    for part in parts[1:]:
        result = result + " " + part
    return result


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return _series(frame, column).fillna("").astype(str).str.strip()


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NA, index=frame.index)
    value = frame[column]
    if isinstance(value, pd.DataFrame):
        value = value.iloc[:, 0]
    return value


def _first_number(*values: object) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _num(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _sub_or_none(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return float(left - right)


def _date_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")
