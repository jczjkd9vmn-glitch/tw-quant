from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.reporting.candidate_forward_returns import generate_candidate_forward_returns


def test_candidate_forward_returns_calculates_5d_and_20d_labels(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    dates = _business_dates("2026-06-01", periods=21)
    closes = [100.0] + [101.0 + index for index in range(1, 5)] + [110.0]
    closes.extend([111.0 + index for index in range(6, 20)])
    closes.append(130.0)
    save_daily_prices(engine, _price_rows(dates, "2330", closes))
    _write_market_indices(
        tmp_path,
        dates,
        [1000.0]
        + [1001.0 + index for index in range(1, 5)]
        + [1050.0]
        + [1051.0 + index for index in range(6, 20)]
        + [1100.0],
    )
    _write_candidate(tmp_path, "2026-06-01", score=80, market_regime_score=65)

    result = generate_candidate_forward_returns(
        engine,
        reports_dir=tmp_path,
        trade_date=dates[-1],
        current_threshold=60,
    )

    row = result.frame.iloc[0]
    assert row["forward_return_5d"] == pytest.approx(0.10)
    assert row["forward_return_20d"] == pytest.approx(0.30)
    assert row["benchmark_return_5d"] == pytest.approx(0.05)
    assert row["benchmark_return_20d"] == pytest.approx(0.10)
    assert row["excess_return_5d"] == pytest.approx(0.05)
    assert row["excess_return_20d"] == pytest.approx(0.20)
    assert row["data_sufficiency_status"] == "OBSERVATION_ONLY"
    assert result.coverage_5d == 1.0
    assert result.coverage_20d == 1.0
    assert (tmp_path / f"candidate_forward_returns_{dates[-1].strftime('%Y%m%d')}.csv").exists()


def test_candidate_forward_returns_marks_insufficient_when_future_prices_missing(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    dates = _business_dates("2026-06-01", periods=4)
    save_daily_prices(engine, _price_rows(dates, "2330", [100.0, 101.0, 102.0, 103.0]))
    _write_market_indices(tmp_path, dates, [1000.0, 1001.0, 1002.0, 1003.0])
    _write_candidate(tmp_path, "2026-06-01", score=80, market_regime_score=65)

    result = generate_candidate_forward_returns(
        engine,
        reports_dir=tmp_path,
        trade_date=dates[-1],
        current_threshold=60,
    )

    row = result.frame.iloc[0]
    assert pd.isna(row["forward_return_5d"])
    assert pd.isna(row["forward_return_20d"])
    assert row["forward_return_5d_status"] == "DATA_INSUFFICIENT"
    assert row["forward_return_20d_status"] == "DATA_INSUFFICIENT"
    assert row["data_sufficiency_status"] == "DATA_INSUFFICIENT"
    assert result.status == "DATA_INSUFFICIENT"


def test_candidate_forward_returns_flags_market_regime_blocked_candidates(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    dates = _business_dates("2026-06-01", periods=21)
    save_daily_prices(engine, _price_rows(dates, "2330", [100.0 + index for index in range(21)]))
    _write_market_indices(tmp_path, dates, [1000.0 + index for index in range(21)])
    _write_candidate(tmp_path, "2026-06-01", score=80, market_regime_score=53)

    result = generate_candidate_forward_returns(
        engine,
        reports_dir=tmp_path,
        trade_date=dates[-1],
        current_threshold=60,
    )

    assert bool(result.frame.iloc[0]["blocked_by_market_regime"]) is True


def _engine(tmp_path: Path):
    engine = create_db_engine(f"sqlite:///{(tmp_path / 'tw_quant.sqlite').as_posix()}")
    init_db(engine)
    return engine


def _business_dates(start: str, *, periods: int) -> list[date]:
    return [value.date() for value in pd.bdate_range(start=start, periods=periods)]


def _price_rows(dates: list[date], symbol: str, closes: list[float]) -> pd.DataFrame:
    rows = []
    for trade_date, close in zip(dates, closes):
        rows.append(
            {
                "trade_date": trade_date.isoformat(),
                "symbol": symbol,
                "name": "台積電",
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000_000,
                "turnover": close * 1_000_000,
                "market": "TSE",
                "source": "TEST",
            }
        )
    return pd.DataFrame(rows)


def _write_candidate(tmp_path: Path, trade_date: str, *, score: float, market_regime_score: float) -> None:
    label = pd.to_datetime(trade_date).strftime("%Y%m%d")
    pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "stock_id": "2330",
                "stock_name": "台積電",
                "total_score": score,
                "market_regime_score": market_regime_score,
            }
        ]
    ).to_csv(tmp_path / f"candidates_{label}.csv", index=False, encoding="utf-8")
    pd.DataFrame([{"trade_date": trade_date, "market_regime_score": market_regime_score}]).to_csv(
        tmp_path / f"market_regime_{label}.csv",
        index=False,
        encoding="utf-8",
    )


def _write_market_indices(tmp_path: Path, dates: list[date], closes: list[float]) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": trade_date.isoformat(),
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "close": close,
                "is_official": True,
            }
            for trade_date, close in zip(dates, closes)
        ]
    ).to_csv(tmp_path / "market_indices.csv", index=False, encoding="utf-8")
