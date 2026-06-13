from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.data.trading_calendar import filter_trading_days, is_trading_day, latest_trading_day


def test_weekend_is_not_trading_day() -> None:
    assert is_trading_day("2026-06-06") is False
    assert is_trading_day("2026-06-05") is True


def test_calendar_csv_can_override_weekday(tmp_path: Path) -> None:
    calendar = tmp_path / "trading_calendar.csv"
    pd.DataFrame(
        [
            {"date": "2026-06-05", "is_trading_day": False, "reason": "holiday"},
            {"date": "2026-06-06", "is_trading_day": True, "reason": "makeup trading"},
        ]
    ).to_csv(calendar, index=False, encoding="utf-8")

    assert is_trading_day("2026-06-05", calendar) is False
    assert is_trading_day("2026-06-06", calendar) is True


def test_filter_trading_days_and_latest_trading_day_skip_weekends() -> None:
    frame = pd.DataFrame(
        [
            {"trade_date": "2026-06-05", "symbol": "0050"},
            {"trade_date": "2026-06-06", "symbol": "0050"},
        ]
    )

    filtered = filter_trading_days(frame)

    assert filtered["trade_date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-06-05"]
    assert latest_trading_day({"2026-06-05", "2026-06-06"}, end_date="2026-06-06").strftime("%Y-%m-%d") == "2026-06-05"
