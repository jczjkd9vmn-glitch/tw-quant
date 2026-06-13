from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.market_regime import evaluate_market_regime


def test_market_regime_does_not_infer_official_index_from_etf_or_etn_names(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    save_daily_prices(engine, _fake_index_like_etf_prices())

    result = evaluate_market_regime(
        engine=engine,
        config=_config(tmp_path),
        config_path=tmp_path / "config.yaml",
        trade_date="2026-06-05",
    )

    assert result.source == "equal_weight_market"
    assert result.frame is not None
    assert result.frame.iloc[0]["source"] == "equal_weight_market"
    assert "缺少正式加權 / 櫃買指數資料" in result.warning


def test_market_regime_uses_official_market_indices_when_available(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    save_daily_prices(engine, _fake_index_like_etf_prices())
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _official_index_prices().to_csv(data_dir / "market_indices.csv", index=False, encoding="utf-8")

    result = evaluate_market_regime(
        engine=engine,
        config=_config(tmp_path),
        config_path=tmp_path / "config.yaml",
        trade_date="2026-06-05",
    )

    assert result.source == "official_index"
    assert result.frame is not None
    assert result.frame.iloc[0]["source"] == "official_index"


def _engine(tmp_path: Path):
    engine = create_db_engine(f"sqlite:///{tmp_path / 'tw_quant.sqlite'}")
    init_db(engine)
    return engine


def _config(tmp_path: Path) -> dict:
    return {
        "database": {"url": f"sqlite:///{tmp_path / 'tw_quant.sqlite'}"},
        "market_regime": {"fallback_to_equal_weight_market": True},
    }


def _fake_index_like_etf_prices() -> pd.DataFrame:
    symbols = {
        "006204": "永豐臺灣加權",
        "00663L": "國泰臺灣加權正2",
        "00664R": "國泰臺灣加權反1",
        "020039": "元大加權N",
    }
    rows = []
    for day_index, date in enumerate(pd.bdate_range("2026-04-20", periods=35), start=1):
        for symbol, name in symbols.items():
            close = 100 + day_index * 0.2
            if symbol == "00664R":
                close = 10 - day_index * 0.03
            if symbol == "020039":
                close = 14 + day_index * 0.01
            rows.append(
                {
                    "trade_date": date.strftime("%Y-%m-%d"),
                    "symbol": symbol,
                    "name": name,
                    "open": close,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 1000,
                    "turnover": close * 1000,
                    "market": "TSE",
                    "source": "test",
                }
            )
    return pd.DataFrame(rows)


def _official_index_prices() -> pd.DataFrame:
    rows = []
    for day_index, date in enumerate(pd.bdate_range("2026-04-20", periods=35), start=1):
        rows.append(
            {
                "trade_date": date.strftime("%Y-%m-%d"),
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 20000 + day_index,
                "high": 20050 + day_index,
                "low": 19950 + day_index,
                "close": 20000 + day_index * 10,
                "source": "test_official",
                "is_official": True,
            }
        )
    return pd.DataFrame(rows)
