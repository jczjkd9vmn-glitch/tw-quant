from __future__ import annotations

from pathlib import Path

import pandas as pd

import scripts.fetch_multi_factor_data as fetch_module
from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.data_sources.base import ProviderResult
from tw_quant.data_sources.local_derived_provider import LocalDerivedProvider
from tw_quant.scoring.multi_factor import apply_multi_factor_scores


def _price_rows(turnover: bool = True) -> pd.DataFrame:
    rows = []
    dates = pd.date_range("2026-04-15", periods=21, freq="B")
    for i, trade_date in enumerate(dates):
        rows.append(
            {
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "symbol": "2330",
                "name": "台積電",
                "open": 100 + i,
                "high": 101 + i,
                "low": 99 + i,
                "close": 100 + i,
                "volume": 1_000_000,
                "turnover": 100_000_000 if turnover else None,
                "market": "TSE",
                "source": "test",
            }
        )
        rows.append(
            {
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "symbol": "9999",
                "name": "低量股",
                "open": 50,
                "high": 51,
                "low": 49,
                "close": 50,
                "volume": 50_000,
                "turnover": 2_000_000 if turnover else None,
                "market": "TSE",
                "source": "test",
            }
        )
    return pd.DataFrame(rows)


def _engine(tmp_path: Path, turnover: bool = True):
    engine = create_db_engine(f"sqlite:///{tmp_path / 'tw_quant.sqlite'}")
    init_db(engine)
    save_daily_prices(engine, _price_rows(turnover=turnover))
    return engine


def test_liquidity_provider_derives_scores_from_sqlite(tmp_path: Path) -> None:
    provider = LocalDerivedProvider(engine=_engine(tmp_path))

    result = provider.fetch_liquidity("20260515")

    assert result.status == "OK"
    assert len(result.data) == 2
    row = result.data[result.data["stock_id"] == "2330"].iloc[0]
    assert row["avg_volume_20d"] == 1_000_000
    assert row["avg_turnover_20d"] == 100_000_000
    assert row["liquidity_score"] >= 90
    low = result.data[result.data["stock_id"] == "9999"].iloc[0]
    assert "流動性偏低" in low["liquidity_warning"]


def test_liquidity_provider_estimates_turnover_when_missing(tmp_path: Path) -> None:
    provider = LocalDerivedProvider(engine=_engine(tmp_path, turnover=False))

    result = provider.fetch_liquidity("20260515")

    row = result.data[result.data["stock_id"] == "2330"].iloc[0]
    assert row["avg_turnover_20d"] > 0
    assert row["latest_turnover"] == row["latest_volume"] * 120


def test_sector_strength_provider_falls_back_to_market_relative_without_industry(tmp_path: Path) -> None:
    provider = LocalDerivedProvider(engine=_engine(tmp_path))

    result = provider.fetch_sector_strength("20260515")

    assert result.status == "OK_WITH_FALLBACK"
    assert "缺少產業分類" in result.warning
    assert len(result.data) == 2
    row = result.data[result.data["stock_id"] == "2330"].iloc[0]
    assert pd.notna(row["stock_return_5d"])
    assert pd.notna(row["stock_return_20d"])
    assert pd.notna(row["relative_strength_20d"])
    assert pd.notna(row["sector_strength_rank"])


def test_sector_strength_provider_chooses_mode_per_stock(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    reference_dir = data_dir / "reference"
    reference_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "industry": "半導體",
                "sub_industry": "晶圓代工",
                "market_type": "TSE",
                "source": "manual",
                "updated_at": "2026-05-15",
            }
        ]
    ).to_csv(reference_dir / "stock_industry_map.csv", index=False, encoding="utf-8-sig")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                f"  url: sqlite:///{tmp_path / 'tw_quant.sqlite'}",
                "industry_enrichment:",
                "  reference_map_path: data/reference/stock_industry_map.csv",
                "  industry_map_path: data/industry_map.csv",
            ]
        ),
        encoding="utf-8",
    )
    provider = LocalDerivedProvider(engine=_engine(tmp_path), config_path=config_path)

    result = provider.fetch_sector_strength("20260515")

    by_symbol = result.data.set_index("stock_id")
    assert result.status == "OK_WITH_FALLBACK"
    assert by_symbol.loc["2330", "industry"] == "半導體"
    assert by_symbol.loc["2330", "sector_strength_mode"] == "industry_relative"
    assert by_symbol.loc["2330", "sector_strength_warning"] == ""
    assert by_symbol.loc["9999", "sector_strength_mode"] == "market_relative_fallback"
    assert "缺少產業分類" in by_symbol.loc["9999", "sector_strength_warning"]


def test_multi_factor_uses_liquidity_and_sector_without_changing_trade_flags(tmp_path: Path) -> None:
    candidates = pd.DataFrame(
        [
            {"rank": 1, "stock_id": "2330", "stock_name": "台積電", "total_score": 80, "risk_pass": 1},
            {"rank": 2, "stock_id": "9999", "stock_name": "低量股", "total_score": 80, "risk_pass": 1},
        ]
    )
    pd.DataFrame(
        [
            {"trade_date": "2026-05-15", "stock_id": "2330", "stock_name": "台積電", "avg_volume_20d": 1_000_000, "avg_turnover_20d": 100_000_000, "latest_volume": 1_000_000, "latest_turnover": 100_000_000, "turnover_ratio_20d": 1.0, "liquidity_score": 92, "slippage_risk_score": 90, "liquidity_warning": ""},
            {"trade_date": "2026-05-15", "stock_id": "9999", "stock_name": "低量股", "avg_volume_20d": 50_000, "avg_turnover_20d": 2_000_000, "latest_volume": 50_000, "latest_turnover": 2_000_000, "turnover_ratio_20d": 1.0, "liquidity_score": 35, "slippage_risk_score": 30, "liquidity_warning": "流動性偏低，短線進出可能有滑價風險"},
        ]
    ).to_csv(tmp_path / "liquidity.csv", index=False)
    pd.DataFrame(
        [
            {"trade_date": "2026-05-15", "stock_id": "2330", "stock_name": "台積電", "industry": "全市場", "stock_return_5d": 0.05, "stock_return_20d": 0.2, "market_return_5d": 0.01, "market_return_20d": 0.05, "sector_return_5d": 0.01, "sector_return_20d": 0.05, "relative_strength_5d": 0.04, "relative_strength_20d": 0.15, "sector_strength_score": 86, "sector_strength_rank": 1, "sector_strength_warning": "缺少產業分類，使用全市場相對強弱"},
            {"trade_date": "2026-05-15", "stock_id": "9999", "stock_name": "低量股", "industry": "全市場", "stock_return_5d": -0.01, "stock_return_20d": -0.1, "market_return_5d": 0.01, "market_return_20d": 0.05, "sector_return_5d": 0.01, "sector_return_20d": 0.05, "relative_strength_5d": -0.02, "relative_strength_20d": -0.15, "sector_strength_score": 35, "sector_strength_rank": 2, "sector_strength_warning": "缺少產業分類，使用全市場相對強弱"},
        ]
    ).to_csv(tmp_path / "sector_strength.csv", index=False)

    result = apply_multi_factor_scores(
        candidates,
        data_dir=tmp_path,
        config={"multi_factor": {"enabled": True, "affect_ranking": False, "affect_risk_pass": False}},
    ).candidates

    assert result.loc[0, "multi_factor_score"] > result.loc[1, "multi_factor_score"]
    assert result["rank"].tolist() == [1, 2]
    assert result["risk_pass"].tolist() == [1, 1]
    assert "流動性偏低" in result.loc[result["stock_id"] == "9999", "risk_flags"].iloc[0]
    assert "相對強勢" in result.loc[result["stock_id"] == "2330", "positive_signals"].iloc[0]
    assert "相對強勢" not in result.loc[result["stock_id"] == "2330", "blocking_risks"].iloc[0]
    assert "缺少產業分類" in result.loc[result["stock_id"] == "2330", "data_quality_flags"].iloc[0]
    assert "流動性偏低" in result.loc[result["stock_id"] == "9999", "investment_risk_flags"].iloc[0]


def test_fetch_multi_factor_uses_local_derived_providers(tmp_path: Path, monkeypatch) -> None:
    class EmptyMOPSProvider:
        def __init__(self, cache_dir: Path) -> None:
            pass

        def fetch_monthly_revenue(self, trade_date: str) -> ProviderResult:
            return ProviderResult("monthly_revenue", pd.DataFrame(), "EMPTY")

        def fetch_financials(self, trade_date: str) -> ProviderResult:
            return ProviderResult("financials", pd.DataFrame(), "EMPTY")

        def fetch_material_events(self, trade_date: str) -> ProviderResult:
            return ProviderResult("material_events", pd.DataFrame(), "EMPTY")

    class EmptyTWSEProvider:
        def __init__(self, cache_dir: Path) -> None:
            pass

        def fetch_valuation(self, trade_date: str) -> ProviderResult:
            return ProviderResult("valuation", pd.DataFrame(), "EMPTY")

        def fetch_institutional(self, trade_date: str) -> ProviderResult:
            return ProviderResult("institutional", pd.DataFrame(), "EMPTY")

        def fetch_margin_short(self, trade_date: str) -> ProviderResult:
            return ProviderResult("margin_short", pd.DataFrame(), "EMPTY")

        def fetch_attention_disposition(self, trade_date: str) -> ProviderResult:
            return ProviderResult("attention_disposition", pd.DataFrame(), "EMPTY")

    class FakeLocalProvider:
        def __init__(self, config_path: Path) -> None:
            pass

        def fetch_liquidity(self, trade_date: str) -> ProviderResult:
            return ProviderResult(
                "liquidity",
                pd.DataFrame([{"trade_date": "2026-05-15", "stock_id": "2330", "stock_name": "台積電", "avg_volume_20d": 1, "avg_turnover_20d": 100_000_000, "latest_volume": 1, "latest_turnover": 100_000_000, "turnover_ratio_20d": 1, "liquidity_score": 92, "slippage_risk_score": 90, "liquidity_warning": ""}]),
                "OK",
            )

        def fetch_sector_strength(self, trade_date: str) -> ProviderResult:
            return ProviderResult(
                "sector_strength",
                pd.DataFrame([{"trade_date": "2026-05-15", "stock_id": "2330", "stock_name": "台積電", "industry": "全市場", "stock_return_5d": 0.01, "stock_return_20d": 0.05, "market_return_5d": 0, "market_return_20d": 0, "sector_return_5d": 0, "sector_return_20d": 0, "relative_strength_5d": 0.01, "relative_strength_20d": 0.05, "sector_strength_score": 70, "sector_strength_rank": 1, "sector_strength_warning": ""}]),
                "OK",
            )

    monkeypatch.setattr(fetch_module, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(fetch_module, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(fetch_module, "MOPSProvider", EmptyMOPSProvider)
    monkeypatch.setattr(fetch_module, "TWSEProvider", EmptyTWSEProvider)
    monkeypatch.setattr(fetch_module, "LocalDerivedProvider", FakeLocalProvider)

    status = fetch_module.run_fetch_multi_factor_data("20260515")

    assert (tmp_path / "data" / "liquidity.csv").exists()
    assert (tmp_path / "data" / "sector_strength.csv").exists()
    local_rows = status[status["source_name"].isin(["liquidity", "sector_strength"])]
    assert set(local_rows["provider_maturity"]) == {"local_derived"}
    assert set(local_rows["status"]) == {"OK"}
