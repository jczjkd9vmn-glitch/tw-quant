from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tw_quant.data_sources.base import ProviderResult
from tw_quant.data_sources.mops_provider import MATERIAL_EVENT_COLUMNS, MONTHLY_REVENUE_COLUMNS, MOPSProvider
from tw_quant.data_sources.twse_provider import (
    ATTENTION_DISPOSITION_COLUMNS,
    INSTITUTIONAL_COLUMNS,
    MARGIN_SHORT_COLUMNS,
    TWSEProvider,
)


DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"


@dataclass(frozen=True)
class SourceSpec:
    name: str
    output_path: Path
    columns: list[str]
    fetcher: Callable[[], ProviderResult | pd.DataFrame]


def _ensure_schema(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    output = frame.copy()
    for column in columns:
        if column not in output.columns:
            output[column] = pd.NA
    return output[columns]


def _load_existing(path: Path, columns: list[str]) -> tuple[pd.DataFrame, str]:
    if not path.exists():
        return pd.DataFrame(columns=columns), "MISSING"
    frame = pd.read_csv(path)
    if frame.empty:
        return _ensure_schema(frame, columns), "EMPTY"
    return _ensure_schema(frame, columns), "OK"


def _coerce_result(name: str, value: ProviderResult | pd.DataFrame, columns: list[str]) -> ProviderResult:
    if isinstance(value, ProviderResult):
        return ProviderResult(
            source_name=value.source_name or name,
            data=_ensure_schema(value.data, columns),
            status=value.status,
            warning=value.warning,
            error_message=value.error_message,
        )
    return ProviderResult(name, _ensure_schema(value, columns), "OK")


def run_fetch_multi_factor_data(as_of: str | None = None) -> pd.DataFrame:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    cache_dir = REPORTS_DIR / "cache"
    trade_date = as_of or date.today().strftime("%Y%m%d")
    twse = TWSEProvider(cache_dir=cache_dir)
    mops = MOPSProvider(cache_dir=cache_dir)

    specs = [
        SourceSpec(
            name="monthly_revenue",
            output_path=DATA_DIR / "monthly_revenue.csv",
            columns=MONTHLY_REVENUE_COLUMNS,
            fetcher=lambda: mops.fetch_monthly_revenue(trade_date),
        ),
        SourceSpec(
            name="valuation",
            output_path=DATA_DIR / "valuation.csv",
            columns=["stock_id", "stock_name", "date", "pe_ratio", "pb_ratio", "dividend_yield", "financial_quarter"],
            fetcher=lambda: ProviderResult("valuation", pd.DataFrame(), "EMPTY", "official valuation source not configured"),
        ),
        SourceSpec(
            name="financials",
            output_path=DATA_DIR / "financials.csv",
            columns=[
                "stock_id",
                "stock_name",
                "financial_quarter",
                "eps",
                "eps_yoy",
                "roe",
                "gross_margin",
                "operating_margin",
                "net_margin",
                "debt_ratio",
                "operating_cash_flow",
            ],
            fetcher=lambda: ProviderResult("financials", pd.DataFrame(), "EMPTY", "official financial source not configured"),
        ),
        SourceSpec(
            name="material_events",
            output_path=DATA_DIR / "material_events.csv",
            columns=MATERIAL_EVENT_COLUMNS,
            fetcher=lambda: mops.fetch_material_events(trade_date),
        ),
        SourceSpec(
            name="institutional",
            output_path=DATA_DIR / "institutional.csv",
            columns=[
                *INSTITUTIONAL_COLUMNS,
                "foreign_buy_days",
                "investment_trust_buy_days",
                "institutional_buy_ratio",
                "institutional_warning",
            ],
            fetcher=lambda: twse.fetch_institutional(trade_date),
        ),
        SourceSpec(
            name="margin_short",
            output_path=DATA_DIR / "margin_short.csv",
            columns=MARGIN_SHORT_COLUMNS,
            fetcher=lambda: twse.fetch_margin_short(trade_date),
        ),
        SourceSpec(
            name="attention_disposition",
            output_path=DATA_DIR / "attention_disposition.csv",
            columns=ATTENTION_DISPOSITION_COLUMNS,
            fetcher=lambda: twse.fetch_attention_disposition(trade_date),
        ),
        SourceSpec(
            name="sector_strength",
            output_path=DATA_DIR / "sector_strength.csv",
            columns=[
                "trade_date",
                "stock_id",
                "industry",
                "stock_return_5d",
                "stock_return_20d",
                "market_return_5d",
                "market_return_20d",
                "sector_return_5d",
                "sector_return_20d",
                "relative_strength_5d",
                "relative_strength_20d",
                "sector_strength_rank",
            ],
            fetcher=lambda: ProviderResult("sector_strength", pd.DataFrame(), "EMPTY", "sector strength is derived from local data in a later version"),
        ),
        SourceSpec(
            name="liquidity",
            output_path=DATA_DIR / "liquidity.csv",
            columns=["trade_date", "stock_id", "avg_volume_20d", "avg_turnover_20d", "intraday_trading_ratio"],
            fetcher=lambda: ProviderResult("liquidity", pd.DataFrame(), "EMPTY", "liquidity source not configured"),
        ),
    ]

    status_rows: list[dict[str, object]] = []
    for spec in specs:
        warning = ""
        error_message = ""
        try:
            fetched = _coerce_result(spec.name, spec.fetcher(), spec.columns)
        except Exception as exc:  # noqa: BLE001
            fetched = ProviderResult(
                spec.name,
                pd.DataFrame(columns=spec.columns),
                "FAILED",
                "fetch failed; fallback to existing csv",
                f"{type(exc).__name__}: {exc}",
            )
        output = fetched.data
        rows = len(output)
        status = fetched.status
        warning = fetched.warning
        error_message = fetched.error_message

        if output.empty and status != "CACHE":
            existing, existing_status = _load_existing(spec.output_path, spec.columns)
            output = existing
            rows = len(output)
            if existing_status == "OK":
                status = "OK"
                warning = (warning + "; " if warning else "") + "fallback to existing csv"
            elif existing_status == "EMPTY":
                status = "EMPTY"
                warning = (warning + "; " if warning else "") + "fallback to empty csv"
            else:
                status = "MISSING" if status != "FAILED" else "FAILED"
                warning = (warning + "; " if warning else "") + "no existing csv"

        output = _ensure_schema(output, spec.columns)
        output.to_csv(spec.output_path, index=False, encoding="utf-8-sig")
        status_rows.append(
            {
                "source_name": spec.name,
                "status": status,
                "rows": rows,
                "warning": warning,
                "error_message": error_message,
            }
        )

    result = pd.DataFrame(status_rows)
    date_tag = pd.to_datetime(trade_date).strftime("%Y%m%d")
    result.to_csv(REPORTS_DIR / f"data_fetch_status_{date_tag}.csv", index=False, encoding="utf-8-sig")
    return result


def main() -> None:
    status = run_fetch_multi_factor_data()
    print(status.to_string(index=False))


if __name__ == "__main__":
    main()
