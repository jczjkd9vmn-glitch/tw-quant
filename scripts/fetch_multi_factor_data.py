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


DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"


@dataclass(frozen=True)
class SourceSpec:
    name: str
    output_path: Path
    columns: list[str]
    fetcher: Callable[[], pd.DataFrame]


def _fetch_monthly_revenue_public() -> pd.DataFrame:
    # 先保留公開來源介接點；第一版若抓取失敗，回退既有 CSV 或空 schema
    raise RuntimeError("monthly revenue public source not configured")


def _fetch_unavailable(_name: str) -> pd.DataFrame:
    raise RuntimeError("public source not configured")


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


def run_fetch_multi_factor_data() -> pd.DataFrame:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    specs = [
        SourceSpec(
            name="monthly_revenue",
            output_path=DATA_DIR / "monthly_revenue.csv",
            columns=[
                "stock_id",
                "stock_name",
                "year_month",
                "revenue",
                "revenue_yoy",
                "revenue_mom",
                "accumulated_revenue",
                "accumulated_revenue_yoy",
            ],
            fetcher=_fetch_monthly_revenue_public,
        ),
        SourceSpec(
            name="valuation",
            output_path=DATA_DIR / "valuation.csv",
            columns=["stock_id", "stock_name", "date", "pe_ratio", "pb_ratio", "dividend_yield", "financial_quarter"],
            fetcher=lambda: _fetch_unavailable("valuation"),
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
            fetcher=lambda: _fetch_unavailable("financials"),
        ),
        SourceSpec(
            name="material_events",
            output_path=DATA_DIR / "material_events.csv",
            columns=[
                "event_date",
                "stock_id",
                "stock_name",
                "title",
                "summary",
                "event_type",
                "event_sentiment",
                "event_risk_level",
            ],
            fetcher=lambda: _fetch_unavailable("material_events"),
        ),
        SourceSpec(
            name="institutional",
            output_path=DATA_DIR / "institutional.csv",
            columns=[
                "date",
                "stock_id",
                "stock_name",
                "foreign_net_buy",
                "investment_trust_net_buy",
                "dealer_net_buy",
                "institutional_total_net_buy",
                "institutional_3d_sum",
                "institutional_5d_sum",
            ],
            fetcher=lambda: _fetch_unavailable("institutional"),
        ),
    ]

    status_rows: list[dict[str, object]] = []
    for spec in specs:
        warning = ""
        error_message = ""
        status = "OK"
        rows = 0

        try:
            fetched = _ensure_schema(spec.fetcher(), spec.columns)
            if fetched.empty:
                status = "EMPTY"
                warning = "public source returned empty data"
            output = fetched
        except Exception as exc:  # noqa: BLE001
            error_message = str(exc)
            existing, existing_status = _load_existing(spec.output_path, spec.columns)
            output = existing
            rows = len(output)
            if existing_status == "OK":
                status = "OK"
                warning = "fetch failed, fallback to existing csv"
            elif existing_status == "EMPTY":
                status = "EMPTY"
                warning = "fetch failed, fallback to empty csv"
            else:
                status = "MISSING"
                warning = "fetch failed and no existing csv"
        else:
            rows = len(output)

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
    date_tag = date.today().strftime("%Y%m%d")
    result.to_csv(REPORTS_DIR / f"data_fetch_status_{date_tag}.csv", index=False, encoding="utf-8-sig")
    return result


def main() -> None:
    status = run_fetch_multi_factor_data()
    print(status.to_string(index=False))


if __name__ == "__main__":
    main()
