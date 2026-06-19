from __future__ import annotations

from pathlib import Path
import warnings

import pandas as pd

from scripts.generate_html_report import generate_html_report
from scripts.send_daily_notification import build_notification_message
from tw_quant.config import load_config
from tw_quant.data.database import create_db_engine, init_db, save_daily_prices
from tw_quant.enrichment.industry import update_industry_map
from tw_quant.enrichment.report import _derive_context_columns, _merge_optional, generate_ai_enrichment
from tw_quant.enrichment.rule_based_enricher import RuleBasedEnricher
from tw_quant.trading.pending import execute_pending_orders


def test_execution_guardrail_blocked_pending_order_is_not_executed(tmp_path: Path) -> None:
    _write_pending(tmp_path, candidate_grade="A")
    pd.DataFrame([{"total_equity_after_cost": 900_000, "realized_pnl_after_cost_today": 0}]).to_csv(
        tmp_path / "paper_summary_20260515.csv",
        index=False,
    )
    engine = _engine_with_prices(tmp_path, ["2026-05-16"])

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000, config=_config())

    pending = pd.read_csv(tmp_path / "pending_orders_20260515.csv", dtype={"stock_id": str})
    rejected = pd.read_csv(tmp_path / "rejected_paper_orders_20260516.csv", dtype={"stock_id": str})
    assert result.executed_orders.empty
    assert pending.iloc[0]["status"] == "CANCELLED_BY_GUARDRAIL"
    assert rejected.iloc[0]["rejection_stage"] == "execution"
    assert "總回撤" in rejected.iloc[0]["rejection_reason"]
    assert not (tmp_path / "paper_trades.csv").exists()


def test_expired_pending_order_does_not_execute_and_writes_report(tmp_path: Path) -> None:
    _write_pending(tmp_path, signal_date="2026-05-15", candidate_grade="A")
    engine = _engine_with_prices(tmp_path, ["2026-05-16", "2026-05-17"])
    config = _config()
    config["pending_order"]["expire_after_trading_days"] = 0

    result = execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000, config=config)

    pending = pd.read_csv(tmp_path / "pending_orders_20260515.csv", dtype={"stock_id": str})
    rejected = pd.read_csv(tmp_path / "rejected_paper_orders_20260517.csv", dtype={"stock_id": str})
    assert result.executed_orders.empty
    assert pending.iloc[0]["status"] == "EXPIRED"
    assert pending.iloc[0]["attempted_execution_date"] == "2026-05-17"
    assert int(pending.iloc[0]["order_age_trading_days"]) == 2
    assert rejected.iloc[0]["final_order_status"] == "EXPIRED"


def test_low_grade_and_event_blocked_pending_orders_are_cancelled(tmp_path: Path) -> None:
    _write_pending(tmp_path, candidate_grade="B", event_blocked=True)
    engine = _engine_with_prices(tmp_path, ["2026-05-16"])

    execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000, config=_config())

    pending = pd.read_csv(tmp_path / "pending_orders_20260515.csv", dtype={"stock_id": str})
    assert pending.iloc[0]["status"] == "CANCELLED_BY_EVENT_RISK"
    rejected = pd.read_csv(tmp_path / "rejected_paper_orders_20260516.csv", dtype={"stock_id": str})
    assert "高風險事件" in rejected.iloc[0]["rejection_reason"]


def test_max_open_positions_blocks_execution_without_touching_closed_trades(tmp_path: Path) -> None:
    _write_pending(tmp_path, candidate_grade="A")
    trades = []
    for index in range(8):
        trades.append({"stock_id": f"10{index}", "stock_name": "測試", "status": "OPEN", "position_value": 10_000})
    trades.append({"stock_id": "9999", "stock_name": "舊平倉", "status": "CLOSED", "exit_reason": "STOP_LOSS"})
    pd.DataFrame(trades).to_csv(tmp_path / "paper_trades.csv", index=False, encoding="utf-8")
    engine = _engine_with_prices(tmp_path, ["2026-05-16"])

    execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000, config=_config())

    pending = pd.read_csv(tmp_path / "pending_orders_20260515.csv", dtype={"stock_id": str})
    trades_after = pd.read_csv(tmp_path / "paper_trades.csv", dtype={"stock_id": str})
    assert pending.iloc[0]["status"] == "CANCELLED_BY_MAX_POSITION"
    assert len(trades_after[trades_after["status"] == "CLOSED"]) == 1


def test_rejected_report_is_not_duplicated_on_repeated_execution(tmp_path: Path) -> None:
    _write_pending(tmp_path, candidate_grade="B")
    engine = _engine_with_prices(tmp_path, ["2026-05-16"])

    execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000, config=_config())
    execute_pending_orders(engine, reports_dir=tmp_path, capital=1_000_000, config=_config())

    rejected = pd.read_csv(tmp_path / "rejected_paper_orders_20260516.csv", dtype={"stock_id": str})
    assert len(rejected) == 1
    assert rejected.iloc[0]["final_order_status"] == "CANCELLED_BY_LOW_GRADE"


def test_rule_based_enrichment_generates_context_and_evidence(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "stock_id": "2330",
                "stock_name": "台積電",
                "candidate_grade": "B",
                "decision": "WATCH_ONLY",
                "pe_ratio": 40,
                "pb_ratio": 6,
                "dividend_yield": 1.2,
                "industry": "半導體",
                "margin_change_5d": 1000,
                "margin_change_20d": 5000,
                "price_return_5d": -0.01,
                "price_return_20d": -0.02,
                "risk_flags": "PE 偏高；融資連增但股價不漲",
            },
            {"trade_date": "2026-05-15", "stock_id": "2317", "stock_name": "鴻海", "pe_ratio": 20, "industry": "半導體"},
        ]
    )

    result = RuleBasedEnricher().enrich(frame, "2026-05-15")

    row = result[result["stock_id"] == "2330"].iloc[0]
    assert bool(row["ai_used"]) is False
    assert row["valuation_risk_level"] in {"MEDIUM", "HIGH"}
    assert row["margin_risk_level"] == "HIGH"
    assert "資料來源" not in row["ai_summary"]
    assert "保證獲利" not in row["ai_summary"]
    assert "此為紙上交易輔助說明，需人工確認" not in row["ai_summary"]
    assert int(row["source_evidence_count"]) > 0
    assert "source_name" in row["source_evidence_json"]


def test_generate_ai_enrichment_and_industry_map_use_rule_based_fallback(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    data = tmp_path / "data"
    reports.mkdir()
    data.mkdir()
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "stock_id": "2330",
                "stock_name": "台積電",
                "decision": "BUY_CANDIDATE",
                "candidate_grade": "A",
                "pe_ratio": 35,
                "risk_flags": "PE 偏高",
            }
        ]
    ).to_csv(reports / "trading_decisions_20260515.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [{"stock_id": "2330", "stock_name": "台積電", "industry": "半導體"}]
    ).to_csv(data / "valuation.csv", index=False, encoding="utf-8")

    path, status, rows = update_industry_map(data_dir=data, config_path=tmp_path / "missing.yaml")
    result = generate_ai_enrichment(reports_dir=reports, data_dir=data, config_path=tmp_path / "missing.yaml", trade_date="2026-05-15")

    assert status == "OK"
    assert rows == 1
    assert path.exists()
    assert result.output_path is not None and result.output_path.exists()
    assert result.cache_path is not None and result.cache_path.exists()
    assert len(result.enrichment) == 1
    for column in ["stock_id", "stock_name", "enrichment_provider", "source_evidence_json", "valuation_context"]:
        assert column in result.enrichment.columns
    assert result.enrichment.iloc[0]["enrichment_provider"] == "rule_based"
    assert bool(result.enrichment.iloc[0]["ai_used"]) is False


def test_enrichment_merge_preserves_stock_alignment_without_fragmentation_warning() -> None:
    base = pd.DataFrame(
        [
            {"stock_id": "2330", "stock_name": "台積電", "pe_ratio": ""},
            {"stock_id": "2317", "stock_name": "鴻海", "pe_ratio": 20},
        ]
    )
    extra_rows = [
        {"stock_id": "2317", "pe_ratio": 18, "industry_main": "電子代工", "stock_return_5d": 0.02},
        {"stock_id": "2330", "pe_ratio": 35, "industry_main": "半導體", "stock_return_5d": 0.05},
    ]
    for index in range(140):
        extra_rows[0][f"factor_{index}"] = f"foxconn_{index}"
        extra_rows[1][f"factor_{index}"] = f"tsmc_{index}"
    extra = pd.DataFrame(extra_rows)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", pd.errors.PerformanceWarning)
        merged = _merge_optional(base, extra)
        derived = _derive_context_columns(merged)

    performance_warnings = [warning for warning in caught if issubclass(warning.category, pd.errors.PerformanceWarning)]
    assert performance_warnings == []
    for column in ["factor_139", "industry_main", "price_return_5d", "sector_strength_mode"]:
        assert column in derived.columns

    tsmc = derived[derived["stock_id"] == "2330"].iloc[0]
    foxconn = derived[derived["stock_id"] == "2317"].iloc[0]
    assert tsmc["pe_ratio"] == 35
    assert tsmc["factor_139"] == "tsmc_139"
    assert tsmc["industry_main"] == "半導體"
    assert tsmc["price_return_5d"] == 0.05
    assert foxconn["pe_ratio"] == 20
    assert foxconn["factor_139"] == "foxconn_139"
    assert foxconn["industry_main"] == "電子代工"


def test_update_industry_map_uses_reference_map_when_no_local_industry(tmp_path: Path) -> None:
    data = tmp_path / "data"
    reference = data / "reference"
    reference.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "industry": "半導體業",
                "sub_industry": "半導體業",
                "market_type": "TWSE",
                "source": "manual",
                "updated_at": "2026-05-28",
            }
        ]
    ).to_csv(reference / "stock_industry_map.csv", index=False, encoding="utf-8")

    path, status, rows = update_industry_map(data_dir=data, config_path=tmp_path / "missing.yaml")

    result = pd.read_csv(path, dtype={"stock_id": str}, encoding="utf-8")
    assert status == "OK"
    assert rows == 1
    assert result.loc[0, "stock_id"] == "2330"
    assert result.loc[0, "industry_main"] == "半導體業"
    assert result.loc[0, "market"] == "TWSE"


def test_html_report_shows_pending_distribution_and_enrichment(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "pending_orders_active_count": 1,
                "pending_orders_expired_count": 1,
                "pending_orders_cancelled_count": 1,
                "rejected_orders_signal_count": 1,
                "rejected_orders_execution_count": 2,
                "rejected_orders_total_count": 3,
                "guardrail_status": "BLOCKED",
                "market_regime_score": 45,
                "ai_enrichment_status": "OK",
                "ai_used_count": 0,
                "rule_based_enrichment_count": 1,
                "enrichment_insufficient_data_count": 1,
                "industry_map_status": "OK",
            }
        ]
    ).to_csv(tmp_path / "daily_summary_20260515.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {"signal_date": "2026-05-15", "stock_id": "2330", "stock_name": "台積電", "status": "PENDING"},
            {"signal_date": "2026-05-14", "stock_id": "2317", "stock_name": "鴻海", "status": "EXPIRED"},
            {"signal_date": "2026-05-14", "stock_id": "2603", "stock_name": "長榮", "status": "CANCELLED_BY_GUARDRAIL"},
        ]
    ).to_csv(tmp_path / "pending_orders_20260515.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "stock_id": "2603",
                "stock_name": "長榮",
                "rejection_stage": "execution",
                "final_order_status": "CANCELLED_BY_GUARDRAIL",
                "rejection_reason": "guardrail BLOCKED",
            }
        ]
    ).to_csv(tmp_path / "rejected_paper_orders_20260515.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "stock_id": "2330",
                "stock_name": "台積電",
                "ai_summary": "資料不足時僅供人工確認",
                "manual_review_focus": "確認估值",
                "enrichment_status": "PARTIAL",
                "enrichment_provider": "rule_based",
                "ai_used": False,
                "source_evidence_count": 2,
                "source_evidence_json": '[{"source_name":"reports"}]',
            }
        ]
    ).to_csv(tmp_path / "ai_enrichment_20260515.csv", index=False, encoding="utf-8")

    output = generate_html_report(reports_dir=tmp_path, docs_dir=tmp_path / "docs")
    html = output.read_text(encoding="utf-8")

    assert "Active pending" in html
    assert "Expired pending" in html
    assert "Cancelled pending" in html
    assert "AI / Enrichment 摘要" in html
    assert "資料來源依據" in html
    assert "pending order 有效期限" in html.lower()
    assert "保證獲利" not in html


def test_discord_message_includes_pending_and_enrichment_summary(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "decision": "BUY_CANDIDATE",
                "candidate_grade": "A",
            }
        ]
    ).to_csv(tmp_path / "trading_decisions_20260515.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "ai_summary": "僅供人工確認，未自動下單",
                "risk_explanation": "資料不足時不做強結論",
            }
        ]
    ).to_csv(tmp_path / "ai_enrichment_20260515.csv", index=False, encoding="utf-8")
    summary = {
        "trade_date": "2026-05-15",
        "requested_date": "2026-05-15",
        "status": "OK",
        "pending_orders_active_count": 1,
        "pending_orders_executed_count": 0,
        "pending_orders_expired_count": 1,
        "pending_orders_cancelled_count": 2,
        "rejected_orders_signal_count": 1,
        "rejected_orders_execution_count": 2,
        "market_regime_score": 45,
        "guardrail_status": "BLOCKED",
        "ai_enrichment_status": "OK",
        "ai_used_count": 0,
        "rule_based_enrichment_count": 1,
        "enrichment_insufficient_data_count": 1,
    }

    message = build_notification_message(summary, reports_dir=tmp_path, pages_url="https://example.test")

    assert "Expired pending orders：1" in message
    assert "Cancelled pending orders：2" in message
    assert "Execution rejected：2" in message
    assert "AI / Enrichment 狀態：OK" in message
    assert "前 5 名 BUY_CANDIDATE AI 摘要" in message
    assert "僅供人工確認" in message
    assert "保證獲利" not in message


def test_ai_enrichment_config_is_advisory_and_external_ai_disabled_by_default() -> None:
    config = load_config(Path("missing-test-config.yaml"))

    assert config["ai_enrichment"]["enabled"] is True
    assert config["ai_enrichment"]["provider"] == "rule_based"
    assert config["ai_enrichment"]["allow_external_ai"] is False
    assert config["ai_enrichment"]["advisory_only"] is True
    assert config["pending_order"]["expire_after_trading_days"] == 1


def _write_pending(
    path: Path,
    *,
    signal_date: str = "2026-05-15",
    candidate_grade: str = "A",
    event_blocked: bool = False,
) -> None:
    pd.DataFrame(
        [
            {
                "signal_date": signal_date,
                "planned_entry_date": "NEXT_AVAILABLE_TRADING_DAY",
                "stock_id": "2330",
                "stock_name": "台積電",
                "signal_close": 100,
                "total_score": 90,
                "stop_loss_price": 92,
                "suggested_position_pct": 0.1,
                "status": "PENDING",
                "candidate_grade": candidate_grade,
                "decision": "BUY_CANDIDATE",
                "event_blocked": event_blocked,
            }
        ]
    ).to_csv(path / "pending_orders_20260515.csv", index=False, encoding="utf-8")


def _engine_with_prices(tmp_path: Path, dates: list[str]):
    engine = create_db_engine(f"sqlite:///{(tmp_path / 'prices.sqlite').as_posix()}")
    init_db(engine)
    for offset, trade_date in enumerate(dates, start=1):
        save_daily_prices(
            engine,
            pd.DataFrame(
                [
                    {
                        "trade_date": trade_date,
                        "symbol": "2330",
                        "name": "台積電",
                        "open": 100 + offset,
                        "high": 105 + offset,
                        "low": 95 + offset,
                        "close": 102 + offset,
                        "volume": 2_000_000,
                        "turnover": (102 + offset) * 2_000_000,
                        "market": "TSE",
                        "source": "TEST",
                    }
                ]
            ),
        )
    return engine


def _config() -> dict:
    return {
        "paper_trading_guardrails": {
            "enabled": True,
            "min_grade_for_new_entry": "A",
            "max_total_drawdown_pct": 0.05,
            "max_daily_loss_pct": 0.02,
            "max_consecutive_stop_loss": 3,
            "pause_new_entries_days": 5,
            "max_open_positions": 8,
            "max_daily_new_positions": 2,
        },
        "pending_order": {
            "expire_after_trading_days": 1,
            "cancel_if_guardrail_blocked": True,
            "cancel_if_market_regime_below_threshold": True,
            "keep_rejected_history": True,
        },
        "market_regime": {
            "enabled": False,
            "min_score_for_new_entries": 60,
            "fallback_to_equal_weight_market": True,
        },
    }
