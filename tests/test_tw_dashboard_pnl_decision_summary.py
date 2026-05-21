from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from scripts.generate_html_report import generate_html_report
from scripts.send_daily_notification import build_notification_message
from tw_quant.enrichment.report import generate_ai_enrichment
from tw_quant.enrichment.rule_based_enricher import RuleBasedEnricher
from tw_quant.reporting.dashboard_data import generate_market_recap, generate_pnl_chart_data
from tw_quant.workflow.daily import run_all_daily


def test_pnl_chart_data_and_html_chart_render(tmp_path: Path) -> None:
    _write_dashboard_reports(tmp_path)

    result = generate_pnl_chart_data(tmp_path, trade_date="2026-05-20")
    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert result.output_path is not None and result.output_path.exists()
    assert len(result.frame) >= 2
    assert "今日損益圖" in html
    assert "今日損益摘要" in html
    assert "近期資產 / 損益趨勢" in html
    assert "profit-positive" in html
    assert "profit-negative" in html
    assert "保證獲利" not in html


def test_pnl_chart_data_handles_missing_summary(tmp_path: Path) -> None:
    result = generate_pnl_chart_data(tmp_path, trade_date="2026-05-20")

    assert result.status == "EMPTY"
    assert result.output_path is not None and result.output_path.exists()
    assert result.frame.empty


def test_market_recap_generates_fallback_report(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text("database:\n  url: sqlite:///:memory:\n", encoding="utf-8")
    pd.DataFrame([{"trade_date": "2026-05-20", "market_regime_score": 45}]).to_csv(
        tmp_path / "market_regime_20260520.csv",
        index=False,
    )

    result = generate_market_recap(tmp_path, config_path=config, trade_date="2026-05-20")

    assert result.output_path is not None and result.output_path.exists()
    assert result.frame.iloc[0]["market_regime_score"] == 45
    assert "大盤" in result.frame.iloc[0]["recap_summary"]


def test_html_contains_decision_dashboard_market_recap_config_and_enrichment_context(tmp_path: Path) -> None:
    _write_dashboard_reports(tmp_path)
    generate_pnl_chart_data(tmp_path, trade_date="2026-05-20")
    generate_market_recap(tmp_path, config_path=_config(tmp_path), trade_date="2026-05-20")

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "決策儀表盤" in html
    assert "BUY_CANDIDATE" in html
    assert "WATCH_ONLY" in html
    assert "NO_TRADE" in html
    assert "風險警報" in html
    assert "利好催化" in html
    assert "大盤復盤" in html
    assert "配置說明" in html
    assert "auto_trading.enabled" in html and "false" in html
    assert "auto_trading.can_place_real_orders" in html
    assert "ai_enrichment.allow_external_ai" in html
    assert "AI / Enrichment 摘要" in html
    assert "估值脈絡" in html
    assert "融資籌碼" in html
    assert "sector_strength_mode" in html or "相對強弱模式" in html
    assert "資料來源依據" in html
    assert "<details" in html
    assert "保證獲利" not in html
    assert "一定買進" not in html
    assert "一定賣出" not in html


def test_rule_based_enrichment_expands_risk_context() -> None:
    frame = pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "industry_main": "半導體",
                "pe_ratio": 30,
                "pb_ratio": 5,
                "dividend_yield": 1.5,
                "margin_change_5d": 1000,
                "margin_change_20d": 5000,
                "price_return_5d": -0.01,
                "price_return_20d": -0.02,
                "institutional_net_buy_5d": -200,
                "sector_strength_score": 72,
                "relative_strength_5d": 0.03,
                "relative_strength_20d": 0.12,
                "sector_strength_mode": "market_relative_fallback",
                "risk_flags": "PE 偏高；融資連增但股價不漲；相對強勢；產業資料不足",
            },
            {
                "stock_id": "2303",
                "stock_name": "聯電",
                "industry_main": "半導體",
                "pe_ratio": 10,
            },
        ]
    )

    result = RuleBasedEnricher().enrich(frame, "2026-05-20")
    row = result[result["stock_id"] == "2330"].iloc[0]

    assert "PE=30.00" in row["valuation_context"]
    assert "中位數" in row["valuation_context"]
    assert "近 20 日融資增加" in row["margin_credit_context"]
    assert bool(row["margin_price_divergence"]) is True
    assert "全市場相對強弱" in row["sector_context"]
    assert "不能直接推論為同產業強勢" in row["sector_context"]


def test_ai_enrichment_writes_evidence_csv(tmp_path: Path) -> None:
    _write_dashboard_reports(tmp_path)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (tmp_path / "config.yaml").write_text("ai_enrichment:\n  provider: rule_based\n", encoding="utf-8")

    result = generate_ai_enrichment(
        reports_dir=tmp_path,
        data_dir=data_dir,
        config_path=tmp_path / "config.yaml",
        trade_date="2026-05-20",
    )

    assert result.output_path is not None and result.output_path.exists()
    assert result.evidence_path is not None and result.evidence_path.exists()
    evidence = pd.read_csv(result.evidence_path)
    assert "source_name" in evidence.columns


def test_discord_message_includes_pnl_decision_market_recap_and_enrichment(tmp_path: Path) -> None:
    _write_dashboard_reports(tmp_path)
    pd.DataFrame(
        [{"trade_date": "2026-05-20", "market_regime_score": 45, "regime_label": "大盤偏空", "market_breadth_summary": "市場廣度偏空"}]
    ).to_csv(tmp_path / "market_recap_20260520.csv", index=False)

    summary = pd.read_csv(tmp_path / "daily_summary_20260520.csv").iloc[0].to_dict()
    message = build_notification_message(summary, reports_dir=tmp_path, pages_url="https://example.github.io/tw-quant/")

    assert "今日損益摘要" in message
    assert "大盤復盤摘要" in message
    assert "風險警報" in message
    assert "利好催化" in message
    assert "僅供人工確認" in message
    assert "未自動下單" in message
    assert "資料不足時不做強結論" in message
    assert "保證獲利" not in message


def test_run_all_daily_adds_dashboard_status_fields(tmp_path: Path) -> None:
    result = run_all_daily(
        config_path=_config(tmp_path),
        trade_date="20260520",
        reports_dir=tmp_path / "reports",
        run_daily_func=_fake_run_daily,
        export_func=_fake_export,
        paper_func=_fake_paper,
        execute_func=_fake_execute,
        update_func=_fake_update,
        validation_func=lambda **kwargs: SimpleNamespace(validation=pd.DataFrame(), warning=""),
        decision_func=lambda **kwargs: SimpleNamespace(decisions=pd.DataFrame(), warning=""),
        loss_attribution_func=lambda **kwargs: SimpleNamespace(attribution=pd.DataFrame(), warning=""),
        industry_map_func=lambda **kwargs: (tmp_path / "data" / "industry_map.csv", "EMPTY", 0),
        enrichment_func=lambda **kwargs: SimpleNamespace(enrichment=pd.DataFrame(), warning=""),
        pnl_chart_func=lambda **kwargs: SimpleNamespace(status="OK", frame=pd.DataFrame([{"trade_date": "2026-05-20"}]), warning=""),
        market_recap_func=lambda **kwargs: SimpleNamespace(status="OK", frame=pd.DataFrame([{"trade_date": "2026-05-20"}]), warning=""),
    )

    row = pd.read_csv(result.summary_path).iloc[0]
    assert row["pnl_chart_status"] == "OK"
    assert row["market_recap_status"] == "OK"
    assert row["decision_dashboard_status"] in {"OK", "WARNING"}
    assert row["config_summary_status"] == "OK"
    assert "pnl_chart_data OK" in "\n".join(result.messages)


def _write_dashboard_reports(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "requested_date": "2026-05-20",
                "trade_date": "2026-05-20",
                "status": "OK",
                "candidate_rows": 2,
                "risk_pass_rows": 1,
                "pending_orders": 1,
                "executed_orders": 0,
                "open_positions": 1,
                "closed_positions": 1,
                "unrealized_pnl": 1200,
                "realized_pnl": -500,
                "realized_pnl_after_cost": -650,
                "realized_pnl_after_cost_today": 300,
                "total_equity": 1_001_200,
                "total_equity_after_cost": 1_000_900,
                "total_capital": 1_000_000,
                "total_cost": 120,
                "market_regime_score": 45,
                "guardrail_status": "BLOCKED",
                "new_entries_allowed": False,
                "pause_new_entries_reason": "market_regime_score 45 低於門檻",
                "grade_a_count": 1,
                "grade_b_count": 1,
                "grade_c_count": 0,
                "grade_d_count": 1,
                "buy_candidate_count": 1,
                "watch_only_count": 1,
                "no_trade_count": 1,
                "hold_count": 1,
                "reduce_count": 0,
                "exit_review_count": 0,
                "ai_enrichment_status": "OK",
                "ai_used_count": 0,
                "rule_based_enrichment_count": 2,
                "enrichment_insufficient_data_count": 1,
                "industry_map_status": "EMPTY",
            },
            {
                "requested_date": "2026-05-19",
                "trade_date": "2026-05-19",
                "status": "OK",
                "unrealized_pnl": -800,
                "realized_pnl_after_cost": -400,
                "total_equity": 998_000,
                "total_equity_after_cost": 997_600,
                "total_capital": 1_000_000,
            },
        ]
    ).to_csv(path / "daily_summary_20260520.csv", index=False)
    candidate = {
        "trade_date": "2026-05-20",
        "stock_id": "2330",
        "stock_name": "台積電",
        "total_score": 82,
        "multi_factor_score": 78,
        "final_market_score": 76,
        "confidence_score": 68,
        "risk_pass": 1,
        "risk_flags": "PE 偏高；融資連增但股價不漲；相對強勢；產業資料不足",
        "revenue_yoy": 12,
        "institutional_score": 70,
        "liquidity_score": 75,
        "sector_strength_score": 72,
        "relative_strength_5d": 0.03,
        "relative_strength_20d": 0.12,
        "sector_strength_mode": "market_relative_fallback",
        "pe_ratio": 30,
        "pb_ratio": 5,
        "dividend_yield": 1.5,
        "valuation_context": "PE=30.00，高於全市場中位數",
        "valuation_risk_level": "HIGH",
        "margin_credit_context": "近 5 日融資增加但股價沒有明顯上漲",
        "margin_risk_level": "MEDIUM",
        "margin_price_divergence": True,
        "ai_summary": "估值偏高且籌碼需檢查，僅供人工確認。",
        "manual_review_focus": "確認估值與融資籌碼",
        "risk_explanation": "PE 偏高；融資連增但股價不漲",
        "opportunity_explanation": "相對強勢但使用全市場 fallback",
        "data_quality_explanation": "產業資料不足",
        "source_evidence_count": 3,
        "source_evidence_json": "[]",
        "enrichment_status": "PARTIAL",
        "ai_used": False,
    }
    pd.DataFrame([candidate]).to_csv(path / "candidates_20260520.csv", index=False)
    pd.DataFrame([candidate]).to_csv(path / "risk_pass_candidates_20260520.csv", index=False)
    pd.DataFrame(
        [
            {
                **candidate,
                "decision": "BUY_CANDIDATE",
                "candidate_grade": "A",
                "decision_level": "WATCH",
                "action": "review_before_entry",
                "can_auto_trade": False,
                "requires_manual_review": True,
                "reason": "買進候選，需人工確認",
            },
            {
                "stock_id": "2603",
                "stock_name": "長榮",
                "decision": "NO_TRADE",
                "candidate_grade": "D",
                "risk_flags": "流動性偏低",
                "liquidity_score": 35,
            },
        ]
    ).to_csv(path / "trading_decisions_20260520.csv", index=False)
    pd.DataFrame([candidate]).to_csv(path / "ai_enrichment_20260520.csv", index=False)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-20",
                "stock_id": "2330",
                "stock_name": "台積電",
                "source_name": "reports",
                "source_type": "csv",
                "source_date": "2026-05-20",
                "field_name": "pe_ratio",
                "field_value": 30,
                "evidence_summary": "pe_ratio=30",
                "fallback_used": False,
                "confidence_impact": 0.8,
            }
        ]
    ).to_csv(path / "enrichment_evidence_20260520.csv", index=False)
    pd.DataFrame([{"stock_id": "2330", "stock_name": "台積電", "status": "PENDING"}]).to_csv(
        path / "pending_orders_20260520.csv",
        index=False,
    )
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-20",
                "stock_id": "2330",
                "stock_name": "台積電",
                "status": "OPEN",
                "entry_price": 100,
                "current_price": 101,
                "market_value": 101000,
                "unrealized_pnl": 1200,
                "unrealized_pnl_pct": 0.012,
                "shares": 1000,
                "remaining_shares": 1000,
                "stop_loss_price": 95,
                **candidate,
            },
            {
                "trade_date": "2026-05-18",
                "stock_id": "2603",
                "stock_name": "長榮",
                "status": "CLOSED",
                "entry_price": 100,
                "exit_date": "2026-05-20",
                "exit_price": 95,
                "realized_pnl_after_cost": -500,
                "exit_reason": "STOP_LOSS",
            },
        ]
    ).to_csv(path / "paper_trades.csv", index=False)


def _config(tmp_path: Path) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text("database:\n  url: sqlite:///:memory:\n", encoding="utf-8")
    return path


def _fake_run_daily(**_kwargs):
    return SimpleNamespace(trade_date=date(2026, 5, 20), fetched_rows=0, scored_rows=1, candidate_rows=1, message="")


def _fake_export(*_args, **_kwargs):
    return SimpleNamespace(
        trade_date=pd.Timestamp("2026-05-20"),
        candidates=pd.DataFrame({"stock_id": ["2330"]}),
        risk_pass_candidates=pd.DataFrame({"stock_id": ["2330"]}),
        warning="",
    )


def _fake_paper(*_args, **_kwargs):
    return SimpleNamespace(
        pending_orders=pd.DataFrame({"stock_id": ["2330"], "status": ["PENDING"]}),
        new_positions=pd.DataFrame(),
        positions=pd.DataFrame(),
        rejected_orders=pd.DataFrame(),
        warning="",
    )


def _fake_execute(*_args, **_kwargs):
    return SimpleNamespace(
        pending_orders=pd.DataFrame({"stock_id": ["2330"], "status": ["PENDING"]}),
        executed_orders=pd.DataFrame(),
        skipped_orders=pd.DataFrame(),
        warnings=[],
    )


def _fake_update(*_args, **_kwargs):
    return SimpleNamespace(
        summary=pd.DataFrame(
            [{"open_positions": 1, "closed_positions": 0, "unrealized_pnl": 100, "realized_pnl": 0, "total_equity": 1_000_100}]
        ),
        warning="",
    )
