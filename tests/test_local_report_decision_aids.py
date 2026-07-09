from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.generate_html_report import (
    _apply_holding_risk_lights,
    _enrich_with_local_factor_csv,
    _today_action_summary,
    generate_html_report,
)


def test_holding_risk_light_rules() -> None:
    frame = pd.DataFrame(
        [
            {
                "stock_id": "A",
                "current_price": 100,
                "stop_loss_price": 98,
                "liquidity_score": 80,
                "confidence_score": 90,
            },
            {
                "stock_id": "B",
                "current_price": 100,
                "stop_loss_price": 80,
                "liquidity_score": 80,
                "confidence_score": 50,
            },
            {
                "stock_id": "C",
                "current_price": 100,
                "stop_loss_price": 80,
                "is_attention_stock": True,
                "liquidity_score": 80,
                "confidence_score": 90,
            },
            {
                "stock_id": "D",
                "current_price": 100,
                "stop_loss_price": 80,
                "is_disposition_stock": True,
                "liquidity_score": 80,
                "confidence_score": 90,
            },
            {
                "stock_id": "E",
                "current_price": 100,
                "stop_loss_price": 80,
                "liquidity_score": 80,
                "confidence_score": 90,
            },
        ]
    )

    result = _apply_holding_risk_lights(frame, {"local_factors": {"holding_risk_light": {"near_stop_loss_pct": 0.03}}})
    lights = dict(zip(result["stock_id"], result["risk_light"]))

    assert lights["A"] == "紅燈"
    assert lights["B"] == "黃燈"
    assert lights["C"] == "黃燈"
    assert lights["D"] == "紅燈"
    assert lights["E"] == "綠燈"


def test_today_action_summary_is_limited_and_not_trade_advice() -> None:
    summary = {"trade_date": "2026-05-15", "take_profit_exits": 2, "stop_loss_exits": 1}
    pending = pd.DataFrame([{"status": "PENDING"} for _ in range(3)])
    open_positions = pd.DataFrame([{"risk_light": "紅燈"}, {"risk_light": "黃燈"}])
    status = pd.DataFrame(
        [
            {"source_name": "liquidity", "status": "OK"},
            {"source_name": "sector_strength", "status": "OK_WITH_FALLBACK"},
        ]
    )

    html = _today_action_summary(summary, pending, open_positions, status)

    assert "今日操作重點" not in html
    assert html.count("<li>") <= 5
    assert "等待進場" in html
    assert "紅燈" in html and "黃燈" in html
    assert "建議買進" not in html
    assert "建議賣出" not in html


def test_open_position_can_enrich_local_factors_when_not_candidate(tmp_path: Path, monkeypatch) -> None:
    import scripts.generate_html_report as report_module

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(report_module, "ROOT", tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "stock_id": "2330",
                "liquidity_score": 92,
                "avg_turnover_20d": 100_000_000,
                "slippage_risk_score": 90,
            }
        ]
    ).to_csv(data_dir / "liquidity.csv", index=False)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "stock_id": "2330",
                "sector_strength_score": 86,
                "relative_strength_5d": 0.03,
                "relative_strength_20d": 0.12,
            }
        ]
    ).to_csv(data_dir / "sector_strength.csv", index=False)
    frame = pd.DataFrame([{"stock_id": "2330", "stock_name": "台積電"}])

    result = _enrich_with_local_factor_csv(frame)

    assert result.loc[0, "liquidity_score"] == 92
    assert result.loc[0, "sector_strength_score"] == 86


def test_html_report_shows_local_factor_decision_aids(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "status": "OK",
                "requested_date": "2026-05-15",
                "trade_date": "2026-05-15",
                "candidate_rows": 1,
                "risk_pass_rows": 1,
                "pending_orders": 1,
                "open_positions": 1,
                "closed_positions": 0,
                "take_profit_exits": 0,
                "stop_loss_exits": 0,
                "market_regime_score": 42,
                "new_entries_allowed": False,
                "guardrail_status": "BLOCKED",
                "pause_new_entries_reason": "market_regime_score 42.00 低於新增持倉門檻 60.00",
                "rejected_orders": 1,
                "loss_attribution_loss_count": 1,
                "loss_attribution_top_reason": "市場環境偏弱",
                "total_equity_after_cost": 1_000_000,
            }
        ]
    ).to_csv(tmp_path / "daily_summary_20260515.csv", index=False)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "stock_id": "2330",
                "stock_name": "台積電",
                "status": "OPEN",
                "entry_price": 100,
                "current_price": 101,
                "market_value": 101_000,
                "unrealized_pnl": 1_000,
                "unrealized_pnl_pct": 0.01,
                "shares": 1000,
                "remaining_shares": 1000,
                "stop_loss_price": 90,
            }
        ]
    ).to_csv(tmp_path / "paper_trades.csv", index=False)
    pd.DataFrame([{"total_capital": 1_000_000, "total_equity_after_cost": 1_000_000}]).to_csv(
        tmp_path / "paper_summary_20260515.csv", index=False
    )
    candidate = {
        "rank": 1,
        "trade_date": "2026-05-15",
        "stock_id": "2330",
        "stock_name": "台積電",
        "total_score": 80,
        "multi_factor_score": 82,
        "final_market_score": 78,
        "confidence_score": 75,
        "liquidity_score": 92,
        "sector_strength_score": 86,
        "avg_turnover_20d": 100_000_000,
        "relative_strength_5d": 0.03,
        "relative_strength_20d": 0.12,
        "risk_pass": 1,
        "risk_flags": "相對強勢",
        "final_comment": "流動性與相對強弱偏佳",
    }
    pd.DataFrame([candidate]).to_csv(tmp_path / "candidates_20260515.csv", index=False)
    pd.DataFrame([candidate]).to_csv(tmp_path / "risk_pass_candidates_20260515.csv", index=False)
    pd.DataFrame([{"stock_id": "2330", "stock_name": "台積電", "status": "PENDING"}]).to_csv(
        tmp_path / "pending_orders_20260515.csv", index=False
    )
    pd.DataFrame(
        [
            {
                "stock_id": "3008",
                "stock_name": "大立光",
                "status": "REJECTED_GUARDRAIL",
                "candidate_grade": "B",
                "market_regime_score": 42,
                "rejected_reason": "候選分級 B 低於新增紙上交易門檻 A",
            }
        ]
    ).to_csv(tmp_path / "rejected_paper_orders_20260515.csv", index=False)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-15",
                "market_regime_score": 42,
                "source": "equal_weight_market",
            }
        ]
    ).to_csv(tmp_path / "market_regime_20260515.csv", index=False)
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "candidate_grade": "B",
                "decision": "HOLD",
                "unrealized_pnl_pct": -0.02,
                "market_regime_score": 42,
                "loss_bucket": "unrealized_loss",
                "likely_loss_reason": "市場環境偏弱",
            }
        ]
    ).to_csv(tmp_path / "loss_attribution_20260515.csv", index=False)
    pd.DataFrame(
        [
            {
                "source_name": "liquidity",
                "provider_maturity": "local_derived",
                "status": "OK",
                "rows": 1,
                "warning": "",
                "fallback_action": "wrote_new_data",
            },
            {
                "source_name": "sector_strength",
                "provider_maturity": "local_derived",
                "status": "OK_WITH_FALLBACK",
                "rows": 1,
                "warning": "缺少產業分類，使用全市場相對強弱",
                "fallback_action": "used_local_fallback",
            },
        ]
    ).to_csv(tmp_path / "data_fetch_status_20260515.csv", index=False)

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "今日操作重點" in html
    assert "持倉風險燈號" in html
    assert "市場環境分數" in html
    assert "是否允許新增持倉" in html
    assert "Guardrail 狀態" in html
    assert "暫停新倉原因" in html
    assert "Loss attribution 摘要" in html
    assert "被擋下交易明細" in html
    assert "流動性分數" in html
    assert "產業相對強弱分數" in html
    assert "本地價量衍生資料" in html
    assert "持倉綠燈數" in html
