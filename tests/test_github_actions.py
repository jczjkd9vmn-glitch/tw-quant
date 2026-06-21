from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_daily_github_actions_workflow_exists_and_contains_required_steps() -> None:
    workflow = ROOT / ".github" / "workflows" / "daily.yml"

    assert workflow.exists()
    text = workflow.read_text(encoding="utf-8")
    assert "workflow_dispatch:" in text
    assert "schedule:" in text
    assert 'cron: "30 12 * * *"' in text
    assert 'python-version: "3.12"' in text
    assert "python -m pytest" in text
    assert "python scripts/backfill.py --days 45 --timeout 30 --retries 3 --sleep 1" in text
    assert "python scripts/run_all_daily.py --capital 1000000 --allow-fallback-latest" in text
    assert "python scripts/generate_html_report.py" in text
    assert "reports/public_report_publish_status.csv" in text
    assert "public_report.docs_written" in text
    assert "public_report.trading_day_lag" in text
    assert "public_report.market_closed" in text
    assert "python scripts/send_daily_notification.py" in text
    assert "DISCORD_WEBHOOK_URL: ${{ secrets.DISCORD_WEBHOOK_URL }}" in text
    assert "git add data/ reports/ docs/" in text
    assert "git diff --cached --quiet" in text
    assert "pages: write" in text
    assert "id-token: write" in text
    assert "actions/configure-pages@v5" in text
    assert "actions/upload-pages-artifact@v4" in text
    assert "path: docs" in text
    assert "actions/deploy-pages@v4" in text
    assert "GitHub Pages deploy skipped because public docs were not updated." in text
    assert "github.ref == 'refs/heads/main'" in text


def test_daily_github_actions_smoke_tests_deployed_pages_report() -> None:
    workflow = ROOT / ".github" / "workflows" / "daily.yml"

    text = workflow.read_text(encoding="utf-8")
    deploy_step_index = text.index("uses: actions/deploy-pages@v4")
    smoke_step_index = text.index("name: Smoke test deployed GitHub Pages report")

    assert deploy_step_index < smoke_step_index
    assert "if: needs.daily.outputs.docs_written == 'true' && github.ref == 'refs/heads/main'" in text
    assert "report_requested_date: ${{ steps.public_report.outputs.requested_date }}" in text
    assert "report_trade_date: ${{ steps.public_report.outputs.trade_date }}" in text
    assert "report_actual_data_date: ${{ steps.public_report.outputs.actual_data_date }}" in text
    assert "report_trading_day_lag: ${{ steps.public_report.outputs.trading_day_lag }}" in text
    assert "report_freshness_source: ${{ steps.public_report.outputs.freshness_source }}" in text
    assert "public_report.trade_date" in text
    assert "public_report.freshness_source" in text
    assert "PAGES_URL: ${{ steps.deployment.outputs.page_url }}" in text
    assert "REPORT_REQUESTED_DATE: ${{ needs.daily.outputs.report_requested_date }}" in text
    assert "REPORT_TRADE_DATE: ${{ needs.daily.outputs.report_trade_date }}" in text
    assert "REPORT_ACTUAL_DATA_DATE: ${{ needs.daily.outputs.report_actual_data_date }}" in text
    assert "REPORT_TRADING_DAY_LAG: ${{ needs.daily.outputs.report_trading_day_lag }}" in text
    assert "REPORT_FRESHNESS_SOURCE: ${{ needs.daily.outputs.report_freshness_source }}" in text
    assert "max_attempts=8" in text
    assert "sleep_seconds=12" in text
    assert "curl -L --silent --show-error --connect-timeout 10 --max-time 30" in text
    assert "HTTP status=${http_status}" in text
    assert "Smoke test attempt ${attempt}/${max_attempts}" in text
    assert "Failure reason: ${failure_reason}" in text
    assert "Smoke test failed after ${max_attempts} attempts." in text
    assert "台股" in text
    assert "紙上交易" in text
    assert "實際交易日" in text
    assert "freshness_source" in text
    assert "資料來源" in text
    assert "trading_day_lag" in text
    assert "落後有效交易日" in text


def test_ci_github_actions_workflow_runs_quality_gates() -> None:
    workflow = ROOT / ".github" / "workflows" / "ci.yml"

    assert workflow.exists()
    text = workflow.read_text(encoding="utf-8")
    assert "python -m ruff check ." in text
    assert "python -m ruff format --check" in text
    assert "src/tw_quant/backtest/engine.py" in text
    assert "src/tw_quant/trading/pending.py" in text
    assert "python -m pytest -q" in text


def test_gitignore_excludes_runtime_sqlite_and_keeps_reports() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")

    assert "data/tw_quant.sqlite" in gitignore
    assert "data/*.sqlite" in gitignore
    assert "!data/tw_quant.sqlite" not in gitignore
    assert "data/*.sqlite.local_backup" in gitignore
    assert "data/*.db" in gitignore
    assert "data/*.db.local_backup" in gitignore
    assert "reports/*.csv" not in gitignore
    assert "logs/*" in gitignore
    assert "!logs/.gitkeep" in gitignore


def test_readme_documents_github_actions_setup() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "GitHub Actions 每日自動執行" in readme
    assert "private GitHub repo" in readme
    assert "workflow_dispatch" in readme
    assert "Daily Taiwan Stock Paper Trading" in readme
    assert "data/tw_quant.sqlite" in readme
    assert "reports/" in readme
    assert "繁體中文靜態 HTML 報表" in readme
    assert "GitHub Pages 設定方式" in readme
    assert "GitHub Actions" in readme
    assert "actions/deploy-pages" in readme
    assert "stale_docs_behavior: keep_previous" in readme
    assert "docs/index.html" in readme
    assert "DISCORD_WEBHOOK_URL" in readme
