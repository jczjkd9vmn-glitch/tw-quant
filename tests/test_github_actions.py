from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_daily_github_actions_workflow_exists_and_contains_required_steps() -> None:
    workflow = ROOT / ".github" / "workflows" / "daily.yml"

    assert workflow.exists()
    text = workflow.read_text(encoding="utf-8")
    assert "workflow_dispatch:" in text
    assert "schedule:" in text
    assert 'cron: "30 12 * * 1-5"' in text
    assert 'python-version: "3.12"' in text
    assert "python -m pytest" in text
    assert "python scripts/backfill.py --days 10 --timeout 30 --retries 3 --sleep 1" in text
    assert "python scripts/run_all_daily.py --capital 1000000 --allow-fallback-latest" in text
    assert "python scripts/generate_html_report.py" in text
    assert "python scripts/send_daily_notification.py" in text
    assert "DISCORD_WEBHOOK_URL: ${{ secrets.DISCORD_WEBHOOK_URL }}" in text
    assert "git add data/ reports/ docs/" in text
    assert "git diff --cached --quiet" in text


def test_gitignore_keeps_persistent_data_and_reports() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")

    assert "data/*.sqlite" in gitignore
    assert "!data/tw_quant.sqlite" in gitignore
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
    assert "Deploy from a branch" in readme
    assert "/docs" in readme
    assert "DISCORD_WEBHOOK_URL" in readme
