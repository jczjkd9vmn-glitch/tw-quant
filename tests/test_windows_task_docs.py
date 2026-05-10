from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_run_daily_task_ps1_exists_and_contains_expected_commands() -> None:
    script = ROOT / "scripts" / "run_daily_task.ps1"

    assert script.exists()
    text = script.read_text(encoding="utf-8")
    assert "Set-Location $ProjectRoot" in text
    assert ".venv\\Scripts\\Activate.ps1" in text
    assert "scripts/backfill.py" in text
    assert '"--days", "10"' in text
    assert '"--timeout", "30"' in text
    assert '"--retries", "3"' in text
    assert '"--sleep", "1"' in text
    assert "scripts/run_all_daily.py" in text
    assert '"--capital", "1000000"' in text
    assert "logs" in text
    assert "daily_{0}.log" in text


def test_logs_gitignore_keeps_gitkeep() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")

    assert (ROOT / "logs" / ".gitkeep").exists()
    assert "logs/*" in gitignore
    assert "!logs/.gitkeep" in gitignore


def test_readme_contains_windows_task_scheduler_instructions() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "Windows 每日自動執行" in readme
    assert "工作排程器" in readme
    assert "20:30" in readme
    assert "powershell.exe" in readme
    assert "-ExecutionPolicy Bypass -File scripts/run_daily_task.ps1" in readme
    assert "logs/daily_YYYYMMDD.log" in readme
