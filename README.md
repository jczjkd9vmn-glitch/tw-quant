# 台股自動化量化分析系統

這是第一版 Python 台股量化分析專案骨架，重點是資料治理、規則化評分、回測、風控與模擬交易。系統不會保證獲利，也不包含任何真實下單功能。

## 功能

- 自動抓取每日收盤資料的資料層介面。
- 使用 SQLite 儲存價格、評分、模擬交易與回測資料。
- 依照趨勢、動能、基本面、籌碼、風險產生量化評分。
- 每個候選股票都輸出買進理由、停損價與建議部位。
- 回測引擎採用收盤後產生訊號、隔日開盤模擬進場，避免同日偷看資料。
- 風控模組會檢查資料異常、流動性、波動、停損與部位大小。
- Streamlit Dashboard 顯示候選清單、評分、理由、停損、部位與回測績效。
- 第一版只允許模擬交易，真實券商下單介面會直接拒絕。

## 專案結構

```text
.
├── .github/
│   └── workflows/
│       └── daily.yml
├── config.yaml
├── requirements.txt
├── pyproject.toml
├── reports/
├── scripts/
│   ├── backfill.py
│   ├── export_candidates.py
│   ├── paper_trade.py
│   ├── run_all_daily.py
│   ├── run_daily_task.ps1
│   ├── update_paper_positions.py
│   └── run_daily.py
├── src/
│   └── tw_quant/
│       ├── data/
│       ├── strategy/
│       ├── risk/
│       ├── backtest/
│       ├── trading/
│       └── app/
└── tests/
```

## 安裝

請在 Python 3.11+ 環境中安裝依賴：

```powershell
python -m pip install -r requirements.txt
```

## 初始化與每日流程

初始化資料庫並執行每日流程：

```powershell
python scripts/run_daily.py --date 2026-05-08
```

若只想用既有 SQLite 資料重新評分，不連線抓資料：

```powershell
python scripts/run_daily.py --date 2026-05-08 --no-fetch
```

若指定日期沒有 TWSE 個股收盤資料，但想改用 SQLite 最新交易日繼續評分，可加上：

```powershell
python scripts/run_daily.py --date 20260510 --allow-fallback-latest
```

## 一鍵每日流程

依序執行每日抓取與評分、候選股匯出、建立新紙上持倉、更新紙上損益：

```powershell
python scripts/run_all_daily.py --date 20260508 --capital 1000000
```

若未指定 `--date`，`run_all_daily.py` 預設啟用 `--allow-fallback-latest`，會先使用 SQLite 內最新有效交易日，不會直接以今天抓取 TWSE。若指定日期沒有個股收盤資料且允許 fallback，console 會顯示 `fallback_date=YYYY-MM-DD reason=no trading data`；若 SQLite 完全沒有交易資料，流程才會失敗。

可指定報告目錄，或略過紙上交易與持倉更新：

```powershell
python scripts/run_all_daily.py --date 20260508 --reports-dir reports
python scripts/run_all_daily.py --date 20260508 --skip-paper-trade
python scripts/run_all_daily.py --date 20260508 --skip-update
```

每個步驟會印出清楚結果；若某步驟失敗，CLI 會顯示簡潔錯誤訊息，不輸出完整 traceback。流程最後會輸出：

```text
reports/daily_summary_YYYYMMDD.csv
```

summary 包含 `trade_date`、`scored_rows`、`candidate_rows`、`risk_pass_rows`、`new_positions`、`open_positions`、`closed_positions`、`unrealized_pnl`、`realized_pnl` 與 `total_equity`。

## Windows 每日自動執行

專案提供 PowerShell 輔助腳本：

```powershell
scripts/run_daily_task.ps1
```

此腳本會：

- 切換到專案根目錄。
- 啟動 `.venv`。
- 執行 `python scripts/backfill.py --days 10 --timeout 30 --retries 3 --sleep 1`。
- 執行 `python scripts/run_all_daily.py --capital 1000000`。
- 將輸出寫入 `logs/daily_YYYYMMDD.log`。

Windows 工作排程器設定：

1. 開啟「工作排程器」。
2. 選擇「建立基本工作」。
3. 名稱可填 `TW Quant Daily`。
4. 觸發程序選「每天」。
5. 時間設定為 `20:30`。
6. 動作選「啟動程式」。
7. 程式或指令碼填：

```text
powershell.exe
```

8. 新增引數填：

```text
-ExecutionPolicy Bypass -File scripts/run_daily_task.ps1
```

9. 起始位置請填專案根目錄，例如：

```text
C:\Users\lin37\Documents\Codex\2026-05-08\python-1-2-sqlite-3-4
```

工作排程只會執行模擬交易與報告更新，不會真實下單。

## GitHub Actions 每日自動執行

專案提供 GitHub Actions workflow：

```text
.github/workflows/daily.yml
```

此 workflow 支援：

- `workflow_dispatch` 手動執行。
- 週一到週五台灣時間 `20:30` 自動執行。
- 使用 Python 3.12。
- 安裝 `requirements.txt`。
- 執行 `python -m pytest`。
- 執行 `python scripts/backfill.py --days 10 --timeout 30 --retries 3 --sleep 1`。
- 執行 `python scripts/run_all_daily.py --capital 1000000 --allow-fallback-latest`。
- 將 `data/` 與 `reports/` 的變更 commit 回 repo。
- 若沒有變更，workflow 會顯示 no changes，不會失敗。

非交易日或週末執行時，workflow 會使用 SQLite 最新交易日繼續產生報告，不會因當天沒有 TWSE 個股收盤資料而失敗；只有在 SQLite 完全沒有任何交易資料時才會中止。

### 建立 private GitHub repo

1. 登入 GitHub。
2. 點選右上角 `+`，選擇 `New repository`。
3. Repository name 可填 `tw-quant-paper-trading`。
4. Visibility 選 `Private`。
5. 不要勾選初始化 README、`.gitignore` 或 license，避免和本地專案衝突。
6. 建立 repository。

### Push 專案到 GitHub

在專案根目錄執行：

```powershell
git init
git add .
git commit -m "Initial Taiwan stock paper trading system"
git branch -M main
git remote add origin https://github.com/<your-user>/tw-quant-paper-trading.git
git push -u origin main
```

請把 `<your-user>` 換成你的 GitHub 帳號或組織名稱。

### 啟用 Actions

1. 開啟 GitHub repo。
2. 進入 `Actions` 分頁。
3. 若 GitHub 顯示需要啟用 workflows，選擇允許。
4. 確認左側出現 `Daily Taiwan Stock Paper Trading` workflow。

### 手動執行 workflow_dispatch

1. 開啟 repo 的 `Actions` 分頁。
2. 點選 `Daily Taiwan Stock Paper Trading`。
3. 點選 `Run workflow`。
4. 選擇 `main` branch。
5. 再按一次 `Run workflow`。

### 查看 reports

workflow 成功後會把最新資料 commit 回 repo。可在 GitHub 網頁中查看：

```text
data/tw_quant.sqlite
reports/
```

常用報告：

```text
reports/candidates_YYYYMMDD.csv
reports/risk_pass_candidates_YYYYMMDD.csv
reports/paper_trades.csv
reports/paper_summary_YYYYMMDD.csv
reports/daily_summary_YYYYMMDD.csv
```

`logs/` 仍只供本機 Windows 工作排程使用，不會提交到 GitHub。

## 歷史資料回補

依指定日期區間回補：

```powershell
python scripts/backfill.py --start 20250101 --end 20260508
```

從結束日往前回補 90 個日曆天：

```powershell
python scripts/backfill.py --end 20260508 --days 90
```

若省略 `--end`，`--days` 會以今天作為結束日：

```powershell
python scripts/backfill.py --days 90
```

調整 timeout、retry 與日期間隔：

```powershell
python scripts/backfill.py --days 120 --timeout 30 --retries 3 --sleep 0.5
```

若需要查看 TWSE payload table debug：

```powershell
python scripts/backfill.py --days 10 --verbose
```

backfill 會逐日抓取 TWSE 收盤資料並寫入 SQLite。遇到假日、無交易資料或 TWSE 空資料時，該日會顯示 `SKIP` 並繼續下一天；已存在於 SQLite 的日期也會跳過，避免重複寫入。遇到 timeout 或連線錯誤時會依 `--retries` 重試，仍失敗則該日顯示 `FAILED` 並繼續後續日期。回補結束後會自動重新計算最新交易日的 scoring；若歷史資料仍不足策略要求的 40 筆，會顯示 warning，不會丟出錯誤。

## Dashboard

```powershell
streamlit run src/tw_quant/app/dashboard.py
```

Dashboard 會讀取 `config.yaml` 指定的 SQLite 資料庫，顯示最新候選股票、每檔評分、買進理由、停損價、建議部位與回測績效。Dashboard 也會讀取 `reports/paper_trades.csv`、`reports/paper_summary_*.csv` 或 `reports/daily_summary_*.csv`，顯示紙上交易績效。

紙上交易績效區塊包含：

- `total_equity`、`unrealized_pnl`、`realized_pnl` 曲線。
- `open_positions`、`closed_positions`、`win_rate`、`total_return_pct`、`max_drawdown`。
- 目前 `OPEN` 持倉表格。
- `CLOSED` 交易紀錄。

若 reports 資料不足，Dashboard 會顯示 warning，不會中斷。

## 匯出候選股報告

輸出最新 scoring date 的候選股：

```powershell
python scripts/export_candidates.py
```

預設會產生：

```text
reports/candidates_YYYYMMDD.csv
reports/risk_pass_candidates_YYYYMMDD.csv
```

第一份檔案包含完整候選股清單，第二份只包含通過風控的候選股。匯出欄位包含排名、交易日、股票代號、股票名稱、收盤價、總分、五構面分數、候選狀態、風控狀態、風控理由、買進理由、停損價與建議部位。

## 模擬交易

用最新的 `risk_pass_candidates_YYYYMMDD.csv` 建立模擬持倉：

```powershell
python scripts/paper_trade.py
```

指定初始資金：

```powershell
python scripts/paper_trade.py --capital 1000000
```

模擬交易會依 `suggested_position_pct` 計算每檔投入金額，使用候選股收盤價作為模擬進場價，並輸出：

```text
reports/paper_positions_YYYYMMDD.csv
reports/paper_trades.csv
```

若同一檔股票已存在 `OPEN` 未平倉紀錄，系統會跳過該股票，不會重複買進。此功能只建立紙上交易紀錄，不會真實下單。

## 每日紙上持倉更新

用 SQLite 最新交易日價格更新紙上持倉：

```powershell
python scripts/update_paper_positions.py
```

指定更新日期與初始資金：

```powershell
python scripts/update_paper_positions.py --date 20260508 --capital 1000000
```

此流程會讀取 `reports/paper_trades.csv` 中 `OPEN` 的持倉，依指定日期或最新交易日收盤價更新目前市值、未實現損益、持有天數與是否觸及停損。若 `current_price <= stop_loss_price`，該筆交易會改為 `CLOSED`，並寫入 `exit_date`、`exit_price`、`realized_pnl`、`realized_pnl_pct` 與 `exit_reason=STOP_LOSS`。

輸出檔案：

```text
reports/paper_portfolio_YYYYMMDD.csv
reports/paper_summary_YYYYMMDD.csv
reports/paper_trades.csv
```

若指定日期沒有價格資料，系統會顯示 warning，不會修改交易紀錄。

## 測試

```powershell
python -m pytest
```

## 重要限制

- 本系統只做研究、回測與模擬交易，不保證任何投資績效。
- 第一版沒有任何真實下單能力。
- 資料缺漏、OHLC 異常、重複資料或風控不通過時，系統會拒絕產生可交易指令。
- 基本面與籌碼欄位若資料不足，評分會使用中性或保守規則，並在理由中揭露。
