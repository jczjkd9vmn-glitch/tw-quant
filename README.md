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

依序執行每日抓取與評分、候選股匯出、建立待進場委託、執行到期委託、更新紙上損益：

```powershell
python scripts/run_all_daily.py --date 20260508 --capital 1000000
```

若未指定 `--date`，`run_all_daily.py` 預設啟用 `--allow-fallback-latest`，會先使用 SQLite 內最新有效交易日，不會直接以今天抓取 TWSE。若指定日期沒有個股收盤資料且允許 fallback，console 會顯示 `fallback_date=YYYY-MM-DD reason=no trading data`；若 SQLite 完全沒有交易資料，流程才會失敗。

當 requested date 與實際 trade date 不同時，summary 會顯示 `status=OK_WITH_FALLBACK`，並寫出 requested date 的 `reports/daily_summary_YYYYMMDD.csv`。若該檔案先前是 FAILED，成功 fallback 後會被覆蓋為最新成功結果。

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

summary 包含 `trade_date`、`scored_rows`、`candidate_rows`、`risk_pass_rows`、`new_positions`、`open_positions`、`closed_positions`、`unrealized_pnl`、`realized_pnl`、`total_equity`、`total_cost`、`realized_pnl_after_cost`、`total_equity_after_cost`、出場策略觸發統計與基本面加分 / 警告候選股數。

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
reports/index.html
docs/index.html
```

`logs/` 仍只供本機 Windows 工作排程使用，不會提交到 GitHub。

### 繁體中文靜態 HTML 報表

可用以下指令從 `reports/` 內最新 CSV 產生手機可讀的深色 HTML 報表：

```powershell
python scripts/generate_html_report.py
```

輸出檔案：

```text
reports/index.html
docs/index.html
```

GitHub Pages 報表已改為手機優先的帳務 / 庫存介面，首頁標題為「台股紙上交易帳務」。最上方會先顯示損益總覽，讓手機打開後能直接看到總現值、總成本、總損益、報酬率、未實現損益、累計已實現損益、累計交易成本與扣成本後總資產。

損益顏色採台股習慣：紅色代表正損益，綠色代表負損益，灰白色代表 0 或無資料。報表包含：

- 損益總覽
- 持倉卡片
- 待進場卡片
- 已出場卡片
- 基本面摘要
- 系統健康檢查
- 交易成本摘要

手機版已改為更精簡的分頁式介面，預設先顯示損益總覽；持倉、待進場、已出場、基本面與健康檢查可由上方分頁切換。持倉與已出場交易優先用卡片呈現，未實現 / 已實現損益會放大顯示。今日候選股詳細表、通過風控股票詳細表、最近每日 summary、健康檢查明細與原始資料表格預設收合，需要檢查時再展開。

桌機版仍保留表格作為詳細檢查，但手機版以卡片與收合資訊為主，避免首頁過長。GitHub Actions 每日流程會在 `run_all_daily.py` 後自動產生此報表，並將 `reports/index.html` 與 `docs/index.html` 一併提交回 repo。

### 多面向資料與輔助評分

系統會在匯出候選股時補上多因子觀察欄位，產生 `multi_factor_score` 與 `multi_factor_reason`。第一版預設只作為輔助判斷，不改變原本 `total_score` 排序，也不改變既有 `risk_pass` 結果；也就是技術面、動能、風控與既有紙上交易流程仍是主流程。

目前支援的資料面向：

- 月營收資料：讀取 `data/monthly_revenue.csv`，輸出 `revenue_score`、`revenue_yoy`、`revenue_mom`、`accumulated_revenue_yoy` 與 `revenue_reason`。
- 估值資料：讀取 `data/valuation.csv`，觀察 PE、PB、殖利率，輸出 `valuation_score`、`valuation_reason` 與 `valuation_warning`。
- 財報資料：讀取 `data/financials.csv`，觀察 EPS、ROE、毛利率、營益率、負債比與營業現金流，輸出 `financial_score`、`financial_reason` 與 `financial_warning`。
- 重大訊息 / 新聞風險：第一版以官方重大訊息型資料 `data/material_events.csv` 為主，用關鍵字分類利多、利空與高風險事件，輸出 `event_score`、`event_risk_level`、`event_reason` 與 `event_blocked`。
- 籌碼資料：讀取 `data/institutional.csv`，觀察外資、投信、自營商買賣超與法人合計買賣超，輸出 `institutional_score` 與 `institutional_reason`。

缺少任一資料來源時，workflow 不會失敗，該面向採中性分數並在 reason 欄位記錄資料不足。每日匯出會另外產生：

```text
reports/data_fetch_status_YYYYMMDD.csv
```

用來追蹤各資料來源狀態、筆數、warning 與錯誤訊息。若 `multi_factor.block_on_high_risk_event` 為 `true`，高風險重大訊息會阻擋新進場；但在預設 `affect_ranking: false`、`affect_risk_pass: false` 下，不會改變原本候選股排序與 `risk_pass` 欄位。

### GitHub Pages 設定方式

1. 開啟 GitHub repo 的 `Settings`。
2. 點選左側 `Pages`。
3. `Source` 選 `Deploy from a branch`。
4. `Branch` 選 `main`。
5. `Folder` 選 `/docs`。
6. 儲存後等待 GitHub Pages 部署完成，即可用手機瀏覽 `docs/index.html` 對應的公開頁面。

### Discord 每日通知

GitHub Actions 每日流程會在產生 HTML 報表並提交資料後執行：

```powershell
python scripts/send_daily_notification.py
```

通知內容會以繁體中文顯示執行狀態、原始執行日期、實際交易日、是否使用替代交易日、候選股數、通過風控數、待進場筆數、今日成交筆數、跳過進場筆數、新增持倉數、目前持倉數、未實現損益、已實現損益、總資產、累計交易成本、扣成本後總資產、今日停利 / 停損 / 移動停利 / 趨勢出場筆數、今日扣成本後已實現損益、基本面加分 / 警告候選股數與 GitHub Pages 報表網址。

設定 Discord Webhook：

1. 在 Discord 頻道建立 Webhook，複製 Webhook URL。
2. 到 GitHub repo 的 `Settings`。
3. 點選 `Secrets and variables` → `Actions`。
4. 點選 `New repository secret`。
5. Name 填入 `DISCORD_WEBHOOK_URL`。
6. Secret 填入 Discord Webhook URL。
7. 儲存後，下次 GitHub Actions 執行完成會自動發送通知。

若未設定 `DISCORD_WEBHOOK_URL`，workflow 只會顯示 warning 並略過通知，不會因此失敗。

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

用最新的 `risk_pass_candidates_YYYYMMDD.csv` 建立待進場委託。收盤後產生訊號，不會用同一天收盤價直接建立 `OPEN` 持倉：

```powershell
python scripts/paper_trade.py
```

指定初始資金：

```powershell
python scripts/paper_trade.py --capital 1000000
```

輸出檔案：

```text
reports/pending_orders_YYYYMMDD.csv
```

待進場委託會記錄 `signal_date`、`planned_entry_date`、股票、停損價、建議部位與狀態 `PENDING`。若 SQLite 尚無下一個有效交易日資料，`planned_entry_date` 會先標記為 `NEXT_AVAILABLE_TRADING_DAY`。

執行到期待進場委託：

```powershell
python scripts/execute_pending_orders.py
```

或使用同一個 CLI：

```powershell
python scripts/paper_trade.py --mode execute
```

執行邏輯會從 SQLite 找出 `signal_date` 之後第一個有價格資料的有效交易日，優先使用該日開盤價作為 `entry_price`；若開盤價缺失或無效，才 fallback 使用收盤價，並記錄 `entry_price_source=CLOSE_FALLBACK` 與 warning。成交後寫入：

```text
reports/paper_trades.csv
```

若尚無下一個有效交易日資料，委託維持 `PENDING` 並等待下次執行。若同一檔股票已存在 `OPEN` 未平倉紀錄，委託會改為 `SKIPPED_EXISTING_POSITION`，不會重複買進。既有舊版 `OPEN` 持倉會保留，不會刪除或重建。此功能只建立紙上交易紀錄，不會真實下單。

### 交易成本與滑價

`config.yaml` 的 `trading_cost` 可設定紙上交易成本：

```yaml
trading_cost:
  commission_rate: 0.000399
  min_commission: 1
  sell_tax_rate_stock: 0.003
  sell_tax_rate_etf: 0.001
  sell_tax_rate_bond_etf: 0.0
  slippage_rate: 0.001
```

`commission_rate = 0.000399` 代表國泰台股電子下單手續費率 0.399‰，`min_commission = 1` 代表最低手續費 1 元。買進手續費使用滑價後成交金額計算，買進不收證券交易稅。賣出時一般股票交易稅使用 `sell_tax_rate_stock = 0.003`，ETF 使用 `sell_tax_rate_etf = 0.001`，債券 ETF 使用 `sell_tax_rate_bond_etf = 0.0`。

`slippage_rate = 0.001` 代表 0.1% 滑價假設。買進滑價會讓買進價變高：`entry_price_after_slippage = entry_price * (1 + slippage_rate)`；賣出滑價會讓賣出價變低：`exit_price_after_slippage = exit_price * (1 - slippage_rate)`。滑價不是券商實際收費，而是模擬真實成交可能偏離理想價格的保守估計。

交易成本會分開記錄手續費、證券交易稅與滑價。`paper_trades.csv` 會保留既有 `entry_slippage`、`entry_commission`、`exit_slippage`、`exit_commission`、`exit_tax`，並新增或維護 `entry_price_raw`、`exit_price_raw`、`slippage_rate`、`buy_slippage_cost`、`sell_slippage_cost`、`buy_commission`、`sell_commission`、`sell_tax`、`total_cost`、`realized_pnl_after_cost` 與 `realized_pnl_pct_after_cost`。`total_cost` 會包含滑價、買賣手續費與賣出交易稅。

### 出場策略

`config.yaml` 的 `exit_strategy` 控制紙上持倉更新時的出場規則：

```yaml
exit_strategy:
  take_profit_1_pct: 0.10
  take_profit_1_sell_pct: 0.50
  take_profit_2_pct: 0.20
  take_profit_2_sell_pct: 1.00
  trailing_stop_activate_pct: 0.08
  trailing_stop_drawdown_pct: 0.06
  ma_exit_window: 20
  max_holding_days: 20
  min_profit_for_holding: 0.03
```

每日更新會依序檢查停損、第一段停利、第二段停利、移動停利、跌破 20 日均線與持有過久出場。第一段停利只賣出部分部位，會更新 `remaining_shares` 並保留 `OPEN`；剩餘股數歸零時才改為 `CLOSED`。每次部分賣出都會計算賣出手續費、交易稅與滑價。

## 基本面與月營收觀察

可選擇提供月營收 CSV：

```text
data/monthly_revenue.csv
```

欄位：

```text
stock_id, stock_name, year_month, revenue, revenue_yoy, revenue_mom, accumulated_revenue, accumulated_revenue_yoy
```

候選股匯出會新增 `revenue_yoy`、`revenue_mom`、`accumulated_revenue_yoy` 與 `fundamental_reason`。第一版基本面資料只作為觀察欄位與報表輔助，不改變 `total_score`、候選股排序、`risk_pass` 或紙上交易進場邏輯。若缺少 `data/monthly_revenue.csv` 或個股沒有資料，會顯示「基本面資料不足，採中性分數」，流程不會失敗。

## 每日紙上持倉更新

用 SQLite 最新交易日價格更新紙上持倉：

```powershell
python scripts/update_paper_positions.py
```

指定更新日期與初始資金：

```powershell
python scripts/update_paper_positions.py --date 20260508 --capital 1000000
```

此流程會讀取 `reports/paper_trades.csv` 中 `OPEN` 的持倉，依指定日期或最新交易日收盤價更新目前市值、未實現損益、持有天數與是否觸及停損。若 `current_price <= stop_loss_price`，該筆交易會改為 `CLOSED`，並寫入 `exit_date`、`exit_price`、`realized_pnl`、`realized_pnl_pct`、`realized_pnl_after_cost`、`realized_pnl_pct_after_cost` 與 `exit_reason=STOP_LOSS`。

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

## 多因子資料自動抓取（第一版）

新增 `scripts/fetch_multi_factor_data.py`，每日會嘗試更新以下 5 個資料來源：

- `monthly_revenue`
- `valuation`
- `financials`
- `material_events`
- `institutional`

輸出檔案：

- `data/monthly_revenue.csv`
- `data/valuation.csv`
- `data/financials.csv`
- `data/material_events.csv`
- `data/institutional.csv`

每次執行會額外寫出：

- `reports/data_fetch_status_YYYYMMDD.csv`

狀態欄位：

- `source_name`
- `status`（`OK` / `EMPTY` / `MISSING` / `FAILED`）
- `rows`
- `warning`
- `error_message`

## Mobile Report、出場策略與 Market Intelligence

本專案新增手機優先的 GitHub Pages 報表、完整紙上交易出場策略，以及 market intelligence 輔助判斷模組。這些功能不保證獲利，且不會改變既有選股核心邏輯、pending order 隔日進場架構或真實下單限制。

### 手機版報表

`scripts/generate_html_report.py` 會產生：

```text
reports/index.html
docs/index.html
```

報表首頁加入「今日重點結論」與「系統健康檢查」，手機上優先顯示結論，再用卡片式呈現候選股、通過風控股票、待進場、目前持倉、已出場交易與 market intelligence 摘要。桌機版仍保留表格，詳細資料預設用 `<details><summary>` 收合。

損益顏色採台股習慣：紅色代表正損益，綠色代表負損益，灰白色代表 0 或無資料。資料缺失時會顯示 warning 或「今日無資料」，不會讓整份 HTML 報表失敗。

### 出場策略

`config.yaml` 的 `exit_strategy` 預設值：

```yaml
exit_strategy:
  take_profit_1_pct: 0.08
  take_profit_1_sell_pct: 0.50
  take_profit_2_pct: 0.15
  take_profit_2_sell_pct: 0.50
  trailing_stop_activate_pct: 0.08
  trailing_stop_drawdown_pct: 0.08
  ma_exit_window: 20
  max_holding_days: 30
  min_profit_for_holding: 0.03
```

每日更新紙上持倉時依序檢查：

1. 跌破停損價：賣出剩餘全部部位，`exit_reason=stop_loss`。
2. 報酬達 +8%：賣出 50%，`exit_reason=take_profit_1`，並記錄 `partial_exit_1_done`。
3. 報酬達 +15%：賣出剩餘部位 50%，`exit_reason=take_profit_2`，並記錄 `partial_exit_2_done`。
4. 從持有期間最高價回落 8%：賣出剩餘全部部位，`exit_reason=trailing_stop`。
5. 收盤跌破 20 日均線：賣出剩餘全部部位，`exit_reason=ma20_break`。資料不足 20 日時會略過，不會失敗。
6. 持有超過 30 個交易日且獲利不足 3%：賣出剩餘全部部位，`exit_reason=max_holding_days`。

出場同樣套用既有交易成本與滑價模型，會計算賣出手續費、證券交易稅、賣出滑價與扣成本後損益。舊版 `paper_trades.csv` 缺少新欄位時會自動補欄位並轉 dtype，保留 legacy CSV 相容性。

完整出場策略預設啟用；若未來需要停用，應透過明確 config（例如 `enable_exit_strategy: false`）控制，不要透過省略 `exit_strategy` 來隱性關閉。

### Market Intelligence

新增模組：

```text
src/tw_quant/market_intel/providers/base.py
src/tw_quant/market_intel/providers/mock_provider.py
src/tw_quant/market_intel/providers/yfinance_provider.py
src/tw_quant/market_intel/scoring.py
src/tw_quant/market_intel/report.py
```

第一版預設使用 `mock` provider 與既有候選股 / 多因子欄位補足資料，不需要 API key。`yfinance` provider 是可替換設計；若未安裝或外部來源失敗，只會回傳 warning 與中性分數，不會讓 pipeline crash。

Market intelligence 會輸出基本面分數、估值分數、動能分數、新聞情緒分數、綜合市場分數、信心分數、主要風險標籤、系統短評、資料來源與 warning。

輸出檔案：

```text
reports/market_intel_YYYYMMDD.csv
reports/cache/market_intel_YYYYMMDD.json
```

`reports/cache/market_intel_YYYYMMDD.json` 是快取，避免重跑時重複打外部 provider。若 cache 存在，系統會優先讀 cache；若不存在，會使用 provider 建立並寫入 cache。

### 交易限制

Market intelligence 預設只影響報表、Discord 摘要與候選股輔助欄位，不會直接產生買單，不會直接排除股票，也不會改變 pending order 進場日期邏輯。

預設設定：

```yaml
market_intel:
  enabled: true
  provider: mock
  cache_enabled: true
  affect_ranking: false
  affect_trading: false
  enable_market_intel_filter: false
```

若未來要讓 market intelligence 影響排序或進場，必須先透過 config 明確開啟，且仍需保留風控檢查與可追蹤理由。

### Discord 通知

Discord 每日通知新增今日市場判斷摘要、分數最高前 5 名候選股、新聞風險最高前 5 名、資料不足警告、今日系統健康狀態、目前 OPEN 持倉重點與今日 exit signal / 出場原因摘要。Webhook URL 仍從 GitHub Secrets 的 `DISCORD_WEBHOOK_URL` 讀取，不可寫死在程式碼。

設計原則：

- 優先抓公開來源。
- 若抓取失敗，會 fallback 到既有 CSV；若連既有 CSV 都沒有，會建立對應 schema 的空檔。
- 缺資料時不會讓 workflow 失敗，選股與交易流程維持可執行。
- 缺資料時多因子評分採中性分數，不會假造資料。
## 官方市場資料來源 provider

本專案新增官方市場資料來源 provider，主要放在 `src/tw_quant/data_sources/`，用於補強 market intelligence 與多因子輔助評分。第一版資料只影響報表、分數、排序參考、warning 與 Discord 通知；不會直接產生買單，也不會改變既有選股核心邏輯。`market_intel.affect_trading` 預設為 `false`。

目前資料來源與 fallback：

- `TWSEProvider`：三大法人買賣超、信用交易 / 融資融券資料、注意股 / 處置股資料的官方 TWSE provider。三大法人會優先抓 TWSE T86；信用與事件資料抓不到時會回傳 warning 並使用空資料。
- `MOPSProvider`：月營收與重大訊息 provider。月營收會嘗試讀官方公開表格，失敗時 fallback 到 `data/monthly_revenue.csv`；重大訊息第一版保留為非中斷式 warning。
- `TPEXProvider`：櫃買資料 provider 骨架，第一版回傳 warning 與空資料，方便後續補齊 OTC 官方端點。
- 本機 CSV fallback：`data/institutional.csv`、`data/margin_short.csv`、`data/attention_disposition.csv`、`data/monthly_revenue.csv`、`data/material_events.csv`、`data/sector_strength.csv`、`data/liquidity.csv`。

外部資料 cache 會寫到 `reports/cache/`，此目錄已加入 `.gitignore`，不應 commit。cache 損壞或資料來源失敗時，流程不會 crash，會記錄 warning 並以中性分數繼續。

新增輔助欄位包含：

- 籌碼：`chip_score`、`foreign_net_buy`、`investment_trust_net_buy`、`dealer_net_buy`、`total_institutional_net_buy`。
- 信用風險：`credit_score`、`margin_balance`、`margin_change`、`short_balance`、`securities_lending_sell_volume`。
- 事件風險：`event_risk_score`、`event_risk_level`、`event_blocked`、`risk_flags`。
- 月營收：`monthly_revenue`、`revenue_yoy`、`revenue_mom`、`accumulated_revenue_yoy`、`fundamental_score`。
- 產業相對強弱：`sector_strength_score`、`relative_strength_5d`、`relative_strength_20d`。
- 流動性：`liquidity_score`、`avg_turnover_20d`、`slippage_risk_score`。
- 綜合判斷：`final_market_score`、`confidence_score`、`data_source_warning`、`system_comment`。

`final_market_score` 第一版權重：

- 動能：25%
- 籌碼：20%
- 基本面 / 月營收：15%
- 估值：10%
- 產業相對強弱：10%
- 事件風險：10%
- 流動性：5%
- 新聞情緒：5%

`news_sentiment_score` 會先由 `-100` 到 `+100` 轉成 `0` 到 `100` 再加權。缺資料時使用中性分數 `50`，並降低 `confidence_score`。

事件風險設定：

```yaml
event_risk:
  block_disposition_stock: true
  block_attention_stock: false
```

處置股預設會阻擋新增 pending order，這屬於風控阻擋，不是 market intelligence 自動下單。注意股預設只顯示 warning 與 risk flag。

可手動更新多因子資料：

```powershell
python scripts/fetch_multi_factor_data.py
```

該腳本會輸出 `reports/data_fetch_status_YYYYMMDD.csv`，記錄每個來源的 `source_name`、`status`、`rows`、`warning` 與 `error_message`，方便追蹤資料缺口。

### 官方資料來源成熟度

目前官方資料 provider 是分階段接入，不代表所有來源都已完整正式上線：

- `TWSE institutional`：`best_effort`，目前可用，優先抓 TWSE T86 三大法人買賣超；失敗時保留既有 CSV 或使用中性分數。
- `TWSE margin_short`：`best_effort`，目前以官方資料表解析為主，但 TWSE 欄位格式可能變動；解析失敗時不覆寫既有有效 CSV。
- `TWSE attention_disposition`：目前以 `csv_fallback` 為主，官方注意股 / 處置股 endpoint 尚未完整接上。
- `MOPS monthly_revenue`：`best_effort`，會嘗試抓公開月營收表格；若環境缺少 `lxml` 或 HTML table 結構變動，會 fallback 到 `data/monthly_revenue.csv`。
- `material_events`：目前仍是 `placeholder` / local CSV fallback，尚未完整串接 MOPS 重大訊息正式 endpoint。
- `TPEXProvider`：目前是 `placeholder`，不是完整正式資料來源，保留給後續櫃買官方資料接入。
- `valuation`、`financials`、`sector_strength`、`liquidity`：第一版以 CSV fallback 或本地衍生資料為主，缺資料時採中性分數。

`reports/data_fetch_status_YYYYMMDD.csv` 會包含 `provider_maturity` 欄位：

- `production`：正式穩定來源。
- `best_effort`：可用但仍需容錯，來源格式可能變動。
- `placeholder`：保留架構，尚未完整接正式 endpoint。
- `csv_fallback`：以本機 CSV 或空 schema fallback 為主。

若 provider 回傳 `FAILED`、`MISSING` 或 `EMPTY`：

- 既有 CSV 有資料時，保留既有資料，不覆寫成空檔。
- 既有 CSV 不存在時，才建立只有 schema 的空 CSV。
- `warning` 會標示 `provider failed, kept existing csv`、`provider empty, kept existing csv` 或 `no existing csv, wrote empty schema`。

### final_market_score 與 multi_factor_score

- `final_market_score`：官方資料 / market intelligence 綜合分，用於 HTML 報表、Discord 摘要與觀察排序參考。
- `multi_factor_score`：原候選股多因子輔助分，目前仍不直接影響交易；只有在 config 明確開啟 `multi_factor.affect_ranking` 或 `multi_factor.affect_risk_pass` 時才會影響排序或風控通過結果。
- `market_intel.affect_trading` 預設為 `false`，market intelligence 不會直接產生買單。
## Official Provider Robustness Notes

本專案的官方資料來源仍採分階段接入，資料源失敗時不會中斷每日流程，也不會覆寫既有有效 CSV。

- MOPS 月營收：使用 `requests.Session()` 與瀏覽器風格 headers。若 MOPS 回傳 `THE PAGE CANNOT BE ACCESSED`、`FOR SECURITY REASONS` 或 `頁面無法執行`，會回傳 `FAILED`，warning 顯示 `MOPS security block detected; fallback to existing csv`，並保留既有 `data/monthly_revenue.csv`。
- MOPS HTML parser：月營收 HTML 解析使用 `pandas.read_html(StringIO(html))`，因此 `requirements.txt` 已加入 `lxml>=4.9`。若官方 HTML table 結構改變，會 fallback，不會 crash。
- TWSE margin_short：目前為 `best_effort`，會自動掃描 `fields/data`、`fields9/data9`、`fields1/data1` 與 `tables`，並容忍「代號 / 股票代號 / 證券代號」、「名稱 / 股票名稱 / 證券名稱」、「融資今日餘額 / 融資餘額 / 今日餘額」等欄位別名。少部分欄位缺失時會填空，不會讓整個 pipeline 失敗。
- TWSE attention_disposition：已改為 `best_effort`，使用 TWSE 公開的 `announcement/notice` 與 `announcement/punish` JSON 端點。若官方端點失敗，仍 fallback to existing CSV。處置股是否阻擋新增 pending order 仍由 `event_risk.block_disposition_stock` 控制。
- material_events：仍是 `placeholder / local CSV fallback`，尚未完整接 MOPS 重大訊息端點。
- TPEXProvider：仍是 `placeholder`，尚未完整接櫃買正式資料來源。

`reports/data_fetch_status_YYYYMMDD.csv` 欄位包含：

- `provider_maturity`：`best_effort`、`placeholder`、`csv_fallback` 或未來的 `production`。
- `fallback_action`：`wrote_new_data`、`cache_used`、`kept_existing_csv`、`wrote_empty_schema`。
- `error_message`：會截斷在合理長度內，避免把整段 HTML 安全頁寫進報表。
