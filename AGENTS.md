# tw-quant AI Agent 工作規範

所有回答請使用繁體中文，語氣直接、專業、簡潔。

## 工作範圍

- 只允許操作目前 workspace / repo 內的檔案。
- 不要讀取、修改或刪除 workspace / repo 以外的檔案。
- 即使 repo 位於 `Documents` 底下，只要是目前 workspace / repo 內的檔案，就可以操作。
- 不要刪除檔案，除非使用者明確要求。
- 修改程式、設定或文件時，請盡量做最小變更，不要無必要重構整個專案。
- 修改前先簡短說明預計修改的檔案、原因與風險；修改後回報變更、測試與仍需人工確認的事項。

## 公開資料、API 與 Skills

- 可以主動連網查詢公開資料、官方文件、API 文件、官方 GitHub、公開交易所資料、公開政府資料、可信開源專案、可用工具與 Codex Skills。
- 可以主動比較 API、SDK、MCP server、Skills、開源工具或資料來源，並整理候選清單、限制、風險與建議使用方式。
- 不要直接把網路查到的資料當成正式資料寫入專案。
- 會影響正式資料、交易邏輯、設定檔或投資判斷的外部資料，必須先產生候選資料或提案，等使用者人工確認後再正式採用。
- 不得直接接入新的第三方 API，除非使用者明確同意。
- 新 API 整合必須預設 disabled，並優先採 proposal-only / dry-run / disabled-by-default 模式。
- 新 API 必須支援 `timeout`、`retry` / retry limit、`max_requests_per_run`、cache、disabled mode，且不得把 API key 放入 repo。

## API Key 與機密資料

- 不要顯示、複製、記錄、修改或輸出任何密碼、token、API key、SSH key、憑證私鑰。
- 若任務明確需要呼叫 API，可以使用已設定在環境變數中的 API key，例如 `ANYSEARCH_API_KEY`。
- 只能判斷 API key 是否存在，不得顯示 API key 內容。
- 不得把 API key 寫入程式碼、config、report、cache、log、測試檔或 Git commit。
- 不得要求使用者把 API key 貼到對話中。
- 若發現任何 key、token、secret 被寫入檔案，必須立即停止並回報。

## 投資系統安全

- 不啟用真實下單。
- 不接券商 API，除非使用者明確要求。
- 不刪除 paper trading 資料，除非使用者明確要求。
- 不要把資料不足偽裝成正常。
- 不要把候選資料直接當正式資料。
- 不要讓 AI、AnySearch 或外部 API 直接決定買賣。
- 所有投資相關資料補強，必須保留資料來源與人工確認狀態。
- 任何會影響買賣決策、風控、評分、持倉或報表結論的改動，都必須能追蹤原因與資料來源。
- 若資料來源不足，請標示 `DATA_MISSING`、`NEEDS_REVIEW` 或 `NEEDS_MANUAL_CHECK`，不要硬湊成正常資料。

## AnySearch

- AnySearch 可以用來查資料、找來源、產生候選資料。
- AnySearch 僅可在 `ANYSEARCH_ENABLED=true` 且 `ANYSEARCH_API_KEY` 存在時呼叫。
- `ANYSEARCH_API_KEY` 只能從環境變數讀取，不得寫入任何檔案。
- AnySearch 每次最多查詢 config 指定的 `max_requests_per_run`，預設最多 30 筆。
- AnySearch 只能 proposal-only / candidate-only，結果只能輸出到候選報表，例如 `reports/anysearch_industry_candidates.csv`。
- AnySearch 不得直接修改 `data/reference/stock_industry_map.csv` 或任何正式 reference data。
- AnySearch 查到的結果必須保留 `source_url`、`source_title`、`confidence`、`reason`、`status`。
- `status` 預設只能是 `PENDING_REVIEW` 或 `NEEDS_MANUAL_CHECK`，除非使用者明確人工核准。
- 查詢結果要快取，避免重複消耗 API 額度。

## 正式資料採用

- 外部 API、AnySearch、網路搜尋與 AI 推論只能產生候選資料或提案。
- 正式 reference data 只能在使用者明確人工核准後才可寫入。
- 寫入正式資料前必須確認是否重複、來源是否可信、是否已人工確認、是否影響既有邏輯，以及是否需要更新測試或報表。
- 不確定就不要寫入正式資料。

## 外部工具與依賴

- 可以查詢並建議使用外部工具或 Skills，也可以建立比較報告或小型 PoC。
- 不要直接安裝來源不明工具。
- 不要執行來路不明腳本。
- 不要下載或執行二進位檔、`exe`、`bat`、`ps1`、`sh`，除非使用者明確同意。
- 若需要新增依賴，請先說明套件名稱、用途、必要性、風險與替代方案。
- 不要新增大型依賴、下載大型模型或未知資料庫，除非使用者明確同意。

## 測試與回報

完成任務後，盡可能執行：

```powershell
python -m pytest -q
python scripts/run_all_daily.py
python scripts/generate_html_report.py
```

若只修改文件，可以只執行：

```powershell
python -m pytest -q
```

回報需包含：

- 修改摘要
- 修改檔案
- 新增檔案
- 測試結果
- 風險與待確認事項
- 是否有啟用真實下單、接券商 API、刪除資料
- 是否有呼叫外部 API 或使用 API key
- 是否有修改正式 reference data
