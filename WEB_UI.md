# Web UI 第一版

## 範圍

新版入口仍是 `app.py`，畫面實作在 `web_ui.py`。本次不修改晨報、午報、爆量報告、更新腳本、排程、行情資料或 OpenClaw 設定。

- 全域代號／名稱搜尋、最近查看（僅本次連線）。
- 個股走勢範圍、分組基本面、資料日期及條件風險提示。
- 表單式精簡選股、明示已套用條件、清除篩選、結果排序；營收、EPS、殖利率、位階、量能五項條件可組合。
- 每檔可查看符合條件及實際值；缺值不會通過數值門檻，數值 0 是有效門檻而不是不限。
- 精簡卡片與表格兩種結果視圖。
- 快取以資料庫檔案版本與 WAL 版本區分，TTL 60 秒；篩選條件、排序、卡片模式與頁碼在個股返回後保留於本次連線。
- 個股可用 `?stock=2330` 直接開啟；搜尋後可返回探索頁，並保留本次研究狀態。
- 近一年位階可設定上下限；清除篩選會重置所有範本、門檻、排序與卡片狀態。
- 缺值顯示未取得；財務原值不在 UI 截斷。來源已存的零值仍保留。
- 側欄「進階工具（原版完整介面）」保留原篩選、相似股票與 AI 顧問。舊介面仍有既有呈現限制，不代表本次已全面重寫。

## 本機檢查

在專案目錄執行 `venv/bin/python -m unittest discover -s tests -v`。
互動測試需要現有資料庫，讀取資料但不生成 AI 或發送報告。

手動啟動：`venv/bin/python -m streamlit run app.py --server.address 127.0.0.1`。
未執行雲端部署。不同部署的套件版本需與本機驗證版本核對；本機驗證 Streamlit 1.55.0。

## 下一階段

永久觀察清單、筆記、比較與 AI 解讀整合尚未實作。需要獨立使用者儲存設計，不能把資料寫回會被行情更新替換的 DB。
手機實機視覺、觸控與實際部署仍待驗收；AppTest 不等同瀏覽器視覺測試。

## 回復方式

切換側欄進階工具即可使用原介面。若要恢復原入口，僅將 app.py 最後的 render_app 呼叫改回 main()；不要 git checkout 整份 app.py，因為它在本次之前已有未提交修改。
## 雲端家庭觀察清單

Web UI 可選擇接上 Supabase。未設定 Secrets 時，研究功能仍可正常使用；設定後才會顯示登入與家庭共用觀察清單。

Streamlit Cloud Secrets 使用以下格式，值請從 Supabase API 設定頁取得，不要提交到 GitHub：

[supabase]
url = "你的 Supabase Project URL"
anon_key = "你的 Supabase anon public key"

啟用後，使用者以 Email＋密碼登入。第一次登入會自動建立家庭群組、成員資料與家庭共用觀察清單。行情資料庫仍為唯讀，觀察清單資料獨立存放於 Supabase。

Supabase Free 方案的預設信件是 Magic Link，且不能自訂信件模板，因此本 App 不要求六位數 OTP。第一次使用可按「建立帳號」，完成 Email 確認後，再用 Email＋密碼登入。
# Supabase 密碼重設

側欄的「寄送重設密碼信」會使用 Streamlit App 網址作為回址。開啟信件後，App 會顯示新密碼與確認密碼欄位；兩次輸入一致且至少 6 個字元後才會更新 Supabase 密碼。

若部署環境不同，可在 Streamlit Secrets 設定 `SUPABASE_REDIRECT_URL`，或在 `[supabase]` 區塊設定 `redirect_url`。不要把任何密碼或 secret key 寫入 GitHub。
