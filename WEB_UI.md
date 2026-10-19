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
部署由 GitHub main 觸發 Streamlit Cloud；push 成功與雲端完成部署是兩個不同的驗證項目。測試結果與部署限制見下方登入修復紀錄。

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

啟用後，使用者以 Email＋密碼登入。「家庭帳號 / 登入」開啟獨立主頁表單，不再塞在側欄。返回研究會保留本次連線中的篩選、排序、頁碼與個股位置。重新整理或新分頁仍需重新登入，未實作跨連線登入持久化。

初始化使用同步 SDK 的 `SyncClientOptions`，不是缺少 `storage` 屬性的基底 `ClientOptions`。未設定、依賴缺失與初始化錯誤分開顯示；診斷僅記錄例外類別，不記錄原始例外、URL、金鑰、密碼、Email 或權杖。不要放 service-role key；瀏覽使用 anon/publishable key 及使用者 session，RLS 必須由管理員配置。

## 註冊與確認信

- 密碼須至少 8 字元並輸入確認密碼；Supabase 可另外要求更嚴格政策。
- 有效 user **與** session 同時存在才視為已登入。
- 沒有 session 時只顯示條件式受理訊息。Supabase 可能為既有帳號回傳模糊成功，因此不能宣稱「帳號已建立」或「信已送達」。
- 使用 implicit 流程的確認信完成服務端信箱驗證後，使用者回 App 以密碼登入；App 不讀 URL fragment，不從 query 接受 access/refresh token，不把任意 code 當 recovery。
- 舊 PKCE 連結不再交換；舊連結需重新申請。不使用會在新 Streamlit session 遺失的 verifier，也不把 verifier 放全域快取。
- 管理員需檢查 Supabase Site URL、Redirect URLs、Email confirmation 與寄件服務。預設寄件服務的收件人限制、速率限制、垃圾郵件都可能影響送達。Supabase 支援自訂信件模板；原文件聲稱 Free 不能自訂是不正確的。

## 密碼重設：手動 Email OTP

選用一次性 Email 驗證碼，讓使用者可在另一分頁、瀏覽器或裝置輸入，不依賴原始 session 的 PKCE verifier。

**管理員尚需完成的設定（本修復不更改雲端設定）：**

1. 在 Supabase Authentication → Email Templates → Reset Password 模板中放入 `{{ .Token }}`，指示使用者回 App 的「忘記密碼」輸入 Email 與驗證碼。不要保留會先消耗驗證碼的 `{{ .ConfirmationURL }}` 重設連結。
2. 確認驗證碼有效期、速率限制與郵件寄送配置；在 Streamlit Secrets 的 `[supabase]` 設定 `recovery_email_otp_enabled = true`。未啟用前 UI 明示未配置，不發送無法使用的重設信。
3. 若部署環境不同，在 `[supabase]` 設定 `redirect_url` 或使用 `SUPABASE_REDIRECT_URL`，並在 Supabase allowlist 加入相同 HTTPS App 網址。
4. 由使用者操作測試帳號，驗收新分頁收碼、`verify_otp` recovery、更新密碼、舊碼重用失敗、新密碼登入及舊密碼失敗。請勿在聊天或 GitHub 傳送驗證碼、密碼或金鑰。

App 呼叫 `verify_otp({email, token, type: 'recovery'})`，取得有效 session 後才開放更新密碼。重設 session 不提供家庭資料、不初始化家庭。更新成功後清除本機登入，即使遠端登出失敗也不顯示成密碼更新失敗。返回研究會丟棄 recovery client。

## 家庭初始化與權限

先查現有 `family_members`，共用成員沿用家庭；多個家庭時以 family_id 排序選第一個，尚無家庭切換 UI。只有沒有成員資格時才查自己建立的家庭，再補 owner membership；真正新使用者才建立家庭。一般成員若家庭沒有 shared list，不會自行升權或建立新家庭。

家庭初始化仍是多個 API 呼叫；沒有交易 RPC，無法保證多分頁同時初始化的原子性。唯一約束、RLS 與 transaction RPC 屬管理員的資料層工作，本次不改 SQL。RLS 錯誤會停止並顯示通用提示，不以新建另一家庭掩蓋失敗。即使清單初始化失敗，使用者仍可從帳號頁登出。

## 修復驗證範圍

`python -m unittest discover -s tests -v` 包含真實 SDK 的無網路 dummy client 建構、mock auth / AppTest、家庭成員重用與研究頁導航。測試不建立真實帳號、不發送郵件、不改雲端 Auth 設定或 SQL。實際郵件送達與使用者參與的端到端登入／重設驗收仍待上述配置完成。

2026-10-19：隔離 venv 安裝全部 requirements；Python 3.12、Streamlit 1.66.0、Supabase 2.33.0，23 項 unittest/AppTest 通過，`pip check` 與 compileall 通過。使用真實 SDK 重現舊 ClientOptions 的 `AttributeError: storage`，新同步 options 在禁止網路的測試通過。核心 UI/Auth 依賴已固定至本次驗證版本。

Chromium 本機桌面 1440×1000、手機 viewport 390×844 已檢查註冊頁、確認密碼、返回研究、無橫向溢出；使用 dummy 設定、未提交註冊。修正既有強制深色背景使淺色主題文字不可讀的問題。此為瀏覽器模擬尺寸，不代表手機實機或真實收信驗收。
