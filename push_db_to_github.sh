#!/bin/bash
# push_db_to_github.sh - commit and push only the compressed database used by Streamlit Cloud

set -u

export PATH="/usr/bin:/bin:/home/sunny/.npm-global/bin:/home/sunny/.local/bin:$PATH"
export GIT_TERMINAL_PROMPT=0

PROJECT_DIR="/home/sunny/family-stock-app"
LOG_FILE="$PROJECT_DIR/cron_local.log"
DB_XZ_FILE="$PROJECT_DIR/stock_data.db.xz"

cd "$PROJECT_DIR" || exit 1

echo "----------------------------------------" >> "$LOG_FILE"
echo "☁️ 資料庫推送開始 | $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"

if [ ! -f "$DB_XZ_FILE" ]; then
    echo "❌ 找不到 stock_data.db.xz，取消推送" >> "$LOG_FILE"
    exit 1
fi

git fetch origin main >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ git fetch 失敗，取消推送" >> "$LOG_FILE"
    exit 1
fi

git pull --rebase --autostash origin main >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ git pull --rebase 失敗，取消推送，避免覆蓋遠端" >> "$LOG_FILE"
    exit 1
fi

if git diff --quiet -- stock_data.db.xz; then
    echo "ℹ️ stock_data.db.xz 沒有變更，不需要推送" >> "$LOG_FILE"
    exit 0
fi

git add stock_data.db.xz
git commit -m "📈 本地更新資料庫: $(date '+%Y-%m-%d %H:%M')" >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "ℹ️ 沒有可提交的資料庫變更" >> "$LOG_FILE"
    exit 0
fi

git push origin HEAD:main >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ git push 失敗，請檢查 GitHub 認證或遠端狀態" >> "$LOG_FILE"
    exit 1
fi

echo "✅ 資料庫已推送到 GitHub，Streamlit 會使用新版 stock_data.db.xz" >> "$LOG_FILE"
