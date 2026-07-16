#!/bin/bash
# push_db_to_github.sh - commit and push only the compressed database used by Streamlit Cloud

set -u

export PATH="/usr/bin:/bin:/home/sunny/.npm-global/bin:/home/sunny/.local/bin:$PATH"
export GIT_TERMINAL_PROMPT=0

PROJECT_DIR="/home/sunny/family-stock-app"
LOG_FILE="$PROJECT_DIR/cron_local.log"
DB_XZ_FILE="$PROJECT_DIR/stock_data.db.xz"
PUSH_REPO_DIR="${PUSH_REPO_DIR:-/home/sunny/.cache/family-stock-app-push}"

cd "$PROJECT_DIR" || exit 1

echo "----------------------------------------" >> "$LOG_FILE"
echo "☁️ 資料庫推送開始 | $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"

if [ ! -f "$DB_XZ_FILE" ]; then
    echo "❌ 找不到 stock_data.db.xz，取消推送" >> "$LOG_FILE"
    exit 1
fi

REMOTE_URL="$(git config remote.origin.url)"

if [ ! -d "$PUSH_REPO_DIR/.git" ]; then
    mkdir -p "$(dirname "$PUSH_REPO_DIR")"
    git clone --depth 1 "$REMOTE_URL" "$PUSH_REPO_DIR" >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        echo "❌ 建立 shallow push repo 失敗" >> "$LOG_FILE"
        exit 1
    fi
else
    git -C "$PUSH_REPO_DIR" fetch --depth 1 origin main >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        echo "❌ shallow repo fetch 失敗，取消推送" >> "$LOG_FILE"
        exit 1
    fi
    git -C "$PUSH_REPO_DIR" checkout main >> "$LOG_FILE" 2>&1
    git -C "$PUSH_REPO_DIR" reset --hard origin/main >> "$LOG_FILE" 2>&1
fi

cp "$DB_XZ_FILE" "$PUSH_REPO_DIR/stock_data.db.xz"

if git -C "$PUSH_REPO_DIR" diff --quiet -- stock_data.db.xz; then
    echo "ℹ️ stock_data.db.xz 沒有變更，不需要推送" >> "$LOG_FILE"
    exit 0
fi

git -C "$PUSH_REPO_DIR" add stock_data.db.xz
git -C "$PUSH_REPO_DIR" commit -m "📈 本地更新資料庫: $(date '+%Y-%m-%d %H:%M')" >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "ℹ️ 沒有可提交的資料庫變更" >> "$LOG_FILE"
    exit 0
fi

git -C "$PUSH_REPO_DIR" push origin HEAD:main >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ git push 失敗，請檢查 GitHub 認證或遠端狀態" >> "$LOG_FILE"
    exit 1
fi

echo "✅ 資料庫已推送到 GitHub，Streamlit 會使用新版 stock_data.db.xz" >> "$LOG_FILE"
