#!/usr/bin/env bash
# Push only the validated compressed DB using a fresh disposable clone.
set -euo pipefail
export PATH="/usr/bin:/bin:/home/sunny/.npm-global/bin:/home/sunny/.local/bin:$PATH"
export GIT_TERMINAL_PROMPT=0
PROJECT_DIR="/home/sunny/family-stock-app"
LOG_FILE="$PROJECT_DIR/cron_local.log"
DB_XZ_FILE="$PROJECT_DIR/stock_data.db.xz"
cd "$PROJECT_DIR"
if [ ! -s "$DB_XZ_FILE" ]; then
    echo "DB_PUSH_FAILED: compressed DB missing" >> "$LOG_FILE"
    exit 1
fi
# This entrypoint can also be run manually, outside the staged updater.
# Never publish a nonempty but truncated/mismatched archive.
"$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/stock_release.py" validate "$PROJECT_DIR/stock_data.db" >> "$LOG_FILE" 2>&1
REMOTE_URL=$(git config --get remote.origin.url)
PUSH_DIR=$(mktemp -d /tmp/family-stock-push-XXXXXXXX)
trap 'status=$?; if [ "$status" -ne 0 ]; then echo "DB_PUSH_FAILED exit=$status worktree=$PUSH_DIR" >> "$LOG_FILE"; fi' EXIT
git clone --quiet --depth 1 --single-branch --branch main "$REMOTE_URL" "$PUSH_DIR" >> "$LOG_FILE" 2>&1
cp "$DB_XZ_FILE" "$PUSH_DIR/stock_data.db.xz"
git -C "$PUSH_DIR" add -- stock_data.db.xz
if git -C "$PUSH_DIR" diff --cached --quiet -- stock_data.db.xz; then
    echo "DB_PUSH_UNCHANGED" >> "$LOG_FILE"
    exit 0
fi
git -C "$PUSH_DIR" commit -m "📈 本地更新資料庫: $(date '+%Y-%m-%d %H:%M')" >> "$LOG_FILE" 2>&1
git -C "$PUSH_DIR" push origin HEAD:main >> "$LOG_FILE" 2>&1
echo "DB_PUSH_SUCCESS commit=$(git -C "$PUSH_DIR" rev-parse --short HEAD)" >> "$LOG_FILE"
