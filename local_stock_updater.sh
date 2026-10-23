#!/usr/bin/env bash
# Staged update; never remove the live DB before validating its replacement.
set -euo pipefail
export PATH="/usr/bin:/bin:/home/sunny/.npm-global/bin:/home/sunny/.local/bin:$PATH"
PROJECT_DIR="/home/sunny/family-stock-app"
LOG_FILE="$PROJECT_DIR/cron_local.log"
PYTHON_BIN="$PROJECT_DIR/venv/bin/python"
cd "$PROJECT_DIR"
exec 8>/tmp/family_stock_local_db_update.lock
if ! flock -n 8; then
    echo "STOCK_UPDATE_SKIPPED: existing update holds lock" >> "$LOG_FILE"
    exit 75
fi
exec >> "$LOG_FILE" 2>&1
trap 'status=$?; if [ "$status" -ne 0 ]; then echo "STOCK_UPDATE_FAILED exit=$status stage=${STAGE_DIR:-none} date=$(date -Is)"; fi' EXIT
echo "STOCK_UPDATE_START $(date -Is)"
if [ ! -f stock_data.db ]; then
    echo "No live DB; refusing automatic restoration/promotion" >&2
    exit 1
fi
STAGE_PARENT="$PROJECT_DIR/.stock-staging"
mkdir -p "$STAGE_PARENT"
STAGE_DIR=$(mktemp -d "$STAGE_PARENT/run-$(date +%Y%m%d-%H%M%S)-XXXXXX")
export STOCK_DB_PATH="$STAGE_DIR/stock_data.db"
export STOCK_FINMIND_DONE_PATH="$STAGE_DIR/finmind_done.txt"
export STOCK_REQUIRE_FRESH_UNIVERSE=1
if [ -f finmind_done.txt ]; then cp finmind_done.txt "$STOCK_FINMIND_DONE_PATH"; fi
"$PYTHON_BIN" verify_copy.py --source "$PROJECT_DIR/stock_data.db" --output "$STOCK_DB_PATH"
"$PYTHON_BIN" fetch_data.py "$@"
REQUIRE_TODAY=(--require-today)
for arg in "$@"; do
    if [ "$arg" = "--force-financials" ]; then REQUIRE_TODAY=(); fi
done
"$PYTHON_BIN" stock_release.py validate "$STOCK_DB_PATH" --live "$PROJECT_DIR/stock_data.db" "${REQUIRE_TODAY[@]}"
"$PYTHON_BIN" stock_release.py promote "$STOCK_DB_PATH" --live "$PROJECT_DIR/stock_data.db" "${REQUIRE_TODAY[@]}"
if [ -f "$STOCK_FINMIND_DONE_PATH" ]; then cp "$STOCK_FINMIND_DONE_PATH" "$PROJECT_DIR/finmind_done.txt"; fi
"$PROJECT_DIR/push_db_to_github.sh"
echo "STOCK_UPDATE_SUCCESS $(date -Is)"
