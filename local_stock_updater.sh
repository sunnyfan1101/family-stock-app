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
REVENUE_ONLY=0
FINANCIALS_ONLY=0
LEGACY_FORCED_FINANCIALS=0
for arg in "$@"; do
    if [ "$arg" = "--revenue-only" ]; then REVENUE_ONLY=1; fi
    if [ "$arg" = "--financials-only" ]; then FINANCIALS_ONLY=1; fi
    if [ "$arg" = "--force-financials" ]; then LEGACY_FORCED_FINANCIALS=1; fi
done
if [ "$REVENUE_ONLY" -eq 1 ] && [ "$FINANCIALS_ONLY" -eq 1 ]; then
    echo "Choose one staged update mode" >&2
    exit 2
fi
if [ "$REVENUE_ONLY" -eq 1 ]; then
    "$PYTHON_BIN" monthly_cutover_guard.py
fi
FINANCIAL_STATE="$PROJECT_DIR/.financial-batch-state.json"
REVENUE_STATE="$PROJECT_DIR/.revenue-batch-state.json"
# Every mode must settle prior monthly/quarterly promotions before changing live DB.
# Otherwise the next run could replace an unpublished archive.
"$PYTHON_BIN" financial_cursor.py reconcile --state "$FINANCIAL_STATE" \
    --live-xz "$PROJECT_DIR/stock_data.db.xz" --push-script "$PROJECT_DIR/push_db_to_github.sh"
"$PYTHON_BIN" financial_cursor.py reconcile --state "$REVENUE_STATE" \
    --live-xz "$PROJECT_DIR/stock_data.db.xz" --push-script "$PROJECT_DIR/push_db_to_github.sh"
if [ "$FINANCIALS_ONLY" -eq 1 ]; then
    FINANCIAL_PROGRESS="$STAGE_DIR/financial-progress.json"
    FINANCIAL_OFFSET=$("$PYTHON_BIN" financial_cursor.py current --state "$FINANCIAL_STATE")
fi
if [ "$REVENUE_ONLY" -eq 1 ]; then
    REVENUE_PROGRESS="$STAGE_DIR/revenue-progress.json"
    REVENUE_OFFSET=$("$PYTHON_BIN" financial_cursor.py current --state "$REVENUE_STATE")
fi
if [ "$REVENUE_ONLY" -eq 0 ] && [ "$FINANCIALS_ONLY" -eq 0 ] && [ "$LEGACY_FORCED_FINANCIALS" -eq 0 ]; then
    set +e
    TZ=Asia/Taipei "$PYTHON_BIN" market_session.py --date "$(TZ=Asia/Taipei date +%F)" --closure-file "$PROJECT_DIR/market_closures.json"
    SESSION_RESULT=$?
    set -e
    if [ "$SESSION_RESULT" -eq 10 ]; then
        echo "STOCK_UPDATE_SKIPPED: documented market closure/weekend"
        exit 0
    fi
    if [ "$SESSION_RESULT" -ne 0 ]; then exit "$SESSION_RESULT"; fi
fi
COPY_ARGS=()
if [ "$REVENUE_ONLY" -eq 1 ] || [ "$FINANCIALS_ONLY" -eq 1 ]; then COPY_ARGS=(--copy-only); fi
"$PYTHON_BIN" verify_copy.py --source "$PROJECT_DIR/stock_data.db" --output "$STOCK_DB_PATH" "${COPY_ARGS[@]}"
if [ "$FINANCIALS_ONLY" -eq 1 ]; then
    "$PYTHON_BIN" financials_only.py --limit "${STOCK_FINANCIALS_LIMIT:-150}" \
        --offset "$FINANCIAL_OFFSET" --progress-path "$FINANCIAL_PROGRESS" \
        --budget-path "$PROJECT_DIR/.financial-daily-budget.json"
    if [ ! -f "$STOCK_DB_PATH.xz" ]; then
        "$PYTHON_BIN" financial_cursor.py advance-nochange --state "$FINANCIAL_STATE" \
            --progress "$FINANCIAL_PROGRESS"
        echo "STOCK_UPDATE_SKIPPED: quarterly financial reports unchanged"
        exit 0
    fi
elif [ "$REVENUE_ONLY" -eq 1 ]; then
    "$PYTHON_BIN" revenue_only.py --limit "${STOCK_REVENUE_LIMIT:-40}" \
        --offset "$REVENUE_OFFSET" --progress-path "$REVENUE_PROGRESS"
    if [ ! -f "$STOCK_DB_PATH.xz" ]; then
        "$PYTHON_BIN" financial_cursor.py advance-nochange --state "$REVENUE_STATE" \
            --progress "$REVENUE_PROGRESS"
        echo "STOCK_UPDATE_SKIPPED: monthly revenue unchanged"
        exit 0
    fi
else
    "$PYTHON_BIN" fetch_data.py "$@"
fi
REQUIRE_TODAY=(--require-today)
for arg in "$@"; do
    if [ "$arg" = "--force-financials" ] || [ "$arg" = "--revenue-only" ] || [ "$arg" = "--financials-only" ]; then REQUIRE_TODAY=(); fi
done
"$PYTHON_BIN" stock_release.py validate "$STOCK_DB_PATH" --live "$PROJECT_DIR/stock_data.db" "${REQUIRE_TODAY[@]}"
if [ "$FINANCIALS_ONLY" -eq 1 ]; then
    "$PYTHON_BIN" financial_cursor.py prepare --state "$FINANCIAL_STATE" \
        --progress "$FINANCIAL_PROGRESS" --candidate-xz "$STOCK_DB_PATH.xz"
fi
if [ "$REVENUE_ONLY" -eq 1 ]; then
    "$PYTHON_BIN" financial_cursor.py prepare --state "$REVENUE_STATE" \
        --progress "$REVENUE_PROGRESS" --candidate-xz "$STOCK_DB_PATH.xz"
fi
"$PYTHON_BIN" stock_release.py promote "$STOCK_DB_PATH" --live "$PROJECT_DIR/stock_data.db" "${REQUIRE_TODAY[@]}"
if [ -f "$STOCK_FINMIND_DONE_PATH" ]; then cp "$STOCK_FINMIND_DONE_PATH" "$PROJECT_DIR/finmind_done.txt"; fi
"$PROJECT_DIR/push_db_to_github.sh"
if [ "$FINANCIALS_ONLY" -eq 1 ]; then
    "$PYTHON_BIN" financial_cursor.py finalize --state "$FINANCIAL_STATE" \
        --live-xz "$PROJECT_DIR/stock_data.db.xz"
fi
if [ "$REVENUE_ONLY" -eq 1 ]; then
    "$PYTHON_BIN" financial_cursor.py finalize --state "$REVENUE_STATE" \
        --live-xz "$PROJECT_DIR/stock_data.db.xz"
fi
echo "STOCK_UPDATE_SUCCESS $(date -Is)"
