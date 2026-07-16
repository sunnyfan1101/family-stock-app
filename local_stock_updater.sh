#!/bin/bash
# local_stock_updater.sh - 本地股價與月營收更新腳本
# 執行內容：解壓縮 → fetch_data.py（股價 + 月營收 + 預計算）

# 🛡️ 確保 PATH 包含必要的指令
export PATH="/usr/bin:/bin:/home/sunny/.npm-global/bin:/home/sunny/.local/bin:$PATH"

cd /home/sunny/family-stock-app

LOG_FILE="/home/sunny/family-stock-app/cron_local.log"
PYTHON_BIN="/home/sunny/family-stock-app/venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="/usr/bin/python3"
fi

echo "========================================" >> $LOG_FILE
echo "📈 股價更新開始 | $(date '+%Y-%m-%d %H:%M:%S')" >> $LOG_FILE

# 1. 解壓縮資料庫（如果有 .xz）
echo "📦 解壓縮資料庫..." >> $LOG_FILE
if [ -f "stock_data.db.xz" ]; then
    # 🛡️ 保留 .xz 備份（用 -k 參數）
    rm -f stock_data.db 2>/dev/null
    xz -d -k stock_data.db.xz 2>> $LOG_FILE
    if [ $? -eq 0 ]; then
        echo "✅ 解壓縮完成" >> $LOG_FILE
    else
        echo "⚠️ 解壓縮失敗" >> $LOG_FILE
    fi
else
    echo "⚠️ 找不到 stock_data.db.xz" >> $LOG_FILE
fi

# 2. 執行股價 + 月營收 + 預計算
echo "🚀 執行 fetch_data.py..." >> $LOG_FILE
"$PYTHON_BIN" fetch_data.py "$@" >> $LOG_FILE 2>&1
FETCH_EXIT=$?
if [ $FETCH_EXIT -eq 0 ]; then
    echo "✅ fetch_data.py 完成" >> $LOG_FILE
    echo "🔍 執行 DB 健康檢查..." >> $LOG_FILE
    "$PYTHON_BIN" db_health_check.py >> $LOG_FILE 2>&1
    HEALTH_EXIT=$?
    if [ $HEALTH_EXIT -ne 0 ]; then
        echo "🛑 DB 健康檢查失敗，取消 GitHub 推送，結束碼: $HEALTH_EXIT" >> $LOG_FILE
        exit $HEALTH_EXIT
    fi
    echo "✅ DB 健康檢查通過" >> $LOG_FILE

    echo "☁️ 推送新版資料庫到 GitHub..." >> $LOG_FILE
    /home/sunny/family-stock-app/push_db_to_github.sh >> $LOG_FILE 2>&1
    PUSH_EXIT=$?
    if [ $PUSH_EXIT -eq 0 ]; then
        echo "✅ GitHub 資料庫推送完成" >> $LOG_FILE
    else
        echo "⚠️ GitHub 資料庫推送失敗，結束碼: $PUSH_EXIT" >> $LOG_FILE
    fi
else
    echo "⚠️ fetch_data.py 結束碼: $FETCH_EXIT" >> $LOG_FILE
fi

echo "✅ 股價更新完成 | $(date '+%Y-%m-%d %H:%M:%S')" >> $LOG_FILE
echo "" >> $LOG_FILE
