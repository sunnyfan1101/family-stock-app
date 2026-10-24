#!/usr/bin/env bash
# Staged quarterly archive, deliberately not activated by cron.
set -euo pipefail
exec /home/sunny/family-stock-app/local_stock_updater.sh --financials-only
