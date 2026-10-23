#!/usr/bin/env bash
# Dedicated monthly revenue job; inherits the same staging and release lock.
set -euo pipefail
exec /home/sunny/family-stock-app/local_stock_updater.sh --revenue-only
