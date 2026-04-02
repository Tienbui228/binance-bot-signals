#!/usr/bin/env bash
set -eu
set -o pipefail

cd /root/binance_bot_signals
DAY=$(TZ=Asia/Ho_Chi_Minh date -d 'yesterday' +%F)

/usr/bin/python3 oi_scanner.py --build-review-pack "$DAY"

SRC="/root/binance_bot_signals/review_exports/debug"
DST="/root/binance_bot_signals/review_exports"

LATEST=$(ls -1t "$SRC"/daily_review_debug_"$DAY"_*.docx 2>/dev/null | head -n 1 || true)

if [ -z "$LATEST" ]; then
  echo "[export] no debug pack found for $DAY"
  exit 1
fi

cp "$LATEST" "$DST/daily_review_$DAY.docx"
echo "[export] final pack -> $DST/daily_review_$DAY.docx"
