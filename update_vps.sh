#!/usr/bin/env bash
# update_vps.sh — safe VPS bot updater (systemd edition)
# Usage: ./update_vps.sh [--check-only]
# --check-only: fetch + print state, no changes
set -euo pipefail

REPO=/root/binance_bot_signals
LOCK=$REPO/oi_scanner.lock
PYTHON=/root/venv/bin/python
LOG=$REPO/UPDATE_LOG.txt

# ── CHECK-ONLY ────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--check-only" ]]; then
    echo "=== CHECK-ONLY (no changes will be made) ==="
    cd "$REPO"
    git fetch origin
    echo ""
    echo "--- Dirty tracked files (should be empty for pull to work) ---"
    DIRTY=$(git status --porcelain --untracked-files=no)
    if [[ -n "$DIRTY" ]]; then
        echo "WARN: dirty tracked files found:"
        echo "$DIRTY"
    else
        echo "(clean)"
    fi
    echo ""
    echo "--- HEAD vs origin/main ---"
    git log --oneline -1 HEAD
    git log --oneline -1 origin/main
    git rev-list --left-right --count HEAD...origin/main | awk '{print "ahead=" $1 "  behind=" $2}'
    echo ""
    echo "--- systemd oibot status ---"
    systemctl status oibot --no-pager 2>/dev/null || echo "(unit not active)"
    echo ""
    echo "--- Running processes ---"
    pgrep -af "oi_scanner|hl_scanner" || echo "(none)"
    echo ""
    echo "--- Lock holder (flock) ---"
    lsof "$LOCK" 2>/dev/null || echo "(no lock holder found)"
    echo ""
    echo "--- Current build markers ---"
    grep 'CODE_BUILD_ID = ' "$REPO/oi_scanner.py" | head -1
    echo ""
    cat "$REPO/RUNNING_CODE_VERSION.txt" 2>/dev/null || echo "RUNNING_CODE_VERSION.txt: not found"
    echo ""
    echo "=== CHECK-ONLY DONE — exiting without changes ==="
    exit 0
fi

# ── STEP 1: dirty check ───────────────────────────────────────────────────────
echo "[1/8] Fetching origin and checking dirty tracked files..."
cd "$REPO"
git fetch origin
DIRTY=$(git status --porcelain --untracked-files=no)
if [[ -n "$DIRTY" ]]; then
    echo "ABORT: dirty tracked files found — resolve before updating:"
    echo "$DIRTY"
    exit 1
fi
echo "[1/8] PASS — working tree clean"

# ── STEP 2: stop via systemd, then orphan fallback ───────────────────────────
echo "[2/8] Stopping oi_scanner (systemd first, then orphan fallback)..."
systemctl stop oibot 2>/dev/null || true

# Orphan fallback — kills any oi_scanner.py still running (screen orphans, manual starts)
_ORPHAN_WAIT=0
while pgrep -f "oi_scanner.py" > /dev/null 2>&1; do
    # Try PID from lock file first, then lsof
    _OPID=$(cat "$LOCK" 2>/dev/null | tr -d '[:space:]' || true)
    if [[ -z "$_OPID" ]]; then
        _OPID=$(lsof -t "$LOCK" 2>/dev/null || true)
    fi
    if [[ -n "$_OPID" ]]; then
        echo "[2/8] orphan fallback: SIGTERM $_OPID"
        kill -TERM "$_OPID" 2>/dev/null || true
    fi
    if [[ $_ORPHAN_WAIT -ge 30 ]]; then
        echo "ABORT: oi_scanner still running after 30s — investigate manually."
        echo "       Check: pgrep -af oi_scanner; lsof $LOCK"
        exit 1
    fi
    sleep 2
    _ORPHAN_WAIT=$((_ORPHAN_WAIT + 2))
done
WAIT=$_ORPHAN_WAIT
echo "[2/8] PASS — process stopped after ${WAIT}s"

# ── STEP 3: CUT_MS + log ─────────────────────────────────────────────────────
echo "[3/8] Recording CUT_MS..."
INCOMING_COMMIT=$(git log --oneline -1 origin/main | cut -d' ' -f1)
CUT_MS=$($PYTHON -c 'import time; print(int(time.time()*1000))')
echo "[3/8] CUT_MS=$CUT_MS  incoming_commit=$INCOMING_COMMIT"
echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') | CUT_MS=$CUT_MS | commit=$INCOMING_COMMIT" >> "$LOG"

# ── STEP 4: pull ─────────────────────────────────────────────────────────────
echo "[4/8] Pulling (--ff-only)..."
git pull --ff-only
echo "[4/8] PASS — pull done"

# ── STEP 5: read new build marker ────────────────────────────────────────────
NEW_BUILD=$(grep 'CODE_BUILD_ID = ' "$REPO/oi_scanner.py" | head -1 | sed 's/.*= *"\(.*\)".*/\1/')
echo "[5/8] New CODE_BUILD_ID on disk: $NEW_BUILD"

# ── STEP 6: start bot via systemd ─────────────────────────────────────────────
echo "[6/8] Starting bot via systemd..."
systemctl start oibot
PULL_EPOCH=$(date +%s)
echo "[6/8] PASS — systemctl start oibot done"

# ── STEP 7: verify ───────────────────────────────────────────────────────────
echo "[7/8] Sleeping 90s for bot startup + RUNNING_CODE_VERSION.txt write..."
sleep 90

# Check systemd MainPID
MAIN_PID=$(systemctl show -p MainPID oibot 2>/dev/null | cut -d= -f2 | tr -d '[:space:]' || true)
if [[ -z "$MAIN_PID" || "$MAIN_PID" == "0" ]]; then
    echo "ABORT: systemctl MainPID=0 after 90s — oibot unit not running"
    systemctl status oibot --no-pager || true
    exit 1
fi
echo "[7/8] systemd MainPID=$MAIN_PID"

# Cross-check with lock file
LOCK_PID=$(cat "$LOCK" 2>/dev/null | tr -d '[:space:]' || true)
if [[ -z "$LOCK_PID" ]]; then
    echo "WARN: oi_scanner.lock is empty — process may still be initializing"
elif [[ "$LOCK_PID" != "$MAIN_PID" ]]; then
    echo "ABORT: PID mismatch — systemd MainPID=$MAIN_PID but lock file PID=$LOCK_PID"
    exit 1
else
    echo "[7/8] lock file PID=$LOCK_PID matches MainPID — OK"
fi

# Read RUNNING_CODE_VERSION.txt
if [[ ! -f "$REPO/RUNNING_CODE_VERSION.txt" ]]; then
    echo "ABORT: RUNNING_CODE_VERSION.txt not found after 90s"
    exit 1
fi

RUNNING_BUILD=$(grep 'code_build_id=' "$REPO/RUNNING_CODE_VERSION.txt" | cut -d= -f2)
WRITTEN_AT=$(grep 'written_at_utc=' "$REPO/RUNNING_CODE_VERSION.txt" | cut -d= -f2)
echo "[7/8] RUNNING_CODE_VERSION.txt  build=$RUNNING_BUILD  written_at=$WRITTEN_AT"

if [[ "$RUNNING_BUILD" != "$NEW_BUILD" ]]; then
    echo "ABORT: build mismatch — RUNNING=$RUNNING_BUILD  EXPECTED=$NEW_BUILD"
    echo "       Possible causes: old lock still held, process crashed at startup, flock guard rejected second instance."
    exit 1
fi
echo "[7/8] PASS — build markers match"

# ── STEP 8: summary ──────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo " UPDATE COMPLETE"
echo "════════════════════════════════════════"
echo " PASS [1] Dirty check: clean"
echo " PASS [2] Process stopped (orphan-safe, ${WAIT}s)"
echo " PASS [3] CUT_MS=$CUT_MS"
echo " PASS [4] git pull --ff-only"
echo " PASS [5] Disk build: $NEW_BUILD"
echo " PASS [6] systemctl start oibot"
echo " PASS [7] build verified; MainPID=$MAIN_PID; lock=$LOCK_PID"
echo ""
echo " >>> INFRASTRUCTURE PASS"
echo " >>> BEHAVIOR PASS requires:"
echo "     fresh rows with created_ts_ms >= $CUT_MS"
echo "     Wait 1-2 scan cycles (~10 min), then check:"
echo "     journalctl -u oibot --since '20 min ago'"
echo "     data/signals/ (send_research_status, send_private_status)"
echo "════════════════════════════════════════"
