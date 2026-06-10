# RUNTIME_VALIDATION_DISCIPLINE

Status: mandatory discipline for any runtime patch / deploy / validation
Updated: 2026-06-02
Supersedes: RUNTIME_DEPLOY_TEST_GUARDRAILS, POST_PATCH_CHECKLIST

This file is long-lived. It is not topic-specific. Read it before validating any change
that touches `oi_scanner.py`, lifecycle truth, stage capture, or live CSV outputs.

---

## 1. The core trap: code-on-disk ≠ code-in-memory

Python loads source into memory at process start. Editing a `.py` after the process is
already running does **not** change the running process.

Dangerous state:
- file on disk = patched and correct
- process in memory = old and wrong
- logs = often clean
- validation = fails (or falsely passes) in misleading ways

Worse inside long-lived `screen` sessions: a session from days ago can keep old code alive.

The real question is never "does the file contain the patch?" — it is
**"is the running process using the patched code?"**

---

## 2. Single-writer rule

Never run `scan_once` / simulate while the daemon is alive. Two writers race the same CSVs
and corrupt truth. Correct order, always:

```
kill daemon → run test → restart daemon
```

Never run `--simulate-case` against the real data dir — it emits `SIM*` rows into
pending/signals/results that must then be cleaned. If you need the harness, point it at a
temporary data dir.

---

## 3. Mandatory post-patch procedure

```text
□ Patch verified present in the file on disk
□ CODE_BUILD_ID bumped in oi_scanner.py (NOT by editing RUNNING_CODE_VERSION.txt — runtime overwrites it)
□ screen -list checked; any session older than the patch killed
□ CUT_MS recorded AFTER killing old sessions, BEFORE starting the new one
□ Fresh runtime session started
□ Process start time (lstart) verified newer than patch time
□ RUNNING_CODE_VERSION.txt shows the new build marker
□ Fresh rows generated after CUT_MS
□ Validation run on fresh rows ONLY
□ Infrastructure pass separated from behavior pass
□ Historical reclassification not confused with live-fix proof
```

### Minimal commands

```bash
# running process start time
ps -eo pid,lstart,cmd | grep oi_scanner | grep -v grep

# old sessions
screen -list

# CUT_MS (after kill, before start)
python3 -c "import time; print(int(time.time()*1000))"

# build marker
cat RUNNING_CODE_VERSION.txt
```

`CODE_BUILD_ID` lives as a literal in `oi_scanner.py`. `RUNNING_CODE_VERSION.txt` is written
by the runtime on start — editing the `.txt` directly is pointless; it gets overwritten.

---

## 4. Validation discipline

### Behavior-pass ≠ infrastructure-pass
A patch can pass syntax, imports, file-grep, and "scan OK" and still fail real behavior.
`import OK` is infrastructure. The real pass condition is correct behavior on fresh rows.

### Fresh-rows only
Judge behavior only from rows satisfying at least one of:
- `created_ts_ms >= CUT_MS`
- `confirmed_ts_ms >= CUT_MS`

Never infer success or failure from arbitrary old rows in a cumulative daily CSV.

### One build marker per patch
Each patch gets its own `CODE_BUILD_ID`. Reusing the previous marker means you can no
longer tell, from `RUNNING_CODE_VERSION.txt`, which build is running.

---

## 5. Trust-but-verify (project-wide working style)

- **Trust evidence, not memory** — including the project owner's memory. Every conclusion
  needs grep / output / numbers behind it.
- **Dry-run before any destructive op.** (This discipline once caught a wrong-delete of 341 rows.)
- **Re-grep before deleting/editing.** When in doubt, keep.
- **One revertable commit per change.** Tag/backup before any one-way operation.

---

## 6. Known recurring traps

- **Old screen session** keeps old code in memory for days.
- **File-level grep gives false confidence** — patch is on disk, behavior still from old process.
- **Moving the validation window** (2h→4h→6h) does nothing if the process was never restarted.
- **A cleaner report/reclassification looks like a runtime fix** — it is not. Only fresh rows
  after `CUT_MS` prove live behavior changed.

---

## 7. Minimum pass criteria for a runtime patch

All must be true:
- new process start time (lstart) is after patch time
- `RUNNING_CODE_VERSION.txt` shows the new build marker
- validation uses fresh rows after `CUT_MS`
- expected fields/stages appear correctly on those fresh rows
- no false-semantic artifacts on fresh rows

---

## 8. If a patch looks present on disk but validation still fails

1. `screen -list` first
2. verify process start time
3. verify build marker
4. only then inspect fresh rows after `CUT_MS`
5. do not stack more patches until the restart is proven
