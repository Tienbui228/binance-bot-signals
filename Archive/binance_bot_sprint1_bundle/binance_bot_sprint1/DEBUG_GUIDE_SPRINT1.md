# Debug guide — Sprint 1

## What this sprint changes
- restores `save_signal`, `save_pending`, `sync_pending_send_decision`, and `close_pending` as real `BinanceScanner` methods
- creates a modular package skeleton without forcing a full rewrite yet
- adds smoke tests so refactor work can stop breaking the base silently

## 1) Syntax check before anything else
```bash
python -m py_compile oi_scanner_sprint1_hotfix.py
```
Expected result: no output.

## 2) Confirm the four critical methods are class methods
```bash
python - <<'PY'
import importlib.util
spec = importlib.util.spec_from_file_location('scanner', 'oi_scanner_sprint1_hotfix.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
for name in ['save_signal', 'save_pending', 'sync_pending_send_decision', 'close_pending']:
    print(name, hasattr(mod.BinanceScanner, name))
PY
```
Expected result: all four print `True`.

## 3) Run smoke tests
From the sprint folder root:
```bash
python -m unittest discover -s binance_bot/tests -t . -v
```

## 4) Runtime startup check
Use your real config path:
```bash
python oi_scanner_sprint1_hotfix.py config.yaml
```
Expected early logs:
- build marker / startup version logs
- no immediate `AttributeError` for the four restored methods

Press Ctrl+C after startup verification if you do not want a full loop run.

## 5) Simulation check
```bash
python oi_scanner_sprint1_hotfix.py config.yaml --simulate-case confirmed_win
```
Expected result:
- `SIMULATION_OK`
- printed pending_id / setup_id / signal_id

## 6) Manual debug pack check
```bash
python oi_scanner_sprint1_hotfix.py config.yaml --build-review-pack 2026-03-28
```
Expected result:
- `REVIEW_PACK_OK` or a clean builder error message
- no crash due to missing `save_pending` / `close_pending`

## 7) If runtime still fails, debug in this order
1. syntax/import
   - run `py_compile`
   - inspect missing local modules such as `review_capture_runtime`
2. class-method existence
   - run the method existence snippet above
3. CLI path
   - test `--simulate-case` before full runtime loop
4. review-pack path
   - test `--build-review-pack` separately from runtime
5. data-path issues
   - inspect missing directories under `data/`, `review_exports/`, `review_workspace/`
6. semantic review issues
   - only after the above passes, inspect whether pending/confirmed/sent/close semantics are wrong

## 8) What not to debug yet
Do not start tweaking strategy thresholds, invalidation logic, or review scoring while the base is still failing smoke checks.

## 9) Next migration step after this hotfix passes
- extract Binance IO into `binance_bot/infra/binance_client.py`
- extract CSV helpers into `binance_bot/infra/csv_store.py`
- then move strategy helper math into `binance_bot/strategies/shared_filters.py`
