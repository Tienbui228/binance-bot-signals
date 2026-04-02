#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def load_csv_or_dir(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.is_dir():
        frames = []
        for fp in sorted(path.glob('*.csv')):
            if fp.name.endswith('_schema.csv') or fp.name == '_schema.csv':
                continue
            try:
                frames.append(pd.read_csv(fp))
            except pd.errors.EmptyDataError:
                continue
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def main() -> None:
    ap = argparse.ArgumentParser(description='Audit bot truth-layer consistency.')
    ap.add_argument('--data-dir', type=Path, default=Path('.'))
    args = ap.parse_args()

    base = args.data_dir.resolve()
    pending_path = (base / 'data' / 'pending') if (base / 'data' / 'pending').exists() else (base / 'pending_setups.csv')
    signals_path = (base / 'data' / 'signals') if (base / 'data' / 'signals').exists() else (base / 'signals.csv')
    results_path = (base / 'data' / 'results') if (base / 'data' / 'results').exists() else (base / 'results.csv')

    pending = load_csv_or_dir(pending_path)
    signals = load_csv_or_dir(signals_path)
    results = load_csv_or_dir(results_path)

    pending_setup_ids = set()
    signal_setup_ids = set()
    signal_ids = set()
    result_signal_ids = set()

    if not pending.empty:
        pending_setup_ids = set(pending.get('setup_id', pending.get('pending_id', pd.Series(dtype=str))).fillna('').astype(str))
    if not signals.empty:
        signal_setup_ids = set(signals.get('setup_id', pd.Series(dtype=str)).fillna('').astype(str))
        signal_ids = set(signals.get('signal_id', pd.Series(dtype=str)).fillna('').astype(str))
    if not results.empty:
        result_signal_ids = set(results.get('signal_id', pd.Series(dtype=str)).fillna('').astype(str))

    confirmed_not_sent = 0
    if not pending.empty and 'status' in pending.columns:
        statuses = pending['status'].fillna('').astype(str).str.upper()
        send_decisions = pending.get('send_decision', pd.Series('', index=pending.index)).fillna('').astype(str).str.upper()
        confirmed_not_sent = int(((statuses == 'CONFIRMED') & (send_decisions != 'SENT')).sum())

    signal_closed_missing_result = 0
    if not signals.empty and 'status' in signals.columns:
        sig_status = signals['status'].fillna('').astype(str).str.upper()
        closed_signal_ids = set(signals.loc[sig_status == 'CLOSED', 'signal_id'].fillna('').astype(str))
        signal_closed_missing_result = len(closed_signal_ids - result_signal_ids)

    result_without_signal = len(result_signal_ids - signal_ids)
    signal_without_pending = len({sid for sid in signal_setup_ids if sid} - {sid for sid in pending_setup_ids if sid})

    print('AUDIT PIPELINE')
    print(f'pending_rows={len(pending)}')
    print(f'signal_rows={len(signals)}')
    print(f'result_rows={len(results)}')
    print(f'confirmed_not_sent={confirmed_not_sent}')
    print(f'signal_closed_missing_result={signal_closed_missing_result}')
    print(f'result_without_signal={result_without_signal}')
    print(f'signal_without_pending={signal_without_pending}')


if __name__ == '__main__':
    main()
