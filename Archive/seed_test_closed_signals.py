#!/usr/bin/env python3
from __future__ import annotations

import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def ensure_header(path: Path, fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        return
    with open(path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        existing = reader.fieldnames or []
        rows = list(reader)
    if existing == fieldnames:
        return
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, '') for k in fieldnames})


def read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with open(path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_rows(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, '') for k in fieldnames})


def append_row(path: Path, row: Dict[str, str], fieldnames: List[str]) -> None:
    ensure_header(path, fieldnames)
    rows = read_rows(path)
    rows.append({k: row.get(k, '') for k in fieldnames})
    write_rows(path, rows, fieldnames)


def latest_or_default(folder: Path, default_name: str) -> Path:
    csvs = sorted([p for p in folder.glob('*.csv') if p.name not in {'_schema.csv'}])
    return csvs[-1] if csvs else (folder / default_name)


def main() -> int:
    project_dir = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path('.').resolve()
    data_dir = project_dir / 'data'
    pending_dir = data_dir / 'pending'
    signals_dir = data_dir / 'signals'
    results_dir = data_dir / 'results'

    now = datetime.now(timezone.utc)
    day_name = f"{now:%Y-%m-%d}.csv"
    month_name = f"signals_{now:%Y-%m}.csv"
    results_name = f"results_{now:%Y-%m}.csv"

    pending_file = latest_or_default(pending_dir, day_name)
    signals_file = latest_or_default(signals_dir, month_name)
    results_file = latest_or_default(results_dir, results_name)

    pending_fields = [
        'pending_id', 'setup_id', 'created_ts_ms', 'signal_open_time', 'symbol', 'side',
        'score', 'confidence', 'reason', 'breakout_level', 'signal_price',
        'signal_high', 'signal_low', 'oi_jump_pct', 'funding_pct', 'vol_ratio',
        'strategy', 'market_regime', 'btc_price', 'btc_24h_change_pct', 'btc_4h_change_pct',
        'btc_1h_change_pct', 'btc_24h_range_pct', 'btc_4h_range_pct', 'alt_market_breadth_pct',
        'btc_regime', 'score_oi', 'score_exhaustion', 'score_breakout', 'score_retest', 'reason_tags', 'status',
        'close_reason', 'bars_waited', 'closed_ts_ms', 'send_decision', 'skip_reason'
    ]

    signal_fields = [
        'signal_id', 'setup_id', 'timestamp_ms', 'symbol', 'side', 'score', 'confidence',
        'reason', 'breakout_level', 'entry_low', 'entry_high', 'entry_ref',
        'stop', 'tp1', 'tp2', 'price', 'oi_jump_pct', 'funding_pct',
        'vol_ratio', 'retest_bars_waited', 'config_version', 'strategy', 'market_regime',
        'btc_price', 'btc_24h_change_pct', 'btc_4h_change_pct', 'btc_1h_change_pct',
        'btc_24h_range_pct', 'btc_4h_range_pct', 'alt_market_breadth_pct', 'btc_regime',
        'risk_pct_real', 'sl_distance_pct', 'tp1_distance_pct', 'tp2_distance_pct', 'break_distance_pct',
        'retest_depth_pct', 'score_oi', 'score_exhaustion', 'score_breakout', 'score_retest',
        'reason_tags', 'stop_was_forced_min_risk', 'manual_tradable', 'manual_trade_note', 'status'
    ]

    result_fields = [
        'signal_id', 'setup_id', 'timestamp_ms', 'symbol', 'side', 'entry_ref', 'stop',
        'tp1', 'tp2', 'outcome', 'r_multiple', 'bars_checked',
        'close_time_ms', 'close_reason', 'config_version', 'strategy', 'market_regime', 'btc_price',
        'btc_24h_change_pct', 'btc_4h_change_pct', 'btc_1h_change_pct', 'btc_24h_range_pct',
        'btc_4h_range_pct', 'alt_market_breadth_pct', 'btc_regime', 'risk_pct_real', 'sl_distance_pct',
        'tp1_distance_pct', 'tp2_distance_pct', 'break_distance_pct',
        'retest_depth_pct', 'score_oi', 'score_exhaustion', 'score_breakout', 'score_retest',
        'reason_tags', 'stop_was_forced_min_risk', 'mfe_pct', 'mae_pct', 'manual_tradable', 'manual_trade_note'
    ]

    for pth, fields in [(pending_file, pending_fields), (signals_file, signal_fields), (results_file, result_fields)]:
        ensure_header(pth, fields)

    # Avoid reseeding duplicates on repeated runs.
    existing_signal_ids = {r.get('signal_id', '') for r in read_rows(signals_file)}

    base_ms = int(time.time() * 1000)
    cases = [
        {
            'symbol': 'TESTCLOSE1USDT', 'side': 'SHORT', 'strategy': 'short_exhaustion_retest',
            'price': '1.2500', 'entry_ref': '1.2400', 'stop': '1.2772', 'tp1': '1.2028', 'tp2': '1.1656',
            'breakout_level': '1.2450', 'outcome': 'WIN_TP1', 'r_multiple': '1.0000', 'bars_checked': '5',
            'close_reason': 'seed_test_tp1', 'risk_pct_real': '2.20', 'sl_distance_pct': '3.00',
            'tp1_distance_pct': '3.00', 'tp2_distance_pct': '6.00', 'break_distance_pct': '0.40',
            'retest_depth_pct': '0.60', 'score': '93.5', 'confidence': '0.9350', 'oi_jump_pct': '-0.80',
            'funding_pct': '-0.0021', 'vol_ratio': '1.55', 'retest_bars_waited': '2',
            'score_oi': '0.0', 'score_exhaustion': '33.0', 'score_breakout': '28.0', 'score_retest': '32.5',
            'reason': 'TEST SEED close path TP1', 'reason_tags': 'test_seed;short;tp1',
            'market_regime': 'BEARISH', 'btc_regime': 'bearish', 'btc_price': '70000',
            'btc_24h_change_pct': '-1.20', 'btc_4h_change_pct': '-0.40', 'btc_1h_change_pct': '-0.15',
            'btc_24h_range_pct': '4.50', 'btc_4h_range_pct': '1.10', 'alt_market_breadth_pct': '0.00',
            'manual_tradable': 'yes', 'manual_trade_note': 'TEST_SEED', 'mfe_pct': '3.80', 'mae_pct': '0.90',
        },
        {
            'symbol': 'TESTCLOSE2USDT', 'side': 'LONG', 'strategy': 'long_breakout_retest',
            'price': '0.8500', 'entry_ref': '0.8600', 'stop': '0.8342', 'tp1': '0.8858', 'tp2': '0.9116',
            'breakout_level': '0.8550', 'outcome': 'LOSS_SL', 'r_multiple': '-1.0000', 'bars_checked': '4',
            'close_reason': 'seed_test_sl', 'risk_pct_real': '1.84', 'sl_distance_pct': '3.00',
            'tp1_distance_pct': '3.00', 'tp2_distance_pct': '6.00', 'break_distance_pct': '0.58',
            'retest_depth_pct': '0.72', 'score': '91.2', 'confidence': '0.9120', 'oi_jump_pct': '2.10',
            'funding_pct': '0.0012', 'vol_ratio': '1.80', 'retest_bars_waited': '1',
            'score_oi': '24.0', 'score_exhaustion': '0.0', 'score_breakout': '35.0', 'score_retest': '32.2',
            'reason': 'TEST SEED close path SL', 'reason_tags': 'test_seed;long;sl',
            'market_regime': 'BULLISH', 'btc_regime': 'bullish', 'btc_price': '70500',
            'btc_24h_change_pct': '1.40', 'btc_4h_change_pct': '0.55', 'btc_1h_change_pct': '0.10',
            'btc_24h_range_pct': '3.90', 'btc_4h_range_pct': '0.95', 'alt_market_breadth_pct': '55.00',
            'manual_tradable': 'yes', 'manual_trade_note': 'TEST_SEED', 'mfe_pct': '1.10', 'mae_pct': '3.20',
        },
        {
            'symbol': 'TESTCLOSE3USDT', 'side': 'SHORT', 'strategy': 'short_exhaustion_retest',
            'price': '5.4000', 'entry_ref': '5.3500', 'stop': '5.5105', 'tp1': '5.1895', 'tp2': '5.0290',
            'breakout_level': '5.3600', 'outcome': 'WIN_TP2', 'r_multiple': '2.0000', 'bars_checked': '7',
            'close_reason': 'seed_test_tp2', 'risk_pct_real': '2.07', 'sl_distance_pct': '3.00',
            'tp1_distance_pct': '3.00', 'tp2_distance_pct': '6.00', 'break_distance_pct': '0.19',
            'retest_depth_pct': '0.45', 'score': '95.0', 'confidence': '0.9500', 'oi_jump_pct': '-1.10',
            'funding_pct': '-0.0008', 'vol_ratio': '2.10', 'retest_bars_waited': '3',
            'score_oi': '0.0', 'score_exhaustion': '34.0', 'score_breakout': '29.0', 'score_retest': '32.0',
            'reason': 'TEST SEED close path TP2', 'reason_tags': 'test_seed;short;tp2',
            'market_regime': 'BEARISH', 'btc_regime': 'bearish', 'btc_price': '69950',
            'btc_24h_change_pct': '-2.20', 'btc_4h_change_pct': '-0.70', 'btc_1h_change_pct': '-0.18',
            'btc_24h_range_pct': '5.10', 'btc_4h_range_pct': '1.30', 'alt_market_breadth_pct': '10.00',
            'manual_tradable': 'yes', 'manual_trade_note': 'TEST_SEED', 'mfe_pct': '6.40', 'mae_pct': '0.70',
        },
    ]

    seeded = 0
    for idx, c in enumerate(cases, start=1):
        ts_open = base_ms - (idx * 600000)
        ts_pending = ts_open - 120000
        ts_close = ts_open + (idx * 180000)
        setup_id = f"{c['symbol']}-{c['side']}-TEST-{ts_open}"
        signal_id = f"{setup_id}-{idx}"
        pending_id = setup_id

        if signal_id in existing_signal_ids:
            print(f"[skip] {signal_id} already exists")
            continue

        pending_row = {
            'pending_id': pending_id,
            'setup_id': setup_id,
            'created_ts_ms': str(ts_pending),
            'signal_open_time': str(ts_open),
            'symbol': c['symbol'],
            'side': c['side'],
            'score': c['score'],
            'confidence': c['confidence'],
            'reason': c['reason'],
            'breakout_level': c['breakout_level'],
            'signal_price': c['price'],
            'signal_high': c['price'],
            'signal_low': c['entry_ref'],
            'oi_jump_pct': c['oi_jump_pct'],
            'funding_pct': c['funding_pct'],
            'vol_ratio': c['vol_ratio'],
            'strategy': c['strategy'],
            'market_regime': c['market_regime'],
            'btc_price': c['btc_price'],
            'btc_24h_change_pct': c['btc_24h_change_pct'],
            'btc_4h_change_pct': c['btc_4h_change_pct'],
            'btc_1h_change_pct': c['btc_1h_change_pct'],
            'btc_24h_range_pct': c['btc_24h_range_pct'],
            'btc_4h_range_pct': c['btc_4h_range_pct'],
            'alt_market_breadth_pct': c['alt_market_breadth_pct'],
            'btc_regime': c['btc_regime'],
            'score_oi': c['score_oi'],
            'score_exhaustion': c['score_exhaustion'],
            'score_breakout': c['score_breakout'],
            'score_retest': c['score_retest'],
            'reason_tags': c['reason_tags'],
            'status': 'CONFIRMED',
            'close_reason': '',
            'bars_waited': c['bars_checked'],
            'closed_ts_ms': '',
            'send_decision': 'SENT',
            'skip_reason': '',
        }

        signal_row = {
            'signal_id': signal_id,
            'setup_id': setup_id,
            'timestamp_ms': str(ts_open),
            'symbol': c['symbol'],
            'side': c['side'],
            'score': c['score'],
            'confidence': c['confidence'],
            'reason': c['reason'],
            'breakout_level': c['breakout_level'],
            'entry_low': c['entry_ref'],
            'entry_high': c['entry_ref'],
            'entry_ref': c['entry_ref'],
            'stop': c['stop'],
            'tp1': c['tp1'],
            'tp2': c['tp2'],
            'price': c['price'],
            'oi_jump_pct': c['oi_jump_pct'],
            'funding_pct': c['funding_pct'],
            'vol_ratio': c['vol_ratio'],
            'retest_bars_waited': c['retest_bars_waited'],
            'config_version': 'TEST_CLOSE_SEED_V1',
            'strategy': c['strategy'],
            'market_regime': c['market_regime'],
            'btc_price': c['btc_price'],
            'btc_24h_change_pct': c['btc_24h_change_pct'],
            'btc_4h_change_pct': c['btc_4h_change_pct'],
            'btc_1h_change_pct': c['btc_1h_change_pct'],
            'btc_24h_range_pct': c['btc_24h_range_pct'],
            'btc_4h_range_pct': c['btc_4h_range_pct'],
            'alt_market_breadth_pct': c['alt_market_breadth_pct'],
            'btc_regime': c['btc_regime'],
            'risk_pct_real': c['risk_pct_real'],
            'sl_distance_pct': c['sl_distance_pct'],
            'tp1_distance_pct': c['tp1_distance_pct'],
            'tp2_distance_pct': c['tp2_distance_pct'],
            'break_distance_pct': c['break_distance_pct'],
            'retest_depth_pct': c['retest_depth_pct'],
            'score_oi': c['score_oi'],
            'score_exhaustion': c['score_exhaustion'],
            'score_breakout': c['score_breakout'],
            'score_retest': c['score_retest'],
            'reason_tags': c['reason_tags'],
            'stop_was_forced_min_risk': 'false',
            'manual_tradable': c['manual_tradable'],
            'manual_trade_note': c['manual_trade_note'],
            'status': c['outcome'],
        }

        result_row = {
            'signal_id': signal_id,
            'setup_id': setup_id,
            'timestamp_ms': str(ts_open),
            'symbol': c['symbol'],
            'side': c['side'],
            'entry_ref': c['entry_ref'],
            'stop': c['stop'],
            'tp1': c['tp1'],
            'tp2': c['tp2'],
            'outcome': c['outcome'],
            'r_multiple': c['r_multiple'],
            'bars_checked': c['bars_checked'],
            'close_time_ms': str(ts_close),
            'close_reason': c['close_reason'],
            'config_version': 'TEST_CLOSE_SEED_V1',
            'strategy': c['strategy'],
            'market_regime': c['market_regime'],
            'btc_price': c['btc_price'],
            'btc_24h_change_pct': c['btc_24h_change_pct'],
            'btc_4h_change_pct': c['btc_4h_change_pct'],
            'btc_1h_change_pct': c['btc_1h_change_pct'],
            'btc_24h_range_pct': c['btc_24h_range_pct'],
            'btc_4h_range_pct': c['btc_4h_range_pct'],
            'alt_market_breadth_pct': c['alt_market_breadth_pct'],
            'btc_regime': c['btc_regime'],
            'risk_pct_real': c['risk_pct_real'],
            'sl_distance_pct': c['sl_distance_pct'],
            'tp1_distance_pct': c['tp1_distance_pct'],
            'tp2_distance_pct': c['tp2_distance_pct'],
            'break_distance_pct': c['break_distance_pct'],
            'retest_depth_pct': c['retest_depth_pct'],
            'score_oi': c['score_oi'],
            'score_exhaustion': c['score_exhaustion'],
            'score_breakout': c['score_breakout'],
            'score_retest': c['score_retest'],
            'reason_tags': c['reason_tags'],
            'stop_was_forced_min_risk': 'false',
            'mfe_pct': c['mfe_pct'],
            'mae_pct': c['mae_pct'],
            'manual_tradable': c['manual_tradable'],
            'manual_trade_note': c['manual_trade_note'],
        }

        append_row(pending_file, pending_row, pending_fields)
        append_row(signals_file, signal_row, signal_fields)
        append_row(results_file, result_row, result_fields)
        seeded += 1
        print(f"[seeded] {signal_id} -> {c['outcome']}")

    print('---')
    print(f'project_dir={project_dir}')
    print(f'pending_file={pending_file}')
    print(f'signals_file={signals_file}')
    print(f'results_file={results_file}')
    print(f'seeded={seeded}')
    print('Note: test rows use config_version=TEST_CLOSE_SEED_V1 and symbol prefix TESTCLOSE* for easy cleanup.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
