from pathlib import Path
import re
import sys

path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('/root/binance_bot_signals/oi_scanner.py')
text = path.read_text(encoding='utf-8')
orig = text

repls = [
    ('strategy: str = "legacy_5m_retest"', 'strategy: str = "long_breakout_retest"'),
    ('return "legacy_5m_retest"', 'return "long_breakout_retest"'),
    ('strategy="legacy_5m_retest",', 'strategy="long_breakout_retest",'),
    ('long_oi_cfg = self.cfg.get("legacy_5m_retest", {})', 'long_oi_cfg = self.cfg.get("long_breakout_retest", {}) or self.cfg.get("legacy_5m_retest", {})'),
    ('return "short_exhaustion_retest" if side == "SHORT" else "legacy_5m_retest"', 'return "short_exhaustion_retest" if side == "SHORT" else "long_breakout_retest"'),
    ('return "legacy_5m_retest"\n', 'return "long_breakout_retest"\n'),
    ("strategy = row.get('strategy', 'legacy_5m_retest') or 'legacy_5m_retest'", "strategy = row.get('strategy', 'long_breakout_retest') or 'long_breakout_retest'"),
    ("key = f\"{row.get('strategy','legacy_5m_retest')}|{row.get('side','UNKNOWN')}\"", "key = f\"{row.get('strategy','long_breakout_retest')}|{row.get('side','UNKNOWN')}\""),
]
for old, new in repls:
    text = text.replace(old, new)

# add close formatter before telegram_send if not present
if 'def format_close_notification(' not in text:
    marker = '    def telegram_send(self, text: str):\n'
    insert = '''    def format_close_notification(self, signal_row: Dict, outcome: str, r_multiple: float, close_reason: str) -> str:\n        symbol = signal_row.get("symbol", "")\n        side = signal_row.get("side", "")\n        strategy = signal_row.get("strategy", "")\n        side_icon = "[LONG]" if side == "LONG" else "[SHORT]"\n        entry_ref = self._to_float(signal_row.get("entry_ref"), 0.0)\n        stop = self._to_float(signal_row.get("stop"), 0.0)\n        tp1 = self._to_float(signal_row.get("tp1"), 0.0)\n        tp2 = self._to_float(signal_row.get("tp2"), 0.0)\n        return (\n            f"[CLOSED] {side_icon} #{symbol} | {outcome} | R {r_multiple:+.2f}\\n\\n"\n            f"Strategy: {strategy}\\n"\n            f"Entry: {entry_ref:.6g}\\n"\n            f"Stop: {stop:.6g}\\n"\n            f"TP1: {tp1:.6g}\\n"\n            f"TP2: {tp2:.6g}\\n\\n"\n            f"Reason: {close_reason}\\n\\n"\n            f"#{side} #{symbol} #CLOSED #BINANCE"\n        )\n\n'''
    text = text.replace(marker, insert + marker)

# add close telegram notify block near end of close_signal
old_block = '        signals = self.read_csv(self.signals_file)\n        for row in signals:\n            if row.get("signal_id") == signal_row.get("signal_id"):\n                row["status"] = outcome\n        self.write_csv(self.signals_file, signals, fieldnames=self.signal_fields)\n'
new_block = '''        signals = self.read_csv(self.signals_file)\n        for row in signals:\n            if row.get("signal_id") == signal_row.get("signal_id"):\n                row["status"] = outcome\n        self.write_csv(self.signals_file, signals, fieldnames=self.signal_fields)\n\n        print(f"[close] {signal_row.get('symbol','')} {signal_row.get('side','')} | {outcome} | r={r_multiple:+.2f} | {close_reason}")\n        if self.cfg.get("telegram", {}).get("send_close_notifications", True):\n            try:\n                close_msg = self.format_close_notification(signal_row, outcome, r_multiple, close_reason)\n                self.telegram_send(close_msg)\n            except Exception as e:\n                print(f"[telegram close error] {e}")\n'''
if old_block in text and '[telegram close error]' not in text:
    text = text.replace(old_block, new_block)

if text == orig:
    print('No changes made; patterns not found or already patched.')
else:
    backup = path.with_suffix(path.suffix + '.bak_long_close')
    backup.write_text(orig, encoding='utf-8')
    path.write_text(text, encoding='utf-8')
    print(f'Patched: {path}')
    print(f'Backup:  {backup}')
