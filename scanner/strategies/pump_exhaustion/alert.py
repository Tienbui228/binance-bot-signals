import time
from typing import Dict

from scanner.strategies.pump_exhaustion.config import get_pump_cfg


def send_alert(msg: str, confidence: str, cfg: Dict) -> None:
    """Print to terminal always. POST to Telegram if enabled. Fail-soft."""
    pump_cfg = get_pump_cfg(cfg)
    alert_cfg = pump_cfg.get("alert", {})

    if alert_cfg.get("terminal_enabled", True):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        print(f"\n{'='*55}")
        print(f"[PUMP_EXHAUSTION][{confidence}][{ts}]")
        print(msg)
        print(f"{'='*55}\n")

    if alert_cfg.get("telegram_enabled", False):
        token = alert_cfg.get("telegram_token", "")
        chat_id = alert_cfg.get("telegram_chat_id", "")
        if not token or not chat_id:
            return
        try:
            import requests
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
        except Exception as e:
            print(f"[PumpAlert] Telegram failed: {e}")
