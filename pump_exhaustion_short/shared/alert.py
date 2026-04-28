import time
from typing import Dict


def send_alert(msg: str, level: str, cfg: Dict) -> None:
    """Print to terminal always. POST to Telegram if enabled. Fail-soft."""
    alert_cfg = cfg.get("alert", {})
    if alert_cfg.get("terminal_enabled", True):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        print(f"[R2-ALERT][{level}][{ts}] {msg}")

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
            print(f"[R2-ALERT] Telegram failed: {e}")
