"""
Telegram dispatch for HL whale accumulation signals.
Reuses telegram config from main config.yaml (telegram.token + telegram.chat_id).
"""

import time
from typing import Optional

import requests


def fire(coin: str, sig: dict, conc: dict, ctx: dict, cfg: dict) -> None:
    """Send a live signal alert."""
    msg = _format_message(coin, sig, conc, ctx, shadow=False)
    _send(msg, cfg)


def fire_shadow(coin: str, sig: dict, conc: dict, ctx: dict, cfg: dict) -> None:
    """Send a [SHADOW] alert (gates_pass but hl_gates_enabled=False)."""
    msg = _format_message(coin, sig, conc, ctx, shadow=True)
    _send(msg, cfg)


def _format_message(coin: str, sig: dict, conc: dict, ctx: dict, shadow: bool) -> str:
    prefix = "[SHADOW] " if shadow else ""
    mark_px = ctx.get("mark_px", 0.0)
    oi_usd = ctx.get("oi_usd", 0.0)
    funding = ctx.get("funding_8h_pct")

    real_conc = sig.get("real_concentration", conc.get("real_concentration", 0.0))
    coverage = sig.get("coverage_estimate", 0.0)
    jaccard = sig.get("topn_jaccard", 0.0)
    top_n = len(conc.get("top_n_clusters", []))
    top_n_notional = conc.get("top_n_long_notional", 0.0)

    g_str = " ".join(
        f"g{i}={'✓' if sig.get(f'g{i}') else '✗'}"
        for i in range(1, 7)
    )

    funding_str = f"{funding:.4f}%" if funding is not None else "unverified"
    pvb = sig.get("price_vs_baseline", 0.0)
    trend = sig.get("price_trend_7v30", 0.0)

    lines = [
        f"{prefix}HL WHALE ACCUM: {coin}",
        f"Mark: ${mark_px:,.2f}  |  OI: ${oi_usd/1e6:.1f}M",
        f"Real conc: {real_conc:.3f}  |  Top-{top_n} notional: ${top_n_notional/1e6:.2f}M",
        f"Coverage: {coverage:.3f}  |  Jaccard: {jaccard:.3f}",
        f"Gates: {g_str}",
        f"Funding 8h: {funding_str}  |  PvB: {pvb:+.2%}  |  Trend7v30: {trend:+.2%}",
        f"Window: {sig.get('window_time_h', '?')}h ({sig.get('snapshots_available', '?')} snapshots)",
    ]
    return "\n".join(lines)


def _send(text: str, cfg: dict) -> None:
    tg_cfg = cfg.get("telegram", {})
    token = tg_cfg.get("token", "")
    chat_id = tg_cfg.get("chat_id", "")
    if not token or not chat_id:
        print(f"[Alert] Telegram not configured — would send:\n{text}")
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
    except Exception as e:
        print(f"[Alert] Telegram send failed: {e}")
