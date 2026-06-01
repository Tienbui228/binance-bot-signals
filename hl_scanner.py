"""
Hyperliquid Whale Accumulation Scanner — standalone entry point.

Run: python hl_scanner.py
     python hl_scanner.py --config config_noreview.yaml

Requires config.yaml with:
  hl_whale_accum.enabled: true
  hl_whale_accum.hl_gates_enabled: false  (shadow mode until §13 validation)

Background thread: fills ingestion loop (polls recentTrades per coin every 5 min).
Main thread: scan_once() loop via HlScanner.run_forever().

DO NOT integrate into oi_scanner.py — this is a separate process for a different exchange.
"""

import sys
import threading
import time
import traceback
import yaml
import os


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


def fills_loop(cfg: dict, universe, registry) -> None:
    """Background thread: ingest recent fills per watchlist coin every 5 minutes."""
    hl_cfg = cfg.get("hl_whale_accum", {})
    fill_interval = int(hl_cfg.get("fills_poll_interval_sec", 300))
    print("[FillsLoop] Started")
    while True:
        try:
            coins = universe.get_watchlist_coins()
            for coin in coins:
                try:
                    added = registry.ingest_fills(coin)
                    if added:
                        print(f"[FillsLoop] {coin}: +{added} new addresses")
                except Exception as e:
                    print(f"[FillsLoop] {coin} error: {e}")
                time.sleep(1)
        except Exception:
            traceback.print_exc()
        time.sleep(fill_interval)


def main():
    config_path = "config.yaml"
    for i, arg in enumerate(sys.argv[1:]):
        if arg in ("--config", "-c") and i + 1 < len(sys.argv) - 1:
            config_path = sys.argv[i + 2]
        elif not arg.startswith("-"):
            config_path = arg

    if not os.path.exists(config_path):
        print(f"[hl_scanner] Config not found: {config_path}")
        sys.exit(1)

    cfg = load_config(config_path)
    hl_cfg = cfg.get("hl_whale_accum", {})

    if not hl_cfg.get("enabled", False):
        print("[hl_scanner] hl_whale_accum.enabled=false in config — exiting")
        sys.exit(0)

    gates_enabled = hl_cfg.get("hl_gates_enabled", False)
    print(f"[hl_scanner] Starting. hl_gates_enabled={gates_enabled} ({'LIVE' if gates_enabled else 'SHADOW'})")

    # Import here so process exits cleanly if hl disabled without importing anything
    from scanner.strategies.hyperliquid.universe import HlUniverse
    from scanner.strategies.hyperliquid.registry import AddressRegistry
    from scanner.strategies.hyperliquid.scanner import HlScanner

    universe = HlUniverse(cfg)
    registry = AddressRegistry(cfg)
    scanner = HlScanner(cfg, universe, registry)

    # Background fills ingestion
    fill_thread = threading.Thread(
        target=fills_loop,
        args=(cfg, universe, registry),
        name="hl_fills_ingestion",
        daemon=True,
    )
    fill_thread.start()
    print("[hl_scanner] Fills ingestion thread started")

    # Main scan loop (blocks)
    scanner.run_forever()


if __name__ == "__main__":
    main()
