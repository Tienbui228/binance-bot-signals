import os
import signal
import sys
import threading
import yaml


def main():
    cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    from pump_exhaustion_short.shared.r2_binance_client import R2BinanceClient
    from pump_exhaustion_short.watchlist.watchlist_manager import WatchlistManager
    from pump_exhaustion_short.loops.discovery_loop import run_discovery_loop
    from pump_exhaustion_short.loops.scan_loop import run_scan_loop
    from pump_exhaustion_short.loops.outcome_updater import run_outcome_loop
    from pump_exhaustion_short.loops.daily_exporter import run_daily_export_loop

    client = R2BinanceClient(cfg["binance"])
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    wl = WatchlistManager(data_dir)
    stop_event = threading.Event()

    threads = [
        threading.Thread(
            target=run_discovery_loop,
            args=(client, wl, cfg, stop_event),
            daemon=True,
            name="r2-discovery"
        ),
        threading.Thread(
            target=run_scan_loop,
            args=(client, wl, cfg, stop_event),
            daemon=True,
            name="r2-scan"
        ),
        threading.Thread(
            target=run_outcome_loop,
            args=(client, wl, cfg, stop_event),
            daemon=True,
            name="r2-outcome"
        ),
        threading.Thread(
            target=run_daily_export_loop,
            args=(wl, cfg, stop_event),
            daemon=True,
            name="r2-exporter"
        ),
    ]

    def _shutdown(sig, frame):
        print("\n[R2] Shutdown signal received. Stopping loops...")
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    print("[R2] pump_exhaustion_short Phase R2 starting.")
    for t in threads:
        t.start()
        print(f"[R2] Started thread: {t.name}")

    stop_event.wait()
    print("[R2] All loops stopped. Flushing state...")
    wl.flush()
    wl.sync_csvs()
    print("[R2] Done.")


if __name__ == "__main__":
    main()
