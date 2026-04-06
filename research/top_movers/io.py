"""
research/top_movers/io.py

Handles:
- Cache directory setup
- Raw API response caching (JSON per symbol per day)
- CSV output writing
- Output directory management

Does NOT write to any live bot directories.
"""

import csv
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


CACHE_BASE = "data/research_cache/binance_proxy"
OUTPUT_BASE = "data/research_output/top_movers"

# Number of pre-day lookback bars at 5m (8h = 96 bars)
LOOKBACK_BARS_5M = 96
LOOKBACK_HOURS = 8


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def day_window_ms(research_day: str):
    """Return (day_start_ms, day_end_ms) for a research day in UTC."""
    dt = datetime.strptime(research_day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(dt.timestamp() * 1000)
    end_ms = start_ms + 24 * 3600 * 1000 - 1
    return start_ms, end_ms


def lookback_start_ms(day_start_ms: int, hours: int = LOOKBACK_HOURS) -> int:
    """Return the start of the pre-day lookback window."""
    return day_start_ms - hours * 3600 * 1000


def normalize_ts_5m(ts_ms: int) -> int:
    """Normalize a millisecond timestamp to the nearest 5m bucket floor."""
    bucket = 5 * 60 * 1000
    return (ts_ms // bucket) * bucket


def normalize_ts_15m(ts_ms: int) -> int:
    bucket = 15 * 60 * 1000
    return (ts_ms // bucket) * bucket


def normalize_ts_1h(ts_ms: int) -> int:
    bucket = 60 * 60 * 1000
    return (ts_ms // bucket) * bucket


def ts_to_str(ts_ms: int) -> str:
    """Convert ms timestamp to readable UTC string."""
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def output_dir(research_day: str) -> str:
    return os.path.join(OUTPUT_BASE, research_day)


def ensure_output_dirs(research_day: str) -> str:
    base = output_dir(research_day)
    for sub in ["csv", "images", "report"]:
        os.makedirs(os.path.join(base, sub), exist_ok=True)
    os.makedirs(os.path.join(CACHE_BASE, research_day), exist_ok=True)
    return base


def csv_path(research_day: str, filename: str) -> str:
    return os.path.join(output_dir(research_day), "csv", filename)


def image_path(research_day: str, case_id: str, image_key: str) -> str:
    """e.g. image_key = 'P0_context_1h'"""
    return os.path.join(output_dir(research_day), "images", f"{case_id}_{image_key}.png")


def report_path(research_day: str) -> str:
    return os.path.join(output_dir(research_day), "report", "daily_top_mover_research_report.md")


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_path(research_day: str, key: str) -> str:
    return os.path.join(CACHE_BASE, research_day, f"{key}.json")


def save_cache(research_day: str, key: str, data: Any) -> None:
    path = _cache_path(research_day, key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


def load_cache(research_day: str, key: str) -> Optional[Any]:
    path = _cache_path(research_day, key)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def cache_exists(research_day: str, key: str) -> bool:
    return os.path.exists(_cache_path(research_day, key))


def load_or_fetch(research_day: str, key: str, fetch_fn) -> Any:
    """Load from cache or call fetch_fn() and cache result."""
    cached = load_cache(research_day, key)
    if cached is not None:
        return cached
    data = fetch_fn()
    save_cache(research_day, key, data)
    return data


# ---------------------------------------------------------------------------
# CSV output helpers
# ---------------------------------------------------------------------------

def write_csv(filepath: str, rows: List[Dict]) -> None:
    if not rows:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            f.write("")
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(filepath: str, content: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)


# ---------------------------------------------------------------------------
# Safe value helpers
# ---------------------------------------------------------------------------

def safe_round(val: Optional[float], ndigits: int = 4) -> Optional[float]:
    if val is None:
        return None
    try:
        return round(float(val), ndigits)
    except Exception:
        return None


def fmt(val: Optional[float], ndigits: int = 4) -> str:
    if val is None:
        return ""
    return str(round(float(val), ndigits))
