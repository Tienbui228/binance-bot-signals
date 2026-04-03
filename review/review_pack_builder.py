"""
review/review_pack_builder.py
------------------------------
Phase F: canonical home for review pack rendering.

Current state: thin re-export. The actual rendering logic lives in
build_daily_review_pack.py (external builder script) which is called
by BinanceScanner.build_daily_review_pack() in oi_scanner.py.

Must:
  - Render from truth only
  - Be a pure renderer — no semantic invention, no truth repair

Must NOT:
  - Invent or repair regime_label, confirmed_ts_ms, sent_ts_ms, closed_ts_ms
  - Insert fake timestamps
  - Introduce new stage names beyond the 4 canonical stages
  - Own any CSV writes

Future: build_daily_review_pack.py logic will be moved here once
the existing DOCX renderer is confirmed stable.
"""
from __future__ import annotations


def build_review_pack(scanner, date_str: str) -> bool:
    """Render the daily review pack for the given date.

    Thin wrapper — delegates to BinanceScanner.build_daily_review_pack()
    which calls the external build_daily_review_pack.py script.

    Args:
        scanner: BinanceScanner instance
        date_str: Date string in YYYY-MM-DD format

    Returns:
        True if pack built successfully, False otherwise.
    """
    return scanner.build_daily_review_pack(date_str)
