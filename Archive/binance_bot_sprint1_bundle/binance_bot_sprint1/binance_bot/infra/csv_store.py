from __future__ import annotations

import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


class CsvTableStore:
    def ensure_header(self, path: Path, fieldnames: List[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            with open(path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
            return
        with open(path, "r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            existing = reader.fieldnames or []
            rows = list(reader)
        if existing == fieldnames:
            return
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in fieldnames})

    def read_csv(self, path: Path) -> List[Dict[str, str]]:
        if not path.exists():
            return []
        with open(path, "r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def write_csv(self, path: Path, rows: Iterable[Dict], fieldnames: List[str]) -> None:
        self.ensure_header(path, fieldnames)
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in fieldnames})

    def append_csv(self, path: Path, row: Dict, fieldnames: List[str]) -> None:
        self.ensure_header(path, fieldnames)
        with open(path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    @staticmethod
    def partition_key(ts_ms: int, granularity: str) -> str:
        dt = datetime.fromtimestamp(max(float(ts_ms), 0.0) / 1000.0, tz=timezone.utc)
        if granularity == "day":
            return dt.strftime("%Y-%m-%d")
        return dt.strftime("%Y-%m")

    def partition_path(self, directory: Path, table_key: str, row: Dict, ts_col: Optional[str], granularity: str) -> Path:
        if ts_col and row.get(ts_col) not in (None, "", "nan"):
            try:
                ts_ms = int(float(row[ts_col]))
            except Exception:
                ts_ms = int(time.time() * 1000)
        else:
            ts_ms = int(time.time() * 1000)
        part = self.partition_key(ts_ms, granularity)
        return directory / f"{table_key}_{part}.csv"
