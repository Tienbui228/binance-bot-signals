import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


class Storage:
    def __init__(
        self,
        table_specs: Dict[str, Dict],
        snapshots_dir: Path,
        snapshot_index_file: Path,
        snapshot_fields: List[str],
        include_legacy_flat_files: bool = False,
    ):
        self.table_specs = table_specs
        self.snapshots_dir = snapshots_dir
        self.snapshot_index_file = snapshot_index_file
        self.snapshot_fields = snapshot_fields
        self.include_legacy_flat_files = include_legacy_flat_files

    def ensure_header(self, path: Path, fieldnames: List[str]):
        if not path.exists():
            with open(path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
            return
        try:
            with open(path, "r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                existing = reader.fieldnames or []
                rows = list(reader)
        except Exception:
            existing, rows = [], []
        if existing == fieldnames:
            return
        migrated = [{name: row.get(name, "") for name in fieldnames} for row in rows]
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(migrated)
        print(f"[schema migrate] {path.name} -> columns={len(fieldnames)}")

    def ensure_storage_layout(self):
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        for spec in self.table_specs.values():
            spec["dir"].mkdir(parents=True, exist_ok=True)
        self.ensure_header(self.snapshot_index_file, self.snapshot_fields)
        self.ensure_table_seed("market_review")

    def ensure_table_seed(self, table_key: str):
        spec = self.table_specs[table_key]
        if not spec.get("fieldnames"):
            return
        seed = spec["dir"] / "_schema.csv"
        if not seed.exists():
            with open(seed, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=spec["fieldnames"])
                writer.writeheader()

    def table_key_for_path(self, path: Path) -> Optional[str]:
        for key, spec in self.table_specs.items():
            if path == spec["logical"]:
                return key
        return None

    def partition_key(self, ts_ms: int, granularity: str) -> str:
        dt = datetime.fromtimestamp(max(float(ts_ms), 0.0) / 1000.0, tz=timezone.utc)
        if granularity == "day":
            return dt.strftime("%Y-%m-%d")
        return dt.strftime("%Y-%m")

    def table_partition_file(self, table_key: str, row: Dict) -> Path:
        spec = self.table_specs[table_key]
        ts_col = spec.get("ts_col")
        if ts_col:
            raw_ts = row.get(ts_col)
            try:
                if raw_ts in (None, "", "nan"):
                    ts_ms = int(time.time() * 1000)
                else:
                    ts_ms = int(float(raw_ts))
            except Exception:
                ts_ms = int(time.time() * 1000)
        else:
            ts_ms = int(time.time() * 1000)
        part = self.partition_key(ts_ms, spec["granularity"])
        return spec["dir"] / f"{table_key}_{part}.csv"

    def iter_table_files(self, table_key: str) -> List[Path]:
        spec = self.table_specs[table_key]
        files = sorted(path for path in spec["dir"].glob("*.csv") if path.name != "_schema.csv")
        if self.include_legacy_flat_files and spec["logical"].exists():
            files = [spec["logical"]] + files
        return files

    def normalize_row_for_fields(self, row: Dict, fieldnames: List[str]) -> Dict:
        return {name: row.get(name, "") for name in fieldnames}

    def read_csv(self, path: Path) -> List[Dict]:
        table_key = self.table_key_for_path(path)
        if table_key:
            fieldnames = self.table_specs[table_key].get("fieldnames")
            rows: List[Dict] = []
            for file_path in self.iter_table_files(table_key):
                if not file_path.exists():
                    continue
                try:
                    with open(file_path, "r", newline="", encoding="utf-8") as handle:
                        file_rows = list(csv.DictReader(handle))
                    if fieldnames:
                        file_rows = [self.normalize_row_for_fields(row, fieldnames) for row in file_rows]
                    rows.extend(file_rows)
                except Exception:
                    continue
            return rows
        if not path.exists():
            return []
        with open(path, "r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def write_csv(self, path: Path, rows: List[Dict], fieldnames: List[str]):
        table_key = self.table_key_for_path(path)
        if table_key:
            spec = self.table_specs[table_key]
            target_dir = spec["dir"]
            target_dir.mkdir(parents=True, exist_ok=True)
            for old_file in target_dir.glob("*.csv"):
                if old_file.name != "_schema.csv":
                    old_file.unlink()
            grouped: Dict[Path, List[Dict]] = {}
            for row in rows:
                file_path = self.table_partition_file(table_key, row)
                grouped.setdefault(file_path, []).append(self.normalize_row_for_fields(row, fieldnames))
            for file_path, grouped_rows in grouped.items():
                with open(file_path, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(grouped_rows)
            self.ensure_table_seed(table_key)
            return
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def append_csv(self, path: Path, row: Dict, fieldnames: List[str]):
        table_key = self.table_key_for_path(path)
        if table_key:
            file_path = self.table_partition_file(table_key, row)
            exists = file_path.exists()
            with open(file_path, "a", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                if not exists:
                    writer.writeheader()
                writer.writerow(self.normalize_row_for_fields(row, fieldnames))
            return
        with open(path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writerow(row)
