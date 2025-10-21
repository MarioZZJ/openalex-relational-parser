"""Iterators over OpenAlex snapshot JSON data."""
from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

JsonDict = Dict[str, object]

try:
    import orjson  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover - optional dependency
    orjson = None

_json_loads = orjson.loads if orjson is not None else json.loads
print(f"Using {'orjson' if orjson is not None else 'json'} for JSON parsing.")


@dataclass
class ProgressReporter:
    """Lightweight progress reporter that prints every *interval* records."""

    label: str
    interval: int = 1000
    _count: int = 0

    def __call__(self, increment: int = 1) -> None:
        self._count += increment
        if self._count % self.interval == 0:
            print(f"{self.label}: processed {self._count:,} records", flush=True)

    def summary(self) -> str:
        return f"{self.label}: processed {self._count:,} records"


class SnapshotReader:
    """Utility for iterating over OpenAlex snapshot entities."""

    def __init__(self, snapshot_root: Path) -> None:
        if not snapshot_root.exists():
            raise FileNotFoundError(f"Snapshot root {snapshot_root} does not exist")
        self.snapshot_root = snapshot_root
        self._last_file_count = 0

    def _resolve_entity_root(self, entity: str) -> Path:
        entity_root = self.snapshot_root / entity
        if not entity_root.exists():
            raise FileNotFoundError(f"Entity {entity} not found under {self.snapshot_root}")
        return entity_root

    def iter_entity(
        self,
        entity: str,
        updated_dates: Optional[Iterable[str]] = None,
        max_files: Optional[int] = None,
        max_records: Optional[int] = None,
        progress: Optional[ProgressReporter] = None,
    ) -> Iterator[JsonDict]:
        """Yield parsed JSON documents for the requested entity."""

        entity_root = self._resolve_entity_root(entity)
        if updated_dates:
            directories = [entity_root / f"updated_date={value}" for value in sorted(set(updated_dates))]
        else:
            directories = sorted(
                path for path in entity_root.iterdir() if path.is_dir() and path.name.startswith("updated_date=")
            )

        yielded = 0
        files_read = 0

        for directory in directories:
            if not directory.exists():
                continue
            part_files = sorted(
                path for path in directory.iterdir() if path.is_file() and path.suffix == ".gz"
            )
            for part_file in part_files:
                files_read += 1
                yield from self._iter_file(part_file, max_records, progress, yielded)
                yielded += self._last_file_count
                if max_records is not None and yielded >= max_records:
                    return
                if max_files is not None and files_read >= max_files:
                    return

    def _iter_file(
        self,
        path: Path,
        max_records: Optional[int],
        progress: Optional[ProgressReporter],
        already_yielded: int,
    ) -> Iterator[JsonDict]:
        self._last_file_count = 0
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                document = _json_loads(line)
                yield document
                self._last_file_count += 1
                if progress:
                    progress()
                if max_records is not None and already_yielded + self._last_file_count >= max_records:
                    return


__all__ = ["JsonDict", "ProgressReporter", "SnapshotReader"]
