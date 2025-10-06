"""Table emission helpers with de-duplication awareness."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, Mapping, MutableMapping, Sequence, Tuple

from .csv_writer import CsvWriterManager

Row = Mapping[str, object]
KeyFields = Sequence[str]


def _build_key(row: Row, fields: KeyFields) -> Tuple[object, ...]:
    return tuple(row.get(field) for field in fields)


class TableEmitter:
    """Emit rows to CSV writers while avoiding duplicate dimension rows."""

    def __init__(self, writers: CsvWriterManager, dedupe_keys: Mapping[str, KeyFields] | None = None) -> None:
        self._writers = writers
        self._dedupe_keys: Dict[str, KeyFields] = dict(dedupe_keys or {})
        self._seen: Dict[str, set[Tuple[object, ...]]] = defaultdict(set)

    def emit(self, table: str, row: Row) -> None:
        key_fields = self._dedupe_keys.get(table)
        if key_fields:
            key = _build_key(row, key_fields)
            if None in key:
                raise ValueError(f"Missing key value for table {table}: {key}")
            if key in self._seen[table]:
                return
            self._seen[table].add(key)
        self._writers.write_row(table, row)

    def emit_many(self, table: str, rows: Iterable[Row]) -> None:
        for row in rows:
            self.emit(table, row)


__all__ = ["TableEmitter"]
