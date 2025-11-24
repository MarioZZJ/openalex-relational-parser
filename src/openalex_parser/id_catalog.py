"""Utilities to collect and persist deterministic ID assignments."""
from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping, MutableMapping, Sequence, Set

from .reference import EnumerationConfig


@dataclass(frozen=True)
class NamespaceConfig:
    """Describe a non-enumeration namespace that requires generated IDs."""

    namespace: str
    filename: str
    id_column: str
    value_column: str


class IdCatalog:
    """Collect unique values and assign sequential IDs per configuration."""

    def __init__(
        self,
        enum_configs: Sequence[EnumerationConfig],
        namespace_configs: Sequence[NamespaceConfig],
    ) -> None:
        self._enum_configs: Dict[str, EnumerationConfig] = {config.table: config for config in enum_configs}
        self._namespace_configs: Dict[str, NamespaceConfig] = {
            config.namespace: config for config in namespace_configs
        }
        self._enum_values: MutableMapping[str, Set[str]] = defaultdict(set)
        self._namespace_values: MutableMapping[str, Set[str]] = defaultdict(set)
        self.enum_assignments: Dict[str, Dict[str, int]] = {}
        self.namespace_assignments: Dict[str, Dict[str, int]] = {}

    def record_enum(self, table: str, value: str) -> None:
        if value:
            self._enum_values[table].add(value)

    def record_namespace(self, namespace: str, value: str) -> None:
        if value:
            self._namespace_values[namespace].add(value)

    def finalize(self, reference_dir: Path) -> None:
        """Assign IDs and write CSV files to *reference_dir*."""
        reference_dir.mkdir(parents=True, exist_ok=True)
        self.enum_assignments = {}
        for table, config in self._enum_configs.items():
            values = self._enum_values.get(table, set())
            assignments = self._assign(values)
            self.enum_assignments[table] = assignments
            filename = config.reference_filename or f"{table}.csv"
            path = reference_dir / filename
            self._write_records(
                path,
                (config.id_column, config.value_column),
                ({config.id_column: identifier, config.value_column: value} for value, identifier in assignments.items()),
            )

        self.namespace_assignments = {}
        for namespace, config in self._namespace_configs.items():
            values = self._namespace_values.get(namespace, set())
            assignments = self._assign(values)
            self.namespace_assignments[namespace] = assignments
            path = reference_dir / config.filename
            self._write_records(
                path,
                (config.id_column, config.value_column),
                ({config.id_column: identifier, config.value_column: value} for value, identifier in assignments.items()),
            )

    def load_existing(self, reference_dir: Path) -> bool:
        """Load assignments from an existing reference directory if possible."""
        if not reference_dir.exists():
            return False

        enum_assignments: Dict[str, Dict[str, int]] = {}
        for table, config in self._enum_configs.items():
            filename = config.reference_filename or f"{table}.csv"
            path = reference_dir / filename
            if not path.exists():
                return False
            enum_assignments[table] = self._read_assignments(path, config.id_column, config.value_column)

        namespace_assignments: Dict[str, Dict[str, int]] = {}
        for namespace, config in self._namespace_configs.items():
            path = reference_dir / config.filename
            if not path.exists():
                return False
            namespace_assignments[namespace] = self._read_assignments(path, config.id_column, config.value_column)

        self.enum_assignments = enum_assignments
        self.namespace_assignments = namespace_assignments
        return True

    @staticmethod
    def _assign(values: Set[str]) -> Dict[str, int]:
        ordered = sorted(values, key=lambda text: (text.casefold(), text))
        return {value: index for index, value in enumerate(ordered, start=1)}

    @staticmethod
    def _write_records(path: Path, headers: Iterable[str], records: Iterable[Dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(headers), delimiter="\t")
            writer.writeheader()
            for row in records:
                writer.writerow(row)

    @staticmethod
    def _read_assignments(path: Path, id_column: str, value_column: str) -> Dict[str, int]:
        assignments: Dict[str, int] = {}
        with path.open(encoding="utf-8-sig", newline="") as handle:
            sample = handle.read(2048)
            handle.seek(0)
            delimiter = "\t" if "\t" in sample else ","
            reader = csv.DictReader(handle, delimiter=delimiter)
            for row in reader:
                raw_id = row.get(id_column)
                raw_value = row.get(value_column)
                if not raw_id or not raw_value:
                    continue
                try:
                    assignments[raw_value] = int(raw_id)
                except ValueError:
                    continue
        return assignments


__all__ = ["IdCatalog", "NamespaceConfig"]
