"""Helpers to load and manage reference enumeration data."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional

from .emitter import TableEmitter
from .identifiers import StableIdGenerator


@dataclass
class EnumerationConfig:
    table: str
    id_column: str
    value_column: str
    bits: int
    reference_filename: Optional[str] = None
    normalise: Callable[[str], str] | None = None


class EnumerationRegistry:
    """Manage enumerations such as work types or licenses."""

    def __init__(self, emitter: TableEmitter, reference_dir: Optional[Path] = None) -> None:
        self._emitter = emitter
        self._reference_dir = reference_dir
        self._generator = StableIdGenerator()
        self._configs: Dict[str, EnumerationConfig] = {}
        self._value_to_id: Dict[str, Dict[str, int]] = {}
        self._id_to_value: Dict[str, Dict[int, str]] = {}

    def register(self, config: EnumerationConfig) -> None:
        self._configs[config.table] = config
        self._value_to_id.setdefault(config.table, {})
        self._id_to_value.setdefault(config.table, {})
        if config.reference_filename and self._reference_dir:
            self._load_reference(config)

    def _load_reference(self, config: EnumerationConfig) -> None:
        path = self._reference_dir / config.reference_filename
        if not path.exists():
            return
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                raw_id = row.get(config.id_column)
                raw_value = row.get(config.value_column)
                if not raw_id or not raw_value:
                    continue
                try:
                    identifier = int(raw_id)
                except ValueError:
                    continue
                value = self._normalise(config, raw_value)
                if not value:
                    continue
                self._value_to_id[config.table][value] = identifier
                self._id_to_value[config.table][identifier] = value
                emit_row = {config.id_column: identifier, config.value_column: value}
                self._emitter.emit(config.table, emit_row)

    def id_for(self, table: str, raw_value: Optional[str]) -> Optional[int]:
        if raw_value is None:
            return None
        config = self._configs[table]
        value = self._normalise(config, raw_value)
        if not value:
            return None
        table_map = self._value_to_id[table]
        if value in table_map:
            return table_map[value]
        identifier = self._generator.generate(table, value, bits=config.bits)
        table_map[value] = identifier
        self._id_to_value[table][identifier] = value
        emit_row = {config.id_column: identifier, config.value_column: value}
        self._emitter.emit(config.table, emit_row)
        return identifier

    @staticmethod
    def _normalise(config: EnumerationConfig, value: str) -> str:
        if config.normalise:
            return config.normalise(value)
        return value.strip()


__all__ = ["EnumerationConfig", "EnumerationRegistry"]
