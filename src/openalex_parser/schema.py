"""Utilities for reading table definitions from the CWTS OpenAlex schema."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class ColumnDefinition:
    """Represents a column in a table definition."""

    name: str
    raw_definition: str


@dataclass(frozen=True)
class TableDefinition:
    """Represents a table and the ordered list of its columns."""

    name: str
    columns: List[ColumnDefinition]

    @property
    def column_names(self) -> List[str]:
        """Return the column names in the order they appear in the schema."""

        return [column.name for column in self.columns]


def _normalise_identifier(identifier: str) -> str:
    """Remove schema qualifiers or double quotes from an identifier."""

    identifier = identifier.strip()
    if identifier.lower().startswith("public."):
        identifier = identifier.split(".", 1)[1]
    if identifier.startswith('"') and identifier.endswith('"'):
        identifier = identifier[1:-1]
    return identifier


def parse_schema(sql: str) -> Dict[str, TableDefinition]:
    """Parse the CWTS SQL schema and return table definitions keyed by name."""

    table_start_pattern = re.compile(r"CREATE TABLE\s+public\.([\"A-Za-z0-9_]+)\s*\(", re.IGNORECASE)
    column_pattern = re.compile(r'^("?[A-Za-z0-9_]+"?)')

    tables: Dict[str, TableDefinition] = {}
    current_table_name: str | None = None
    current_columns: List[ColumnDefinition] = []

    for original_line in sql.splitlines():
        line = original_line.strip()
        if not line or line.startswith("--"):
            continue

        table_match = table_start_pattern.match(line)
        if table_match:
            if current_table_name is not None:
                raise ValueError(
                    f"Encountered start of table definition for {table_match.group(1)} before closing "
                    f"previous table {current_table_name}."
                )
            current_table_name = _normalise_identifier(table_match.group(1))
            current_columns = []
            continue

        if current_table_name is None:
            # Ignore everything until we hit the next CREATE TABLE statement.
            continue

        if line.startswith(")"):
            tables[current_table_name] = TableDefinition(name=current_table_name, columns=current_columns)
            current_table_name = None
            current_columns = []
            continue

        upper_line = line.upper()
        if upper_line.startswith("CONSTRAINT") or upper_line.startswith("PRIMARY KEY") or upper_line.startswith("UNIQUE") or upper_line.startswith("FOREIGN KEY"):
            continue

        # Attempt to parse a column definition.
        working_line = line.rstrip(",")
        column_match = column_pattern.match(working_line)
        if column_match:
            column_name = _normalise_identifier(column_match.group(1))
            current_columns.append(ColumnDefinition(name=column_name, raw_definition=working_line))

    if current_table_name is not None:
        raise ValueError(f"Unclosed table definition for {current_table_name} in schema file.")

    return tables


def load_schema(path: Path) -> Dict[str, TableDefinition]:
    """Read an SQL schema file located at *path* and return table definitions."""

    sql_text = path.read_text(encoding="utf-8")
    return parse_schema(sql_text)


__all__ = ["ColumnDefinition", "TableDefinition", "parse_schema", "load_schema"]
