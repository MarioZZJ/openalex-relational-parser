"""Deterministic identifier lookup backed by precomputed assignments."""
from __future__ import annotations

from typing import Callable, Dict, Mapping, Optional


class StableIdGenerator:
    """Return IDs for auxiliary namespaces based on collected assignments."""

    def __init__(
        self,
        assignments: Optional[Mapping[str, Mapping[str, int]]] = None,
        recorder: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self._assignments: Dict[str, Dict[str, int]] = {
            namespace: dict(values) for namespace, values in (assignments or {}).items()
        }
        self._recorder = recorder

    def generate(self, namespace: str, value: str, bits: int = 63) -> int:  # bits maintained for compatibility
        if not value:
            raise ValueError("value must be a non-empty string")
        if self._recorder is not None and not self._assignments:
            self._recorder(namespace, value)
            return 0
        namespace_map = self._assignments.get(namespace)
        if namespace_map is None:
            raise KeyError(f"No assignments available for namespace '{namespace}'")
        try:
            return namespace_map[value]
        except KeyError as exc:  # pragma: no cover - error path
            raise KeyError(f"Value '{value}' missing from namespace '{namespace}' assignments") from exc


__all__ = ["StableIdGenerator"]
