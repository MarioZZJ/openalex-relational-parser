"""Stable identifier generation utilities."""
from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Dict


class StableIdGenerator:
    """Generate deterministic, collision-aware integer identifiers."""

    def __init__(self) -> None:
        self._value_to_id: Dict[str, Dict[str, int]] = defaultdict(dict)
        self._id_to_value: Dict[str, Dict[int, str]] = defaultdict(dict)

    def generate(self, namespace: str, value: str, bits: int = 63) -> int:
        if value in self._value_to_id[namespace]:
            return self._value_to_id[namespace][value]

        mask = (1 << bits) - 1
        attempt = 0
        while True:
            payload = f"{namespace}:{value}:{attempt}".encode("utf-8")
            digest = hashlib.blake2b(payload, digest_size=8).digest()
            candidate = int.from_bytes(digest, byteorder="big") & mask
            if candidate == 0:
                attempt += 1
                continue
            existing = self._id_to_value[namespace].get(candidate)
            if existing is None or existing == value:
                self._id_to_value[namespace][candidate] = value
                self._value_to_id[namespace][value] = candidate
                return candidate
            attempt += 1


__all__ = ["StableIdGenerator"]
