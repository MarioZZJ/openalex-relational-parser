"""Transformer for funder entities."""
from __future__ import annotations

from typing import Dict

from ..emitter import TableEmitter
from ..reference import EnumerationRegistry
from ..identifiers import StableIdGenerator
from ..utils import (
    canonical_openalex_id,
    canonical_wikidata_id,
    numeric_openalex_id,
    parse_iso_date,
    parse_iso_datetime,
)


class FunderTransformer:
    """Map OpenAlex funder JSON documents to relational rows."""

    def __init__(
        self,
        emitter: TableEmitter,
        enums: EnumerationRegistry,
        id_generator: StableIdGenerator,
    ) -> None:
        self._emitter = emitter
        self._enums = enums
        self._ids = id_generator

    def transform(self, record: Dict[str, object]) -> None:
        funder_id = numeric_openalex_id(record.get("id"))
        if funder_id is None:
            return
        self._emit_funder(funder_id, record)
        self._emit_alternative_names(funder_id, record)
        self._emit_publishers(funder_id, record)

    # ------------------------------------------------------------------
    def _emit_funder(self, funder_id: int, record: Dict[str, object]) -> None:
        ids = record.get("ids") or {}
        wikidata_id = canonical_wikidata_id(ids.get("wikidata"))
        wikipedia_url = ids.get("wikipedia")
        ror = ids.get("ror") or record.get("ror")
        if ror:
            ror = ror.rstrip("/").split("/")[-1]

        self._emitter.emit(
            "funder",
            {
                "funder_id": funder_id,
                "funder": record.get("display_name"),
                "country_iso_alpha2_code": record.get("country_code"),
                "description": record.get("description"),
                "homepage_url": record.get("homepage_url"),
                "ror_id": ror,
                "openalex_id": canonical_openalex_id(record.get("id")),
                "wikidata_id": wikidata_id,
                "image_url": record.get("image_url"),
                "thumbnail_url": record.get("image_thumbnail_url"),
                "updated_date": parse_iso_datetime(record.get("updated_date")),
                "created_date": parse_iso_date(record.get("created_date")),
            },
        )

    # ------------------------------------------------------------------
    def _emit_alternative_names(self, funder_id: int, record: Dict[str, object]) -> None:
        alternatives = record.get("alternate_titles") or []
        seq = 0
        seen = set()
        for name in alternatives:
            if not name or name in seen:
                continue
            seen.add(name)
            seq += 1
            self._emitter.emit(
                "funder_alternative_name",
                {
                    "funder_id": funder_id,
                    "alternative_name_seq": seq,
                    "alternative_name": name,
                },
            )

    # ------------------------------------------------------------------
    def _emit_publishers(self, funder_id: int, record: Dict[str, object]) -> None:
        roles = record.get("roles") or []
        seq = 0
        for role in roles:
            if not isinstance(role, dict):
                continue
            if role.get("role") != "publisher":
                continue
            publisher_id = numeric_openalex_id(role.get("id"))
            if publisher_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "funder_publisher",
                {
                    "funder_id": funder_id,
                    "publisher_seq": seq,
                    "publisher_id": publisher_id,
                },
            )


__all__ = ["FunderTransformer"]
