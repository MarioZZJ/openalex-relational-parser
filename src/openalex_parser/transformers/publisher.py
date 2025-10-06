"""Transformer for publisher entities."""
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


class PublisherTransformer:
    """Map OpenAlex publisher JSON documents to relational rows."""

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
        publisher_id = numeric_openalex_id(record.get("id"))
        if publisher_id is None:
            return
        self._emit_publisher(publisher_id, record)
        self._emit_alternative_names(publisher_id, record)
        self._emit_countries(publisher_id, record)

    # ------------------------------------------------------------------
    def _emit_publisher(self, publisher_id: int, record: Dict[str, object]) -> None:
        ids = record.get("ids") or {}
        wikidata_id = canonical_wikidata_id(ids.get("wikidata"))
        wikipedia_url = ids.get("wikipedia")
        ror = ids.get("ror")
        if ror:
            ror = ror.rstrip("/").split("/")[-1]
        parent = record.get("parent_publisher") or {}
        parent_id = numeric_openalex_id(parent.get("id")) if isinstance(parent, dict) else None

        self._emitter.emit(
            "publisher",
            {
                "publisher_id": publisher_id,
                "publisher": record.get("display_name"),
                "hierarchy_level": record.get("hierarchy_level"),
                "parent_publisher_id": parent_id,
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
    def _emit_alternative_names(self, publisher_id: int, record: Dict[str, object]) -> None:
        names = record.get("alternate_titles") or []
        seen = set()
        seq = 0
        for name in names:
            if not name or name in seen:
                continue
            seen.add(name)
            seq += 1
            self._emitter.emit(
                "publisher_alternative_name",
                {
                    "publisher_id": publisher_id,
                    "alternative_name_seq": seq,
                    "alternative_name": name,
                },
            )

    # ------------------------------------------------------------------
    def _emit_countries(self, publisher_id: int, record: Dict[str, object]) -> None:
        country_codes = record.get("country_codes") or []
        seq = 0
        seen = set()
        for code in country_codes:
            if not code or code in seen:
                continue
            seen.add(code)
            seq += 1
            self._emitter.emit(
                "publisher_country",
                {
                    "publisher_id": publisher_id,
                    "country_seq": seq,
                    "country_iso_alpha2_code": code,
                },
            )


__all__ = ["PublisherTransformer"]
