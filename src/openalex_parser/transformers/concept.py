"""Transformer for concept entities."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from ..emitter import TableEmitter
from ..reference import EnumerationRegistry
from ..identifiers import StableIdGenerator
from ..utils import (
    canonical_openalex_id,
    canonical_wikidata_id,
    numeric_openalex_id,
    parse_iso_date,
    parse_iso_datetime,
    safe_int,
)


class ConceptTransformer:
    """Map OpenAlex concept JSON documents to relational rows."""

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
        concept_id = numeric_openalex_id(record.get("id"))
        if concept_id is None:
            return
        self._emit_concept(concept_id, record)
        self._emit_ancestors(concept_id, record)
        self._emit_international(concept_id, record)
        self._emit_related(concept_id, record)
        self._emit_umls(concept_id, record)

    # ------------------------------------------------------------------
    def _emit_concept(self, concept_id: int, record: Dict[str, object]) -> None:
        ids = record.get("ids") or {}
        wikidata_id = canonical_wikidata_id(ids.get("wikidata") or record.get("wikidata"))
        wikipedia_url = ids.get("wikipedia")
        mag_id = safe_int(ids.get("mag"))

        self._emitter.emit(
            "concept",
            {
                "concept_id": concept_id,
                "concept": record.get("display_name"),
                "description": record.get("description"),
                "level": record.get("level"),
                "openalex_id": canonical_openalex_id(record.get("id")),
                "mag_id": mag_id,
                "wikidata_id": wikidata_id,
                "wikipedia_url": wikipedia_url,
                "image_url": record.get("image_url"),
                "thumbnail_url": record.get("image_thumbnail_url"),
                "updated_date": parse_iso_datetime(record.get("updated_date")),
                "created_date": parse_iso_date(record.get("created_date")),
            },
        )

    # ------------------------------------------------------------------
    def _emit_ancestors(self, concept_id: int, record: Dict[str, object]) -> None:
        ancestors = record.get("ancestors") or []
        seq = 0
        for ancestor in ancestors:
            ancestor_id = numeric_openalex_id(ancestor.get("id")) if isinstance(ancestor, dict) else numeric_openalex_id(ancestor)
            if ancestor_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "concept_ancestor",
                {
                    "concept_id": concept_id,
                    "ancestor_concept_seq": seq,
                    "ancestor_concept_id": ancestor_id,
                },
            )

    # ------------------------------------------------------------------
    def _emit_international(self, concept_id: int, record: Dict[str, object]) -> None:
        international = record.get("international") or {}
        names = international.get("display_name") or {}
        descriptions = international.get("description") or {}

        for language_code, value in sorted(names.items()):
            if not value:
                continue
            self._emitter.emit(
                "concept_international_name",
                {
                    "concept_id": concept_id,
                    "language_code": language_code,
                    "concept_international_name": value,
                },
            )

        for language_code, value in sorted(descriptions.items()):
            if not value:
                continue
            self._emitter.emit(
                "concept_international_description",
                {
                    "concept_id": concept_id,
                    "language_code": language_code,
                    "concept_international_description": value,
                },
            )

    # ------------------------------------------------------------------
    def _emit_related(self, concept_id: int, record: Dict[str, object]) -> None:
        related = record.get("related_concepts") or []
        seq = 0
        for item in related:
            if not isinstance(item, dict):
                continue
            related_id = numeric_openalex_id(item.get("id"))
            if related_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "concept_related",
                {
                    "concept_id": concept_id,
                    "related_concept_seq": seq,
                    "related_concept_id": related_id,
                    "score": item.get("score"),
                },
            )

    # ------------------------------------------------------------------
    def _emit_umls(self, concept_id: int, record: Dict[str, object]) -> None:
        ids = record.get("ids") or {}
        umls_cui = ids.get("umls_cui") or []
        umls_aui = ids.get("umls_aui") or []

        seq = 0
        for value in umls_cui:
            if not value:
                continue
            seq += 1
            self._emitter.emit(
                "concept_umls_cui",
                {
                    "concept_id": concept_id,
                    "umls_cui_seq": seq,
                    "umls_cui": value,
                },
            )

        seq = 0
        for value in umls_aui:
            if not value:
                continue
            seq += 1
            self._emitter.emit(
                "concept_umls_aui",
                {
                    "concept_id": concept_id,
                    "umls_aui_seq": seq,
                    "umls_aui": value,
                },
            )


__all__ = ["ConceptTransformer"]
