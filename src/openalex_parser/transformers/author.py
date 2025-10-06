"""Transformer for author entities."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from ..emitter import TableEmitter
from ..identifiers import StableIdGenerator
from ..reference import EnumerationRegistry
from ..utils import (
    canonical_openalex_id,
    canonical_orcid,
    extract_scopus_author_id,
    numeric_openalex_id,
    parse_iso_datetime,
    parse_iso_date,
    safe_int,
)


class AuthorTransformer:
    """Map OpenAlex author JSON documents to relational rows."""

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
        author_id = numeric_openalex_id(record.get("id"))
        if author_id is None:
            return
        self._emit_author(author_id, record)
        self._emit_alternative_names(author_id, record)
        self._emit_affiliations(author_id, record)
        self._emit_last_known_institutions(author_id, record)

    # ------------------------------------------------------------------
    def _emit_author(self, author_id: int, record: Dict[str, object]) -> None:
        ids = record.get("ids") or {}
        orcid = canonical_orcid(record.get("orcid") or ids.get("orcid"))
        scopus_id = extract_scopus_author_id(ids.get("scopus"))
        wikipedia_url = ids.get("wikipedia")

        self._emitter.emit(
            "author",
            {
                "author_id": author_id,
                "author": record.get("display_name"),
                "orcid": orcid,
                "openalex_id": canonical_openalex_id(record.get("id")),
                "scopus_id": scopus_id,
                "wikipedia_url": wikipedia_url,
                "updated_date": parse_iso_datetime(record.get("updated_date")),
                "created_date": parse_iso_date(record.get("created_date")),
            },
        )

    # ------------------------------------------------------------------
    def _emit_alternative_names(self, author_id: int, record: Dict[str, object]) -> None:
        alternatives = record.get("display_name_alternatives") or []
        seen = set()
        for idx, name in enumerate(alternatives, start=1):
            if not name or name in seen:
                continue
            seen.add(name)
            self._emitter.emit(
                "author_alternative_name",
                {
                    "author_id": author_id,
                    "alternative_name_seq": idx,
                    "alternative_name": name,
                },
            )

    # ------------------------------------------------------------------
    def _emit_affiliations(self, author_id: int, record: Dict[str, object]) -> None:
        affiliations: List[Dict[str, object]] = record.get("affiliations") or []
        inst_seq = 0
        for affiliation in affiliations:
            institution = affiliation.get("institution") if isinstance(affiliation, dict) else None
            institution_id = numeric_openalex_id(institution.get("id")) if institution else None
            if institution_id is None:
                continue
            inst_seq += 1
            self._emitter.emit(
                "author_institution",
                {
                    "author_id": author_id,
                    "institution_seq": inst_seq,
                    "institution_id": institution_id,
                },
            )
            years = affiliation.get("years") if isinstance(affiliation, dict) else None
            if years:
                year_seq = 0
                for year_value in years:
                    year_int = safe_int(year_value)
                    if year_int is None:
                        continue
                    year_seq += 1
                    self._emitter.emit(
                        "author_institution_year",
                        {
                            "author_id": author_id,
                            "institution_seq": inst_seq,
                            "year_seq": year_seq,
                            "year": year_int,
                        },
                    )

    # ------------------------------------------------------------------
    def _emit_last_known_institutions(self, author_id: int, record: Dict[str, object]) -> None:
        institutions: List[Dict[str, object]] = []
        last_known = record.get("last_known_institution")
        if isinstance(last_known, dict):
            institutions.append(last_known)
        last_known_list = record.get("last_known_institutions")
        if isinstance(last_known_list, list):
            institutions.extend(inst for inst in last_known_list if isinstance(inst, dict))

        seen_ids: set[int] = set()
        seq = 0
        for institution in institutions:
            institution_id = numeric_openalex_id(institution.get("id"))
            if institution_id is None or institution_id in seen_ids:
                continue
            seen_ids.add(institution_id)
            seq += 1
            self._emitter.emit(
                "author_last_known_institution",
                {
                    "author_id": author_id,
                    "last_known_institution_seq": seq,
                    "last_known_institution_id": institution_id,
                },
            )


__all__ = ["AuthorTransformer"]
