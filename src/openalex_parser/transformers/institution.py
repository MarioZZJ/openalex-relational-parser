"""Transformer for institution entities."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from ..emitter import TableEmitter
from ..identifiers import StableIdGenerator
from ..reference import EnumerationRegistry
from ..utils import (
    bool_from_flag,
    canonical_openalex_id,
    canonical_wikidata_id,
    numeric_openalex_id,
    parse_iso_date,
    parse_iso_datetime,
    safe_float,
    safe_int,
)


class InstitutionTransformer:
    """Map OpenAlex institution JSON documents to relational rows."""

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
        institution_id = numeric_openalex_id(record.get("id"))
        if institution_id is None:
            return
        self._emit_institution(institution_id, record)
        self._emit_acronyms(institution_id, record)
        self._emit_alternative_names(institution_id, record)
        self._emit_international_names(institution_id, record)
        self._emit_associated(institution_id, record)
        self._emit_roles(institution_id, record)
        self._emit_repositories(institution_id, record)
        self._emit_lineage(institution_id, record)

    # ------------------------------------------------------------------
    def _emit_institution(self, institution_id: int, record: Dict[str, object]) -> None:
        geo = record.get("geo") or {}
        region_id = None
        region_name = geo.get("region")
        if region_name:
            try:
                region_id = self._enums.id_for("region", region_name)
            except KeyError:
                region_id = None

        city_id = safe_int(geo.get("geonames_city_id"))
        city_name = geo.get("city")
        if city_id and city_name:
            self._emitter.emit(
                "city",
                {
                    "geonames_city_id": city_id,
                    "city": city_name,
                },
            )

        country_code = record.get("country_code")
        country_name = geo.get("country")
        if country_code:
            self._emitter.emit(
                "country",
                {
                    "country_iso_alpha2_code": country_code,
                    "country": country_name,
                },
            )

        ror = record.get("ror")
        if ror:
            ror = ror.rstrip("/").split("/")[-1]

        ids = record.get("ids") or {}
        wikipedia_url = ids.get("wikipedia")
        wikidata_id = canonical_wikidata_id(ids.get("wikidata"))
        mag_id = safe_int(ids.get("mag"))

        institution_type_id = self._enums.id_for("institution_type", record.get("type"))

        self._emitter.emit(
            "institution",
            {
                "institution_id": institution_id,
                "institution": record.get("display_name"),
                "institution_type_id": institution_type_id,
                "country_iso_alpha2_code": country_code,
                "region_id": region_id,
                "geonames_city_id": city_id,
                "latitude": safe_float(geo.get("latitude")),
                "longitude": safe_float(geo.get("longitude")),
                "homepage_url": record.get("homepage_url"),
                "is_super_system": bool_from_flag(record.get("is_super_system")),
                "ror_id": ror,
                "grid_id": ids.get("grid"),
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
    def _emit_acronyms(self, institution_id: int, record: Dict[str, object]) -> None:
        acronyms = record.get("display_name_acronyms") or []
        seen = set()
        seq = 0
        for acronym in acronyms:
            if not acronym or acronym in seen:
                continue
            seen.add(acronym)
            seq += 1
            self._emitter.emit(
                "institution_acronym",
                {
                    "institution_id": institution_id,
                    "acronym_seq": seq,
                    "acronym": acronym,
                },
            )

    # ------------------------------------------------------------------
    def _emit_alternative_names(self, institution_id: int, record: Dict[str, object]) -> None:
        names = record.get("display_name_alternatives") or []
        seen = set()
        seq = 0
        for name in names:
            if not name or name in seen:
                continue
            seen.add(name)
            seq += 1
            self._emitter.emit(
                "institution_alternative_name",
                {
                    "institution_id": institution_id,
                    "alternative_name_seq": seq,
                    "alternative_name": name,
                },
            )

    # ------------------------------------------------------------------
    def _emit_international_names(self, institution_id: int, record: Dict[str, object]) -> None:
        international = record.get("international") or {}
        names = international.get("display_name") or {}
        for language_code, name in sorted(names.items()):
            if not name:
                continue
            self._emitter.emit(
                "institution_international_name",
                {
                    "institution_id": institution_id,
                    "language_code": language_code,
                    "institution_international_name": name,
                },
            )

    # ------------------------------------------------------------------
    def _emit_associated(self, institution_id: int, record: Dict[str, object]) -> None:
        associated = record.get("associated_institutions") or []
        seq = 0
        for item in associated:
            if not isinstance(item, dict):
                continue
            associated_id = numeric_openalex_id(item.get("id"))
            if associated_id is None:
                continue
            relationship = item.get("relationship")
            relationship_type_id = None
            if relationship:
                relationship_type_id = self._enums.id_for("institution_relationship_type", relationship)
            seq += 1
            self._emitter.emit(
                "institution_associated",
                {
                    "institution_id": institution_id,
                    "associated_institution_seq": seq,
                    "associated_institution_id": associated_id,
                    "institution_relationship_type_id": relationship_type_id,
                },
            )

    # ------------------------------------------------------------------
    def _emit_roles(self, institution_id: int, record: Dict[str, object]) -> None:
        roles = record.get("roles") or []
        funder_seq = 0
        publisher_seq = 0
        for role in roles:
            if not isinstance(role, dict):
                continue
            role_type = role.get("role")
            role_id = numeric_openalex_id(role.get("id"))
            if role_type == "funder" and role_id is not None:
                funder_seq += 1
                self._emitter.emit(
                    "institution_funder",
                    {
                        "institution_id": institution_id,
                        "funder_seq": funder_seq,
                        "funder_id": role_id,
                    },
                )
            elif role_type == "publisher" and role_id is not None:
                publisher_seq += 1
                self._emitter.emit(
                    "institution_publisher",
                    {
                        "institution_id": institution_id,
                        "publisher_seq": publisher_seq,
                        "publisher_id": role_id,
                    },
                )

    # ------------------------------------------------------------------
    def _emit_repositories(self, institution_id: int, record: Dict[str, object]) -> None:
        repositories = record.get("repositories") or []
        seq = 0
        for repo in repositories:
            if not isinstance(repo, dict):
                continue
            source_id = numeric_openalex_id(repo.get("id"))
            if source_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "institution_repository",
                {
                    "institution_id": institution_id,
                    "repository_seq": seq,
                    "repository_source_id": source_id,
                },
            )

    # ------------------------------------------------------------------
    def _emit_lineage(self, institution_id: int, record: Dict[str, object]) -> None:
        lineage = record.get("lineage") or []
        seq = 0
        for value in lineage:
            lineage_id = numeric_openalex_id(value)
            if lineage_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "institution_lineage",
                {
                    "institution_id": institution_id,
                    "lineage_institution_seq": seq,
                    "lineage_institution_id": lineage_id,
                },
            )


__all__ = ["InstitutionTransformer"]
