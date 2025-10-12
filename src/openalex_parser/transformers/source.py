"""Transformer for source entities."""
from __future__ import annotations

import re
from typing import Dict

from ..emitter import TableEmitter
from ..reference import EnumerationRegistry
from ..identifiers import StableIdGenerator
from ..utils import (
    bool_from_flag,
    canonical_openalex_id,
    canonical_wikidata_id,
    numeric_openalex_id,
    parse_iso_date,
    parse_iso_datetime,
    safe_int,
)

ISSN_PATTERN = re.compile(r"\d{4}-\d{3}[\dX]")


class SourceTransformer:
    """Map OpenAlex source JSON documents to relational rows."""

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
        source_id = numeric_openalex_id(record.get("id"))
        if source_id is None:
            return
        self._emit_source(source_id, record)
        self._emit_alternative_titles(source_id, record)
        self._emit_apc_prices(source_id, record)
        self._emit_issn(source_id, record)
        self._emit_societies(source_id, record)

    # ------------------------------------------------------------------
    def _emit_source(self, source_id: int, record: Dict[str, object]) -> None:
        ids = record.get("ids") or {}
        wikidata_id = canonical_wikidata_id(ids.get("wikidata"))
        fatcat_url = ids.get("fatcat")
        fatcat_id = None
        if fatcat_url:
            fatcat_id = fatcat_url.rstrip("/").split("/")[-1]
        mag_id = safe_int(ids.get("mag"))
        issn_l = self._normalize_issn(record.get("issn_l") or ids.get("issn_l"))

        source_type_id = self._enums.id_for("source_type", record.get("type"))

        host_publisher_id = None
        host_institution_id = None
        host_org = canonical_openalex_id(record.get("host_organization"))
        if host_org:
            if host_org.startswith("P"):
                host_publisher_id = numeric_openalex_id(host_org)
            elif host_org.startswith("I"):
                host_institution_id = numeric_openalex_id(host_org)
        if host_publisher_id is None:
            publisher_id = canonical_openalex_id(record.get("publisher_id"))
            if publisher_id and publisher_id.startswith("P"):
                host_publisher_id = numeric_openalex_id(publisher_id)

        self._emitter.emit(
            "source",
            {
                "source_id": source_id,
                "source": record.get("display_name"),
                "abbreviation": record.get("abbreviated_title"),
                "source_type_id": source_type_id,
                "country_iso_alpha2_code": record.get("country_code"),
                "host_organization_publisher_id": host_publisher_id,
                "host_organization_institution_id": host_institution_id,
                "homepage_url": record.get("homepage_url"),
                "issn_l": issn_l,
                "openalex_id": canonical_openalex_id(record.get("id")),
                "mag_id": mag_id,
                "wikidata_id": wikidata_id,
                "fatcat_id": fatcat_id,
                "is_in_doaj": bool_from_flag(record.get("is_in_doaj")),
                "is_oa": bool_from_flag(record.get("is_oa")),
                "apc_price_usd": safe_int(record.get("apc_usd")),
                "updated_date": parse_iso_datetime(record.get("updated_date")),
                "created_date": parse_iso_date(record.get("created_date")),
            },
        )

    # ------------------------------------------------------------------
    def _emit_alternative_titles(self, source_id: int, record: Dict[str, object]) -> None:
        titles = record.get("alternate_titles") or []
        seen = set()
        seq = 0
        for title in titles:
            if not title or title in seen:
                continue
            seen.add(title)
            seq += 1
            self._emitter.emit(
                "source_alternative_title",
                {
                    "source_id": source_id,
                    "alternative_title_seq": seq,
                    "alternative_title": title,
                },
            )

    # ------------------------------------------------------------------
    def _emit_apc_prices(self, source_id: int, record: Dict[str, object]) -> None:
        prices = record.get("apc_prices") or []
        seq = 0
        for price in prices:
            if not isinstance(price, dict):
                continue
            value = price.get("price")
            currency = price.get("currency")
            if value is None or not currency:
                continue
            seq += 1
            self._emitter.emit(
                "source_apc_price",
                {
                    "source_id": source_id,
                    "apc_price_seq": seq,
                    "apc_price": value,
                    "currency": currency,
                },
            )

    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_issn(raw_value: object) -> str | None:
        if raw_value is None:
            return None
        text = str(raw_value).strip()
        if not text:
            return None
        if ":" in text:
            text = text.rsplit(":", 1)[-1].strip()
        text = text.upper()
        match = ISSN_PATTERN.search(text)
        if match:
            return match.group(0)
        if len(text) >= 9:
            candidate = text[-9:]
            if ISSN_PATTERN.fullmatch(candidate):
                return candidate
        return None

    # ------------------------------------------------------------------
    def _emit_issn(self, source_id: int, record: Dict[str, object]) -> None:
        issns = record.get("issn") or (record.get("ids") or {}).get("issn") or []
        seq = 0
        seen = set()
        for issn in issns:
            normalized = self._normalize_issn(issn)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            seq += 1
            self._emitter.emit(
                "source_issn",
                {
                    "source_id": source_id,
                    "issn_seq": seq,
                    "issn": normalized,
                },
            )

    # ------------------------------------------------------------------
    def _emit_societies(self, source_id: int, record: Dict[str, object]) -> None:
        societies = record.get("societies") or []
        seq = 0
        for society in societies:
            if not isinstance(society, dict):
                continue
            name = society.get("organization") or society.get("name")
            url = society.get("url")
            if not name and not url:
                continue
            seq += 1
            self._emitter.emit(
                "source_society",
                {
                    "source_id": source_id,
                    "society_seq": seq,
                    "society": name,
                    "homepage_url": url,
                },
            )


__all__ = ["SourceTransformer"]
