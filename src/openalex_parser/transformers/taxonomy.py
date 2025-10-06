"""Transformers for OpenAlex taxonomy entities."""
from __future__ import annotations

from typing import Dict, List

from ..emitter import TableEmitter
from ..utils import (
    canonical_wikidata_id,
    numeric_openalex_id,
    parse_iso_date,
    parse_iso_datetime,
)


class DomainTransformer:
    def __init__(self, emitter: TableEmitter) -> None:
        self._emitter = emitter

    def transform(self, record: Dict[str, object]) -> None:
        domain_id = numeric_openalex_id(record.get("id"))
        if domain_id is None:
            return
        ids = record.get("ids") or {}
        wikidata_id = canonical_wikidata_id(ids.get("wikidata"))
        wikipedia_url = ids.get("wikipedia")
        self._emitter.emit(
            "domain",
            {
                "domain_id": domain_id,
                "domain": record.get("display_name"),
                "description": record.get("description"),
                "openalex_id": domain_id,
                "wikidata_id": wikidata_id,
                "wikipedia_url": wikipedia_url,
                "updated_date": parse_iso_datetime(record.get("updated_date")),
                "created_date": parse_iso_date(record.get("created_date")),
            },
        )
        self._emit_alternative_names(domain_id, record)
        self._emit_fields(domain_id, record)
        self._emit_siblings(domain_id, record)

    def _emit_alternative_names(self, domain_id: int, record: Dict[str, object]) -> None:
        names = record.get("display_name_alternatives") or []
        seq = 0
        seen = set()
        for name in names:
            if not name or name in seen:
                continue
            seen.add(name)
            seq += 1
            self._emitter.emit(
                "domain_alternative_name",
                {
                    "domain_id": domain_id,
                    "alternative_name_seq": seq,
                    "alternative_name": name,
                },
            )

    def _emit_fields(self, domain_id: int, record: Dict[str, object]) -> None:
        fields = record.get("fields") or []
        seq = 0
        for field in fields:
            field_id = numeric_openalex_id(field.get("id")) if isinstance(field, dict) else numeric_openalex_id(field)
            if field_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "domain_field",
                {
                    "domain_id": domain_id,
                    "field_seq": seq,
                    "field_id": field_id,
                },
            )

    def _emit_siblings(self, domain_id: int, record: Dict[str, object]) -> None:
        siblings = record.get("siblings") or []
        seq = 0
        for sibling in siblings:
            sibling_id = numeric_openalex_id(sibling.get("id")) if isinstance(sibling, dict) else numeric_openalex_id(sibling)
            if sibling_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "domain_sibling",
                {
                    "domain_id": domain_id,
                    "sibling_domain_seq": seq,
                    "sibling_domain_id": sibling_id,
                },
            )


class FieldTransformer:
    def __init__(self, emitter: TableEmitter) -> None:
        self._emitter = emitter

    def transform(self, record: Dict[str, object]) -> None:
        field_id = numeric_openalex_id(record.get("id"))
        if field_id is None:
            return
        ids = record.get("ids") or {}
        domain = record.get("domain") or {}
        domain_id = numeric_openalex_id(domain.get("id"))
        wikidata_id = canonical_wikidata_id(ids.get("wikidata"))
        wikipedia_url = ids.get("wikipedia")

        self._emitter.emit(
            "field",
            {
                "field_id": field_id,
                "field": record.get("display_name"),
                "description": record.get("description"),
                "openalex_id": field_id,
                "wikidata_id": wikidata_id,
                "wikipedia_url": wikipedia_url,
                "domain_id": domain_id,
                "updated_date": parse_iso_datetime(record.get("updated_date")),
                "created_date": parse_iso_date(record.get("created_date")),
            },
        )
        self._emit_alternative_names(field_id, record)
        self._emit_subfields(field_id, record)
        self._emit_siblings(field_id, record)

    def _emit_alternative_names(self, field_id: int, record: Dict[str, object]) -> None:
        names = record.get("display_name_alternatives") or []
        seen = set()
        seq = 0
        for name in names:
            if not name or name in seen:
                continue
            seen.add(name)
            seq += 1
            self._emitter.emit(
                "field_alternative_name",
                {
                    "field_id": field_id,
                    "alternative_name_seq": seq,
                    "alternative_name": name,
                },
            )

    def _emit_subfields(self, field_id: int, record: Dict[str, object]) -> None:
        subfields = record.get("subfields") or []
        seq = 0
        for subfield in subfields:
            subfield_id = numeric_openalex_id(subfield.get("id")) if isinstance(subfield, dict) else numeric_openalex_id(subfield)
            if subfield_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "field_subfield",
                {
                    "field_id": field_id,
                    "subfield_seq": seq,
                    "subfield_id": subfield_id,
                },
            )

    def _emit_siblings(self, field_id: int, record: Dict[str, object]) -> None:
        siblings = record.get("siblings") or []
        seq = 0
        for sibling in siblings:
            sibling_id = numeric_openalex_id(sibling.get("id")) if isinstance(sibling, dict) else numeric_openalex_id(sibling)
            if sibling_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "field_sibling",
                {
                    "field_id": field_id,
                    "sibling_field_seq": seq,
                    "sibling_field_id": sibling_id,
                },
            )


class SubfieldTransformer:
    def __init__(self, emitter: TableEmitter) -> None:
        self._emitter = emitter

    def transform(self, record: Dict[str, object]) -> None:
        subfield_id = numeric_openalex_id(record.get("id"))
        if subfield_id is None:
            return
        ids = record.get("ids") or {}
        domain = record.get("domain") or {}
        field = record.get("field") or {}
        domain_id = numeric_openalex_id(domain.get("id"))
        field_id = numeric_openalex_id(field.get("id"))
        wikidata_id = canonical_wikidata_id(ids.get("wikidata"))
        wikipedia_url = ids.get("wikipedia")

        self._emitter.emit(
            "subfield",
            {
                "subfield_id": subfield_id,
                "subfield": record.get("display_name"),
                "description": record.get("description"),
                "openalex_id": subfield_id,
                "wikidata_id": wikidata_id,
                "wikipedia_url": wikipedia_url,
                "domain_id": domain_id,
                "field_id": field_id,
                "updated_date": parse_iso_datetime(record.get("updated_date")),
                "created_date": parse_iso_date(record.get("created_date")),
            },
        )
        self._emit_alternative_names(subfield_id, record)
        self._emit_topics(subfield_id, record)
        self._emit_siblings(subfield_id, record)

    def _emit_alternative_names(self, subfield_id: int, record: Dict[str, object]) -> None:
        names = record.get("display_name_alternatives") or []
        seen = set()
        seq = 0
        for name in names:
            if not name or name in seen:
                continue
            seen.add(name)
            seq += 1
            self._emitter.emit(
                "subfield_alternative_name",
                {
                    "subfield_id": subfield_id,
                    "alternative_name_seq": seq,
                    "alternative_name": name,
                },
            )

    def _emit_topics(self, subfield_id: int, record: Dict[str, object]) -> None:
        topics = record.get("topics") or []
        seq = 0
        for topic in topics:
            topic_id = numeric_openalex_id(topic.get("id")) if isinstance(topic, dict) else numeric_openalex_id(topic)
            if topic_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "subfield_topic",
                {
                    "subfield_id": subfield_id,
                    "topic_seq": seq,
                    "topic_id": topic_id,
                },
            )

    def _emit_siblings(self, subfield_id: int, record: Dict[str, object]) -> None:
        siblings = record.get("siblings") or []
        seq = 0
        for sibling in siblings:
            sibling_id = numeric_openalex_id(sibling.get("id")) if isinstance(sibling, dict) else numeric_openalex_id(sibling)
            if sibling_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "subfield_sibling",
                {
                    "subfield_id": subfield_id,
                    "sibling_subfield_seq": seq,
                    "sibling_subfield_id": sibling_id,
                },
            )


class TopicTransformer:
    def __init__(self, emitter: TableEmitter) -> None:
        self._emitter = emitter

    def transform(self, record: Dict[str, object]) -> None:
        topic_id = numeric_openalex_id(record.get("id"))
        if topic_id is None:
            return
        domain = record.get("domain") or {}
        field = record.get("field") or {}
        subfield = record.get("subfield") or {}
        domain_id = numeric_openalex_id(domain.get("id"))
        field_id = numeric_openalex_id(field.get("id"))
        subfield_id = numeric_openalex_id(subfield.get("id"))

        self._emitter.emit(
            "topic",
            {
                "topic_id": topic_id,
                "topic": record.get("display_name"),
                "description": record.get("description"),
                "openalex_id": topic_id,
                "wikipedia_url": (record.get("ids") or {}).get("wikipedia"),
                "domain_id": domain_id,
                "field_id": field_id,
                "subfield_id": subfield_id,
                "updated_date": parse_iso_datetime(record.get("updated_date")),
                "created_date": parse_iso_date(record.get("created_date")),
            },
        )
        self._emit_keywords(topic_id, record)
        self._emit_siblings(topic_id, record)

    def _emit_keywords(self, topic_id: int, record: Dict[str, object]) -> None:
        keywords = record.get("keywords") or []
        seq = 0
        for keyword in keywords:
            if not keyword:
                continue
            seq += 1
            self._emitter.emit(
                "topic_keyword",
                {
                    "topic_id": topic_id,
                    "keyword_seq": seq,
                    "keyword": keyword,
                },
            )

    def _emit_siblings(self, topic_id: int, record: Dict[str, object]) -> None:
        siblings = record.get("siblings") or []
        seq = 0
        for sibling in siblings:
            sibling_id = numeric_openalex_id(sibling.get("id")) if isinstance(sibling, dict) else numeric_openalex_id(sibling)
            if sibling_id is None:
                continue
            seq += 1
            self._emitter.emit(
                "topic_sibling",
                {
                    "topic_id": topic_id,
                    "sibling_topic_seq": seq,
                    "sibling_topic_id": sibling_id,
                },
            )


__all__ = [
    "DomainTransformer",
    "FieldTransformer",
    "SubfieldTransformer",
    "TopicTransformer",
]
