"""Entity-specific transformers for mapping OpenAlex JSON objects into table rows."""

from .author import AuthorTransformer
from .concept import ConceptTransformer
from .funder import FunderTransformer
from .institution import InstitutionTransformer
from .publisher import PublisherTransformer
from .source import SourceTransformer
from .taxonomy import DomainTransformer, FieldTransformer, SubfieldTransformer, TopicTransformer
from .work import WorkTransformer

__all__ = [
    "AuthorTransformer",
    "ConceptTransformer",
    "DomainTransformer",
    "FieldTransformer",
    "FunderTransformer",
    "InstitutionTransformer",
    "PublisherTransformer",
    "SourceTransformer",
    "SubfieldTransformer",
    "TopicTransformer",
    "WorkTransformer",
]
