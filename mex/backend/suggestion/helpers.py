import time
from typing import TYPE_CHECKING, Any

from neo4j.exceptions import Neo4jError
from pydantic import BaseModel

from mex.backend.cache import get_cache_connector
from mex.backend.graph.connector import GraphConnector
from mex.backend.models import ReferenceFilter
from mex.backend.settings import BackendSettings
from mex.backend.suggestion.constants import (
    AGGREGATION_TOP_K,
    CORPUS_PRIOR_CACHE_PREFIX,
    NEIGHBOURHOOD_HIT_LIMIT,
    NEIGHBOURHOOD_SIZE,
    SUGGESTION_QUERY_TIMEOUT,
)
from mex.backend.suggestion.models import SuggestionResponse
from mex.backend.suggestion.scoring import score_field
from mex.backend.suggestion.transform import (
    FieldAggregation,
    Neighbour,
    escape_query_string,
    transform_aggregations,
    transform_neighbours,
)
from mex.backend.types import MergedType, ReferenceFieldName, VocabularyFieldName
from mex.common.fields import (
    REFERENCE_FIELDS_BY_CLASS_NAME,
    STRING_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
    VOCABULARIES_BY_FIELDS_BY_CLASS_NAMES,
    VOCABULARY_FIELDS_BY_CLASS_NAME,
)
from mex.common.logging import logger
from mex.common.transform import ensure_prefix

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mex.common.models import AnyAdditiveModel


class CachedAggregations(BaseModel):
    """A corpus wide vocabulary distribution, with the time it was computed.

    The cache connectors have no expiry of their own, so freshness is carried in
    the payload and checked on read. The timestamp is unix epoch seconds rather
    than a datetime, because the cache serializes with `MExEncoder`, which has
    no datetime support.
    """

    createdAt: float
    cohortSize: int
    aggregations: list[dict[str, Any]]


def _iter_field_values(additive: AnyAdditiveModel, field_names: list[str]) -> list[Any]:
    """Flatten the populated values of the given fields into a single list."""
    values: list[Any] = []
    for field_name in field_names:
        value = getattr(additive, field_name, None)
        if value is None:
            continue
        if isinstance(value, list):
            values.extend(value)
        else:
            values.append(value)
    return values


def derive_query_string(additive: AnyAdditiveModel) -> str:
    """Assemble an escaped lucene query string from a draft item's free text.

    Both text and plain string fields are used, so the caller does not have to
    know which shape a given field has. The result is escaped, because a draft
    is by definition half typed and lucene would otherwise refuse to parse it.
    """
    class_name = additive.entityType
    raw_values = _iter_field_values(
        additive,
        [
            *TEXT_FIELDS_BY_CLASS_NAME[class_name],
            *STRING_FIELDS_BY_CLASS_NAME[class_name],
        ],
    )
    return escape_query_string(
        " ".join(str(getattr(value, "value", value)) for value in raw_values if value)
    )


def derive_reference_filters(additive: AnyAdditiveModel) -> list[ReferenceFilter]:
    """Build reference filters from the references populated on a draft item.

    Only fields that are both populated and known to be reference fields are
    used, so a draft that has not yet picked a unit simply contributes no
    filter rather than an empty one.
    """
    populated = {
        field_name: identifiers
        for field_name in REFERENCE_FIELDS_BY_CLASS_NAME[additive.entityType]
        if (identifiers := _iter_field_values(additive, [field_name]))
    }
    return [
        ReferenceFilter(
            field=ReferenceFieldName(field_name),
            identifiers=[str(identifier) for identifier in identifiers],
        )
        for field_name, identifiers in populated.items()
    ]


def derive_field_names(
    additive: AnyAdditiveModel,
    requested: Sequence[VocabularyFieldName],
) -> list[str]:
    """Pick the vocabulary fields to suggest for a draft item.

    Defaults to the fields the draft has not filled in yet: there is no point
    ranking a field the user has already answered, and leaving them out keeps
    the response small. An explicit request wins, but is still restricted to
    fields that exist on this entity type.
    """
    available = VOCABULARY_FIELDS_BY_CLASS_NAME[additive.entityType]
    if requested:
        wanted = {str(field.value) for field in requested}
        return [name for name in available if name in wanted]
    return [name for name in available if not _iter_field_values(additive, [name])]


def get_corpus_prior(
    entity_type: str,
    vocabulary_fields: Sequence[str],
) -> tuple[int, dict[str, FieldAggregation]]:
    """Fetch the unconditioned distribution for an entity type, via the cache.

    This is the one query that scales with the size of the graph, and it only
    changes on ingest, so it is cached. It is always computed over the full
    field list for the entity type regardless of what this request asked for,
    so that every request shares one cache entry.
    """
    settings = BackendSettings.get()
    cache = get_cache_connector()
    key = f"{CORPUS_PRIOR_CACHE_PREFIX}:{entity_type}"
    if cached := cache.get_value(key):
        corpus = CachedAggregations.model_validate(cached)
        age = time.time() - corpus.createdAt
        if age < settings.suggestion_corpus_prior_ttl:
            return corpus.cohortSize, transform_aggregations(corpus.aggregations)

    connector = GraphConnector.get()
    record = connector.aggregate_vocabulary_values(
        entity_type=[entity_type],
        vocabulary_fields=vocabulary_fields,
        reference_filters=None,
        exclude_identifier=None,
        top_k=AGGREGATION_TOP_K,
        timeout=SUGGESTION_QUERY_TIMEOUT,
    ).one()
    cache.set_value(
        key,
        CachedAggregations(
            createdAt=time.time(),
            cohortSize=int(record["cohortSize"]),
            aggregations=record["aggregations"],
        ),
    )
    return int(record["cohortSize"]), transform_aggregations(record["aggregations"])


def suggest_vocabulary_values_from_graph(
    additive: AnyAdditiveModel,
    requested_fields: Sequence[VocabularyFieldName],
    exclude_identifier: str | None,
    limit: int,
    min_confidence: float,
) -> SuggestionResponse:
    """Suggest likely vocabulary values for a partially filled draft item.

    Args:
        additive: The draft item, with whatever the user has entered so far
        requested_fields: Vocabulary fields to rank, or empty for the unfilled ones
        exclude_identifier: Merged item to leave out of every conditioned signal
        limit: How many candidates to return per field
        min_confidence: Drop candidates scoring below this

    Returns:
        Per field ranked candidates with confidences and their evidence
    """
    entity_type = ensure_prefix(additive.stemType, "Merged")
    field_names = derive_field_names(additive, requested_fields)
    if not field_names:
        return SuggestionResponse(
            entityType=MergedType(entity_type),
            cohortSize=0,
            neighbourhoodSize=0,
            suggestions=[],
        )

    all_fields = VOCABULARY_FIELDS_BY_CLASS_NAME[additive.entityType]
    corpus_size, corpus = get_corpus_prior(entity_type, all_fields)

    connector = GraphConnector.get()
    cohort_size = 0
    cohort: dict[str, FieldAggregation] = {}
    if reference_filters := derive_reference_filters(additive):
        try:
            record = connector.aggregate_vocabulary_values(
                entity_type=[entity_type],
                vocabulary_fields=field_names,
                reference_filters=reference_filters,
                exclude_identifier=exclude_identifier,
                top_k=AGGREGATION_TOP_K,
                timeout=SUGGESTION_QUERY_TIMEOUT,
            ).one()
        except Neo4jError:
            # a degraded answer from the remaining signals beats a 500 in a
            # form the user is actively typing into
            logger.warning("suggestion cohort query failed", exc_info=True)
        else:
            cohort_size = int(record["cohortSize"])
            cohort = transform_aggregations(record["aggregations"])

    neighbours: list[Neighbour] = []
    if query_string := derive_query_string(additive):
        try:
            record = connector.fetch_vocabulary_neighbourhood(
                query_string=query_string,
                entity_type=[entity_type],
                vocabulary_fields=field_names,
                exclude_identifier=exclude_identifier,
                hit_limit=NEIGHBOURHOOD_HIT_LIMIT,
                neighbour_limit=NEIGHBOURHOOD_SIZE,
                timeout=SUGGESTION_QUERY_TIMEOUT,
            ).one()
        except Neo4jError:
            logger.warning("suggestion neighbourhood query failed", exc_info=True)
        else:
            neighbours = transform_neighbours(record["neighbours"])

    vocabularies = VOCABULARIES_BY_FIELDS_BY_CLASS_NAMES[entity_type]
    suggestions = [
        score_field(
            field_name=VocabularyFieldName(field_name),
            allowed_values=frozenset(
                str(concept.value) for concept in vocabularies[field_name]
            ),
            cohort=cohort.get(field_name),
            corpus=corpus.get(field_name),
            neighbours=neighbours,
            limit=limit,
            min_confidence=min_confidence,
        )
        for field_name in field_names
    ]
    return SuggestionResponse(
        entityType=MergedType(entity_type),
        cohortSize=cohort_size or corpus_size,
        neighbourhoodSize=len(neighbours),
        suggestions=suggestions,
    )
