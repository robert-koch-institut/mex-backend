import json
from string import Template
from typing import Annotated, Any, Literal, cast

from neo4j import Driver, GraphDatabase
from pydantic import Field

from mex.backend.fields import (
    FINAL_FIELDS_BY_CLASS_NAME,
    LINK_FIELDS_BY_CLASS_NAME,
    MUTABLE_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    SEARCHABLE_CLASSES,
    SEARCHABLE_FIELDS,
    TEXT_FIELDS_BY_CLASS_NAME,
)
from mex.backend.graph.models import Result
from mex.backend.graph.query import QueryBuilder
from mex.backend.graph.transform import expand_references_in_search_result
from mex.backend.serialization import to_primitive
from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.exceptions import MExError
from mex.common.logging import logger
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    AnyRuleModel,
    ExtractedPrimarySource,
)
from mex.common.models.primary_source import BasePrimarySource
from mex.common.transform import ensure_prefix, to_key_and_values
from mex.common.types import (
    AnyPrimitiveType,
    ExtractedPrimarySourceIdentifier,
    Identifier,
    Link,
    MergedPrimarySourceIdentifier,
    Text,
)


class MExPrimarySource(BasePrimarySource):
    """An automatically extracted metadata set describing a primary source."""

    entityType: Annotated[
        Literal["ExtractedPrimarySource"], Field(alias="$type", frozen=True)
    ] = "ExtractedPrimarySource"
    hadPrimarySource: MergedPrimarySourceIdentifier = (
        MEX_PRIMARY_SOURCE_STABLE_TARGET_ID
    )
    identifier: ExtractedPrimarySourceIdentifier = MEX_PRIMARY_SOURCE_IDENTIFIER
    identifierInPrimarySource: str = MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE
    stableTargetId: MergedPrimarySourceIdentifier = MEX_PRIMARY_SOURCE_STABLE_TARGET_ID


MEX_EXTRACTED_PRIMARY_SOURCE = MExPrimarySource()


class GraphConnector(BaseConnector):
    """Connector to handle authentication and transactions with the graph database."""

    def __init__(self) -> None:
        """Create a new graph database connection."""
        self.driver = self._init_driver()
        self._check_connectivity_and_authentication()
        self._seed_constraints()
        self._seed_indices()
        self._seed_data()

    def _init_driver(self) -> Driver:
        """Initialize and return a database driver."""
        settings = BackendSettings.get()
        return GraphDatabase.driver(
            settings.graph_url,
            auth=(
                settings.graph_user.get_secret_value(),
                settings.graph_password.get_secret_value(),
            ),
            database=settings.graph_db,
        )

    def _check_connectivity_and_authentication(self) -> Result:
        """Check the connectivity and authentication to the graph."""
        query_builder = QueryBuilder.get()
        result = self.commit(query_builder.fetch_database_status())
        if (status := result["currentStatus"]) != "online":
            raise MExError(f"Database is {status}.") from None
        return result

    def _seed_constraints(self) -> list[Result]:
        """Ensure uniqueness constraints are enabled for all entity types."""
        query_builder = QueryBuilder.get()
        return [
            self.commit(
                query_builder.create_identifier_uniqueness_constraint(
                    node_label=class_name
                )
            )
            for class_name in sorted(
                set(EXTRACTED_MODEL_CLASSES_BY_NAME) | set(MERGED_MODEL_CLASSES_BY_NAME)
            )
        ]

    def _seed_indices(self) -> Result:
        """Ensure there is a full text search index for all searchable fields."""
        query_builder = QueryBuilder.get()
        result = self.commit(query_builder.fetch_full_text_search_index())
        if (index := result.one_or_none()) and (
            set(index["node_labels"]) != set(SEARCHABLE_CLASSES)
            or set(index["search_fields"]) != set(SEARCHABLE_FIELDS)
        ):
            # only drop the index if the classes or fields have changed
            self.commit(query_builder.drop_full_text_search_index())
        return self.commit(
            query_builder.create_full_text_search_index(
                node_labels=SEARCHABLE_CLASSES,
                search_fields=SEARCHABLE_FIELDS,
            ),
            index_config={
                "fulltext.eventually_consistent": True,
                "fulltext.analyzer": "german",
            },
        )

    def _seed_data(self) -> list[Identifier]:
        """Ensure the primary source `mex` is seeded and linked to itself."""
        return self.ingest([cast(ExtractedPrimarySource, MEX_EXTRACTED_PRIMARY_SOURCE)])

    def close(self) -> None:
        """Close the connector's underlying requests session."""
        self.driver.close()

    def commit(self, query: str, **parameters: Any) -> Result:
        """Send and commit a single graph transaction."""
        message = Template(query).safe_substitute(
            {
                k: json.dumps(v, ensure_ascii=False)
                for k, v in (parameters or {}).items()
            }
        )
        try:
            with self.driver.session() as session:
                result = Result(session.run(query, parameters))
        except Exception as error:
            logger.error("\n%s\n%s", message, error)
            raise
        if counters := result.get_update_counters():
            logger.debug("\n%s\n%s", message, json.dumps(counters, indent=4))
        else:
            logger.debug("\n%s", message)
        return result

    def fetch_extracted_items(
        self,
        query_string: str | None,
        stable_target_id: str | None,
        entity_type: list[str] | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for extracted items.

        Args:
            query_string: Optional full text search query term
            stable_target_id: Optional stable target ID filter
            entity_type: Optional entity type filter
            skip: How many items to skip for pagination
            limit: How many items to return at most

        Returns:
            Graph result instance
        """
        query_builder = QueryBuilder.get()
        query = query_builder.fetch_extracted_items(
            filter_by_query_string=bool(query_string),
            filter_by_stable_target_id=bool(stable_target_id),
            filter_by_labels=bool(entity_type),
        )
        result = self.commit(
            query,
            query_string=query_string,
            stable_target_id=stable_target_id,
            labels=entity_type,
            skip=skip,
            limit=limit,
        )
        for query_result in result.all():
            for extracted_item in query_result["items"]:
                expand_references_in_search_result(extracted_item)
        return result

    def fetch_identities(
        self,
        had_primary_source: Identifier | None = None,
        identifier_in_primary_source: str | None = None,
        stable_target_id: Identifier | None = None,
        limit: int = 1000,
    ) -> Result:
        """Search the graph for nodes matching the given ID combination.

        Identity queries can be filtered by `stable_target_id`,
        `had_primary_source` or `identifier_in_primary_source`.

        Args:
            had_primary_source: The stableTargetId of a connected PrimarySource
            identifier_in_primary_source: The id the item had in its primary source
            stable_target_id: The stableTargetId of an item
            limit: How many results to return, defaults to 1000

        Returns:
            A graph result set containing identities
        """
        query_builder = QueryBuilder.get()
        query = query_builder.fetch_identities(
            filter_by_had_primary_source=bool(had_primary_source),
            filter_by_identifier_in_primary_source=bool(identifier_in_primary_source),
            filter_by_stable_target_id=bool(stable_target_id),
        )
        return self.commit(
            query,
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
            stable_target_id=stable_target_id,
            limit=limit,
        )

    def _merge_item(
        self,
        model: AnyExtractedModel | AnyRuleModel,
        stable_target_id: Identifier,
        **constraints: AnyPrimitiveType,
    ) -> Result:
        """Upsert an extracted or rule model including merged item and nested objects.

        The given model is created or updated with all its inline properties.
        All nested properties (like Text or Link) are created as their own nodes
        and linked via edges. For multi-valued fields, the position of each nested
        object is stored as a property on the outbound edge.
        Any nested objects that are found in the graph, but are not present on the
        model any more are purged.
        In addition, a merged item is created (if it does not exist yet) and the
        extracted item is linked to it via an edge with the label `stableTargetId`.

        Args:
            model: Model to merge into the graph as a node
            stable_target_id: Identifier the connected merged item should have
            constraints: Mapping of field names and values to use as constraints
                         when finding potential items to update

        Returns:
            Graph result instance
        """
        query_builder = QueryBuilder.get()

        text_fields = set(TEXT_FIELDS_BY_CLASS_NAME[model.entityType])
        link_fields = set(LINK_FIELDS_BY_CLASS_NAME[model.entityType])
        mutable_fields = set(MUTABLE_FIELDS_BY_CLASS_NAME[model.entityType])
        final_fields = set(FINAL_FIELDS_BY_CLASS_NAME[model.entityType])

        mutable_values = to_primitive(model, include=mutable_fields)
        final_values = to_primitive(model, include=final_fields)
        all_values = {**mutable_values, **final_values}

        text_values = to_primitive(model, include=text_fields)
        link_values = to_primitive(model, include=link_fields)

        nested_edge_labels: list[str] = []
        nested_node_labels: list[str] = []
        nested_positions: list[int] = []
        nested_values: list[dict[str, AnyPrimitiveType]] = []

        for nested_type, raws in [(Text, text_values), (Link, link_values)]:
            for nested_edge_label, raw_values in to_key_and_values(raws):
                for position, raw_value in enumerate(raw_values):
                    nested_edge_labels.append(nested_edge_label)
                    nested_node_labels.append(nested_type.__name__)
                    nested_positions.append(position)
                    nested_values.append(raw_value)

        query = query_builder.merge_item(
            current_label=model.entityType,
            current_constraints=sorted(constraints),
            merged_label=ensure_prefix(model.stemType, "Merged"),
            nested_edge_labels=nested_edge_labels,
            nested_node_labels=nested_node_labels,
        )

        return self.commit(
            query,
            **constraints,
            stable_target_id=stable_target_id,
            on_match=mutable_values,
            on_create=all_values,
            nested_values=nested_values,
            nested_positions=nested_positions,
        )

    def _merge_edges(
        self,
        model: AnyExtractedModel | AnyRuleModel,
        stable_target_id: Identifier,
        extra_refs: dict[str, Any] | None = None,
        **constraints: AnyPrimitiveType,
    ) -> Result:
        """Merge edges into the graph for all relations originating from one model.

        All fields containing references will be iterated over. When the referenced node
        is found and no such relation exists yet, it will be created.
        A position attribute is added to all edges, that stores the index the reference
        had in list of references on the originating model. This way, we can preserve
        the order for example of `contact` persons referenced on an activity.

        Args:
            model: Model to ensure all edges are created in the graph
            stable_target_id: Identifier of the connected merged item
            extra_refs: Optional extra references to inject into the merge
            constraints: Mapping of field names and values to use as constraints
                         when finding the current item

        Returns:
            Graph result instance
        """
        query_builder = QueryBuilder.get()

        ref_fields = REFERENCE_FIELDS_BY_CLASS_NAME[model.entityType]
        ref_values = to_primitive(model, include=set(ref_fields))
        ref_values.update(extra_refs or {})

        ref_labels: list[str] = []
        ref_identifiers: list[str] = []
        ref_positions: list[int] = []

        for field, identifiers in to_key_and_values(ref_values):
            for position, identifier in enumerate(identifiers):
                ref_identifiers.append(identifier)
                ref_positions.append(position)
                ref_labels.append(field)

        query = query_builder.merge_edges(
            current_label=model.entityType,
            current_constraints=sorted(constraints),
            merged_label=ensure_prefix(model.stemType, "Merged"),
            ref_labels=ref_labels,
        )

        return self.commit(
            query,
            **constraints,
            stable_target_id=stable_target_id,
            ref_identifiers=ref_identifiers,
            ref_positions=ref_positions,
        )

    def create_rule(self, model: AnyRuleModel) -> AnyRuleModel:
        """Create a new rule to be a applied to one merged item.

        This is a two-step process: first the rule and merged items are created
        along with their nested objects (like Text and Link); then all edges that
        represent references (like hadPrimarySource, parentUnit, etc.) are added to
        the graph in a second step.

        Args:
            model: A single rule model

        Returns:
            The created rule model
        """
        stable_target_id = Identifier.generate()
        self._merge_item(model, stable_target_id)
        self._merge_edges(
            model,
            stable_target_id,
            extra_refs=dict(
                hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                stableTargetId=stable_target_id,
            ),
        )
        # TODO: read the rule back from the database instead of returning the
        #       input; to ensure consistency (MX-1416)
        return model

    def ingest(self, models: list[AnyExtractedModel]) -> list[Identifier]:
        """Ingest a list of models into the graph as nodes and connect all edges.

        This is a two-step process: first all extracted and merged items are created
        along with their nested objects (like Text and Link); then all edges that
        represent references (like hadPrimarySource, parentUnit, etc.) are added to
        the graph in a second step.

        Args:
            models: List of extracted models

        Returns:
            List of identifiers of the ingested models
        """
        for model in models:
            self._merge_item(model, model.stableTargetId, identifier=model.identifier)

        for model in models:
            self._merge_edges(model, model.stableTargetId, identifier=model.identifier)

        return [m.identifier for m in models]
