import json
from string import Template
from typing import Any

from neo4j import Driver, GraphDatabase

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
from mex.backend.graph.transform import (
    expand_references_in_search_result,
)
from mex.backend.transform import to_primitive
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
    ExtractedPrimarySource,
)
from mex.common.transform import to_key_and_values
from mex.common.types import Identifier, Link, Text

MEX_EXTRACTED_PRIMARY_SOURCE = ExtractedPrimarySource.model_construct(
    hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    identifier=MEX_PRIMARY_SOURCE_IDENTIFIER,
    identifierInPrimarySource=MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    stableTargetId=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
)


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
        # break import cycle, sigh
        from mex.backend.settings import BackendSettings

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
            raise MExError(f"Database is {status}.")
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
        """Ensure there are full text search indices for all text fields."""
        query_builder = QueryBuilder.get()
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
        return self.ingest([MEX_EXTRACTED_PRIMARY_SOURCE])

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
        if counters := result.update_counters:
            logger.info("\n%s\n%s", message, json.dumps(counters, indent=4))
        else:
            logger.info("\n%s", message)
        return result

    def query_nodes(
        self,
        query_string: str | None,
        stable_target_id: str | None,
        entity_type: list[str] | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for nodes.

        Args:
            query_string: Full text search query term
            stable_target_id: Optional stable target ID filter
            entity_type: Optional entity type filter
            skip: How many nodes to skip for pagination
            limit: How many nodes to return at most

        Returns:
            Graph result instance
        """
        query_builder = QueryBuilder.get()
        query = query_builder.fetch_extracted_data(
            query_string=bool(query_string),
            stable_target_id=bool(stable_target_id),
            labels=bool(entity_type),
        )
        result = self.commit(
            query,
            query_string=query_string,
            labels=entity_type,
            stable_target_id=stable_target_id,
            skip=skip,
            limit=limit,
        )
        for item in result["items"]:
            expand_references_in_search_result(item)
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
            had_primary_source=bool(had_primary_source),
            identifier_in_primary_source=bool(identifier_in_primary_source),
            stable_target_id=bool(stable_target_id),
        )
        return self.commit(
            query,
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
            stable_target_id=stable_target_id,
            limit=limit,
        )

    def merge_node(self, model: AnyExtractedModel) -> Result:
        """Convert a model into a node and merge it into the graph.

        Existing nodes will be updated, a new node will be created otherwise.

        Args:
            model: Model to merge into the graph as a node

        Returns:
            Graph result instance
        """
        query_builder = QueryBuilder.get()
        extracted_type = model.entityType
        merged_type = extracted_type.replace("Extracted", "Merged")

        text_fields = set(TEXT_FIELDS_BY_CLASS_NAME[extracted_type])
        link_fields = set(LINK_FIELDS_BY_CLASS_NAME[extracted_type])
        mutable_fields = set(MUTABLE_FIELDS_BY_CLASS_NAME[extracted_type])
        final_fields = set(FINAL_FIELDS_BY_CLASS_NAME[extracted_type])

        mutable_values = to_primitive(model, include=mutable_fields)
        final_values = to_primitive(model, include=final_fields)
        all_values = {**mutable_values, **final_values}

        text_values = to_primitive(model, include=text_fields)
        link_values = to_primitive(model, include=link_fields)

        nested_edge_labels: list[str] = []
        nested_node_labels: list[str] = []
        nested_positions: list[int] = []
        nested_values: list[dict[str, Any]] = []

        for nested_node_label, raws in [
            (Text.__name__, text_values),
            (Link.__name__, link_values),
        ]:
            for nested_edge_label, raw_values in to_key_and_values(raws):
                for position, raw_value in enumerate(raw_values):
                    nested_edge_labels.append(nested_edge_label)
                    nested_node_labels.append(nested_node_label)
                    nested_positions.append(position)
                    nested_values.append(raw_value)

        query = query_builder.merge_node(
            extracted_label=extracted_type,
            merged_label=merged_type,
            nested_edge_labels=nested_edge_labels,
            nested_node_labels=nested_node_labels,
        )

        return self.commit(
            query,
            identifier=model.identifier,
            stable_target_id=model.stableTargetId,
            on_match=mutable_values,
            on_create=all_values,
            nested_values=nested_values,
            nested_positions=nested_positions,
        )

    def merge_edges(self, model: AnyExtractedModel) -> Result:
        """Merge edges into the graph for all relations in the given model.

        All fields containing references will be iterated over. When the targeted node
        is found and no such relation exists yet, it will be created.

        Args:
            model: Model to ensure all edges are created in the graph

        Returns:
            Graph result instance
        """
        query_builder = QueryBuilder.get()
        extracted_type = model.entityType
        ref_fields = REFERENCE_FIELDS_BY_CLASS_NAME[model.entityType]
        ref_values = to_primitive(model, include=set(ref_fields))

        ref_labels: list[str] = []
        ref_identifiers: list[str] = []
        ref_positions: list[int] = []

        for field, identifiers in to_key_and_values(ref_values):
            for position, identifier in enumerate(identifiers):
                ref_identifiers.append(str(identifier))
                ref_positions.append(position)
                ref_labels.append(field)

        query = query_builder.merge_edges(
            extracted_label=extracted_type,
            ref_labels=ref_labels,
        )

        return self.commit(
            query,
            identifier=model.identifier,
            ref_identifiers=ref_identifiers,
            ref_positions=ref_positions,
        )

    def ingest(self, models: list[AnyExtractedModel]) -> list[Identifier]:
        """Ingest a list of models into the graph as nodes and connect all edges.

        Args:
            models: List of extracted items

        Returns:
            List of identifiers from the ingested models
        """
        for model in models:
            self.merge_node(model)

        for model in models:
            self.merge_edges(model)

        return [m.identifier for m in models]
