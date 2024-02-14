import json
from string import Template
from typing import Any

from neo4j import Driver, GraphDatabase

from mex.backend.fields import (
    FINAL_FIELDS,
    LINK_FIELDS_BY_CLASS_NAME,
    MUTABLE_FIELDS_BY_CLASS_NAME,
    SEARCH_FIELDS,
    SEARCH_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
)
from mex.backend.graph.models import Result
from mex.backend.graph.queries import q
from mex.backend.graph.transform import (
    expand_references_in_search_result,
    transform_model_to_labels_and_parameters,
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

MERGE_NODE_LOG_MSG = "%s (:%s {identifier: %s})"
MERGE_EDGE_LOG_MSG = "%s ({identifier: %s})-[:%s]→({stableTargetId: %s})"
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
        result = self.commit(q.fetch_database_status())
        if (status := result["currentStatus"]) != "online":
            raise MExError(f"Database is {status}.")
        return result

    def _seed_constraints(self) -> list[Result]:
        """Ensure uniqueness constraints are enabled for all entity types."""
        return [
            self.commit(
                q.create_identifier_uniqueness_constraint(node_label=class_name)
            )
            for class_name in sorted(
                set(EXTRACTED_MODEL_CLASSES_BY_NAME) | set(MERGED_MODEL_CLASSES_BY_NAME)
            )
        ]

    def _seed_indices(self) -> Result:
        """Ensure there are full text search indices for all text fields."""
        return self.commit(
            q.create_full_text_search_index(
                node_labels=SEARCH_FIELDS_BY_CLASS_NAME,
                search_fields=SEARCH_FIELDS,
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

    def commit(self, statement: str, **parameters: Any) -> Result:
        """Send and commit a single graph transaction."""
        message = Template(statement).safe_substitute(
            {
                k: json.dumps(v, ensure_ascii=False)
                for k, v in (parameters or {}).items()
            }
        )
        try:
            with self.driver.session(database="neo4j") as session:
                result = Result(session.run(statement, parameters))
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
        query: str | None,
        stable_target_id: str | None,
        entity_type: list[str] | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for nodes.

        Args:
            query: Full text search query term
            stable_target_id: Optional stable target ID filter
            entity_type: Optional entity type filter
            skip: How many nodes to skip for pagination
            limit: How many nodes to return at most

        Returns:
            Graph result instance
        """
        result = self.commit(
            q.fetch_extracted_data(
                query=bool(query),
                stable_target_id=bool(stable_target_id),
                labels=bool(entity_type),
            ),
            query=query,
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
        return self.commit(
            q.fetch_identities(
                had_primary_source=bool(had_primary_source),
                identifier_in_primary_source=bool(identifier_in_primary_source),
                stable_target_id=bool(stable_target_id),
            ),
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
        text_fields = TEXT_FIELDS_BY_CLASS_NAME[model.entityType]
        link_fields = LINK_FIELDS_BY_CLASS_NAME[model.entityType]
        mutable_fields = MUTABLE_FIELDS_BY_CLASS_NAME[model.entityType]
        mutable_node_values = to_primitive(model, include=set(mutable_fields))
        final_node_values = to_primitive(model, include=FINAL_FIELDS)
        all_node_values = {**mutable_node_values, **final_node_values}

        raw_texts = to_primitive(model, include=set(text_fields))
        raw_links = to_primitive(model, include=set(link_fields))

        nested_spec: list[tuple[str, str]] = []
        nested_values: list[dict[str, Any]] = []
        nested_positions: list[int] = []

        for node_label, raws in [
            (Text.__name__, raw_texts),
            (Link.__name__, raw_links),
        ]:
            for edge_label, raw_values in to_key_and_values(raws):
                for position, raw_value in enumerate(raw_values):
                    nested_values.append(raw_value)
                    nested_positions.append(position)
                    nested_spec.append((edge_label, node_label))

        statement = q.merge_node(
            extracted_label=model.entityType,
            merged_label=model.entityType.replace("Extracted", "Merged"),
            nested_spec=nested_spec,
        )

        return self.commit(
            statement,
            identifier=model.identifier,
            stable_target_id=model.stableTargetId,
            on_match=mutable_node_values,
            on_create=all_node_values,
            nested_values=nested_values,
            nested_positions=nested_positions,
        )

    def merge_edges(self, model: AnyExtractedModel) -> list[Result]:
        """Merge edges into the graph for all relations in the given model.

        All fields containing references will be iterated over. When the targeted node
        is found and no such relation exists yet, it will be created.

        Args:
            model: Model to ensure all edges are created in the graph

        Returns:
            Graph result instance
        """
        results = []
        for label, parameters in transform_model_to_labels_and_parameters(model):
            result = self.commit(
                q.merge_edge(edge_label=label),
                **parameters,
            )
            results.append(result)
        return results

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

        # TODO prune edges

        return [m.identifier for m in models]
