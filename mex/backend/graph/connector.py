import json
from typing import Any

from neo4j import GraphDatabase

from mex.backend.extracted.models import (
    AnyExtractedModel,
)
from mex.backend.fields import TEXT_FIELDS_BY_CLASS_NAME
from mex.backend.graph.models import GraphResult
from mex.backend.graph.queries import (
    CREATE_CONSTRAINTS_STATEMENT,
    CREATE_INDEX_STATEMENT,
    HAD_PRIMARY_SOURCE_AND_IDENTIFIER_IN_PRIMARY_SOURCE_IDENTITY_QUERY,
    MERGE_EDGE_STATEMENT,
    MERGE_NODE_STATEMENT,
    QUERY_MAP,
    STABLE_TARGET_ID_IDENTITY_QUERY,
)
from mex.backend.graph.transform import (
    transform_model_to_edges,
    transform_model_to_node,
)
from mex.common.connector import BaseConnector
from mex.common.exceptions import MExError
from mex.common.logging import logger
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedPrimarySource,
)
from mex.common.types import Identifier


class GraphConnector(BaseConnector):
    """Connector to handle authentication and transactions with the graph database."""

    def __init__(self) -> None:
        """Create a new graph database connection."""
        # break import cycle, sigh
        from mex.backend.settings import BackendSettings

        settings = BackendSettings.get()
        self.driver = GraphDatabase.driver(
            settings.graph_url,
            auth=(
                settings.graph_user.get_secret_value(),
                settings.graph_password.get_secret_value(),
            ),
            database=settings.graph_db,
        )
        self._check_connectivity_and_authentication()
        self._seed_constraints()
        self._seed_indices()
        self._seed_primary_source()

    def _check_connectivity_and_authentication(self) -> None:
        """Check the connectivity and authentication to the graph."""
        self.commit("RETURN 1;")

    def close(self) -> None:
        """Close the connector's underlying requests session."""
        self.driver.close()

    def _seed_constraints(self) -> list[GraphResult]:
        """Ensure uniqueness constraints are enabled for all entity types."""
        constraint_statements = [
            (CREATE_CONSTRAINTS_STATEMENT.format(node_label=entity_type), None)
            for entity_type in EXTRACTED_MODEL_CLASSES_BY_NAME
        ]
        return self.mcommit(*constraint_statements)

    def _seed_indices(self) -> GraphResult:
        """Ensure there are full text search indices for all text fields."""
        node_labels = "|".join(TEXT_FIELDS_BY_CLASS_NAME.keys())
        node_fields = ", ".join(
            sorted(
                {
                    f"n.{f}"
                    for fields in TEXT_FIELDS_BY_CLASS_NAME.values()
                    for f in fields
                }
            )
        )
        return self.commit(
            CREATE_INDEX_STATEMENT.format(
                node_labels=node_labels, node_fields=node_fields
            ),
            config={
                "fulltext.eventually_consistent": True,
                "fulltext.analyzer": "german",
            },
        )

    def _seed_primary_source(self) -> Identifier:
        """Ensure the primary source `mex` is seeded and linked to itself."""
        mex_primary_source = ExtractedPrimarySource.model_construct(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifier=MEX_PRIMARY_SOURCE_IDENTIFIER,
            identifierInPrimarySource=MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
            stableTargetId=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        )
        return self.ingest([mex_primary_source])[0]

    def mcommit(
        self, *statements_with_parameters: tuple[str, dict[str, Any] | None]
    ) -> list[GraphResult]:
        """Send and commit a batch of graph transactions."""
        with self.driver.session(database="neo4j") as session:
            results = []
            logger.info(
                "\033[95m\n%s\n\033[0m",
                json.dumps(
                    {
                        "statements": [
                            {
                                "statement": statement,
                                **({"parameters": parameters} if parameters else {}),
                            }
                            for statement, parameters in statements_with_parameters
                        ]
                    },
                    indent=2,
                ),
            )
            for statement, parameters in statements_with_parameters:
                result = session.run(statement, parameters)
                results.append(GraphResult(data=result.data()))
        return results

    def commit(self, statement: str, **parameters: Any) -> GraphResult:
        """Send and commit a single graph transaction."""
        results = self.mcommit((statement, parameters))
        return results[0]

    def query_nodes(
        self,
        query: str | None,
        stable_target_id: str | None,
        entity_type: list[str] | None,
        skip: int,
        limit: int,
    ) -> list[GraphResult]:
        """Query the graph for nodes.

        Args:
            query: Fulltext search query term
            stable_target_id: Optional stable target ID filter
            entity_type: Optional entity type filter
            skip: How many nodes to skip for pagination
            limit: How many nodes to return at most

        Returns:
            Graph result instances
        """
        search_statement, count_statement = QUERY_MAP[
            (bool(query), bool(stable_target_id), bool(entity_type))
        ]
        return self.mcommit(
            (
                search_statement,
                dict(
                    query=query,
                    labels=entity_type,
                    stable_target_id=stable_target_id,
                    skip=skip,
                    limit=limit,
                ),
            ),
            (
                count_statement,
                dict(
                    query=query,
                    labels=entity_type,
                    stable_target_id=stable_target_id,
                ),
            ),
        )

    def fetch_identities(
        self,
        had_primary_source: Identifier | None = None,
        identifier_in_primary_source: str | None = None,
        stable_target_id: Identifier | None = None,
        limit: int = 1000,
    ) -> GraphResult:
        """Search the graph for nodes matching the given ID combination.

        Identity queries can be filtered either with just a `stable_target_id`
        or with both `had_primary_source` and identifier_in_primary_source`.

        Args:
            had_primary_source: The stableTargetId of a connected PrimarySource
            identifier_in_primary_source: The id the item had in its primary source
            stable_target_id: The stableTargetId of an item
            limit: How many results to return, defaults to 1000

        Raises:
            MExError: When a wrong combination of IDs is given

        Returns:
            A graph result set containing identities
        """
        if (
            not had_primary_source
            and not identifier_in_primary_source
            and stable_target_id
        ):
            query = STABLE_TARGET_ID_IDENTITY_QUERY
        elif (
            had_primary_source and identifier_in_primary_source and not stable_target_id
        ):
            query = HAD_PRIMARY_SOURCE_AND_IDENTIFIER_IN_PRIMARY_SOURCE_IDENTITY_QUERY
        else:
            raise MExError("invalid identity query parameters")

        return self.commit(
            query,
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
            stable_target_id=stable_target_id,
            limit=limit,
        )

    def merge_node(self, model: AnyExtractedModel) -> GraphResult:
        """Convert a model into a node and merge it into the graph.

        Existing nodes will be updated, a new node will be created otherwise.

        Args:
            model: Model to merge into the graph as a node

        Returns:
            Graph result instance
        """
        entity_type = model.__class__.__name__
        logger.info(
            "MERGE (:%s {identifier:%s}) ",
            entity_type,
            model.identifier,
        )
        node = transform_model_to_node(model)
        return self.commit(
            MERGE_NODE_STATEMENT.format(node_label=entity_type),
            identifier=model.identifier,
            **node.model_dump(),
        )

    def merge_edges(self, model: AnyExtractedModel) -> list[GraphResult]:
        """Merge edges into the graph for all relations in the given model.

        All fields containing references will be iterated over. When the targeted node
        is found and no such relation exists yet, it will be created.

        Args:
            model: Model to ensure all edges are created in the graph

        Returns:
            Graph result instances
        """
        edges = transform_model_to_edges(model)
        edge_statements_with_parameters = [
            (MERGE_EDGE_STATEMENT.format(edge_label=edge.label), edge.parameters)
            for edge in edges
        ]
        results = self.mcommit(*edge_statements_with_parameters)
        for result, edge in zip(results, edges):
            if result.data:
                logger.info(f"MERGED {edge.log_message}")
            else:
                logger.error(f"FAILED {edge.log_message}")
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

        return [m.identifier for m in models]
