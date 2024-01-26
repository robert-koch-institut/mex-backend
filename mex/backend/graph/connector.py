import json
import logging
from typing import Any

from neo4j import GraphDatabase

from mex.backend.fields import (
    LINK_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
)
from mex.backend.graph import queries as q
from mex.backend.graph.models import GraphResult
from mex.backend.graph.transform import transform_model_to_edges
from mex.backend.transform import to_primitive
from mex.common.connector import BaseConnector
from mex.common.exceptions import MExError
from mex.common.logging import logger
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    ExtractedPrimarySource,
)
from mex.common.types import Identifier, Link, Text, TextLanguage

MERGE_NODE_LOG_MSG = "%s (:%s {identifier: %s})"
MERGE_EDGE_LOG_MSG = "%s ({identifier: %s})-[:%s]→({stableTargetId: %s})"


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
        self.commit(q.noop())

    def close(self) -> None:
        """Close the connector's underlying requests session."""
        self.driver.close()

    def _seed_constraints(self) -> list[GraphResult]:
        """Ensure uniqueness constraints are enabled for all entity types."""
        constraint_statements = [
            (q.identifier_uniqueness_constraint(entity_type), None)
            for entity_type in EXTRACTED_MODEL_CLASSES_BY_NAME
        ]
        return self.mcommit(*constraint_statements)

    def _seed_indices(self) -> GraphResult:
        """Ensure there are full text search indices for all text fields."""
        return self.commit(
            q.full_text_search_index(TEXT_FIELDS_BY_CLASS_NAME),
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
            title=[Text(value="Metadata Exchange", language=TextLanguage.EN)],
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
    ) -> GraphResult:
        """Query the graph for nodes.

        Args:
            query: Full text search query term
            stable_target_id: Optional stable target ID filter
            entity_type: Optional entity type filter
            skip: How many nodes to skip for pagination
            limit: How many nodes to return at most

        Returns:
            Graph result instances
        """
        if query and stable_target_id:
            raise NotImplementedError(
                "full text query and stableTargetId cannot be combined"
            )

        extracted_labels = "|".join(EXTRACTED_MODEL_CLASSES_BY_NAME)
        nested_labels = "|".join([Link.__name__, Text.__name__])
        if not entity_type:
            entity_type = list(EXTRACTED_MODEL_CLASSES_BY_NAME)

        if query:
            match = r"""
  CALL db.index.fulltext.queryNodes('search_index', $query)
  YIELD node AS hit, score
  MATCH (n)
  WHERE elementId(hit) = elementId(n)
    AND ANY(label IN labels(n) WHERE label IN $labels)
""".strip()
        elif stable_target_id:
            match = r"""
  MATCH (n)
  WHERE n.stableTargetId = $stable_target_id
""".strip()
        else:
            match = r"""
  MATCH (n)
  WHERE ANY(label IN labels(n) WHERE label IN $labels)
""".strip()
        statement = """
CALL {{
  {match}
  RETURN COUNT(n) AS total
}}
CALL {{
  MATCH (n)
  WHERE ANY(label IN labels(n) WHERE label IN $labels)
  CALL {{
    WITH n
    MATCH (n)-[r]->(t:{extracted_labels})
    RETURN type(r) as label, r.position as position, t.stableTargetId as value
  UNION
    WITH n
    MATCH (n)-[r]->(t:{nested_labels})
    RETURN type(r) as label, r.position as position, properties(t) as value
  }}
  WITH n, collect({{label: label, position: position, value: value}}) as refs
  RETURN n{{.*, _label: head(labels(n)), _refs: refs}}
  ORDER BY n.identifier ASC
  SKIP $skip
  LIMIT $limit
}}
RETURN collect(n) AS items, total;
""".format(
            match=match, extracted_labels=extracted_labels, nested_labels=nested_labels
        )

        return self.commit(
            statement.strip(),
            query=query,
            labels=entity_type,
            stable_target_id=stable_target_id,
            skip=skip,
            limit=limit,
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
            statement = q.stable_target_id_identity()
        elif (
            had_primary_source and identifier_in_primary_source and not stable_target_id
        ):
            statement = q.had_primary_source_and_identifier_in_primary_source_identity()
        else:
            raise MExError("invalid identity query parameters")

        return self.commit(
            statement,
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
        ref_fields = REFERENCE_FIELDS_BY_CLASS_NAME[model.entityType]
        text_fields = TEXT_FIELDS_BY_CLASS_NAME[model.entityType]
        link_fields = LINK_FIELDS_BY_CLASS_NAME[model.entityType]
        immutable_fields = {"identifierInPrimarySource", "identifier"}
        static_node_values = to_primitive(
            model,
            exclude={
                *immutable_fields,
                *ref_fields,
                *text_fields,
                *link_fields,
                "entityType",
            },
        )
        immutable_node_values = to_primitive(model, include=immutable_fields)
        s = []

        s.append(
            "MERGE (node:{label} {{identifier: $identifier}})".format(
                label=model.entityType
            )
        )
        s.append("ON CREATE SET node = $on_create")
        s.append("ON MATCH SET node += $on_match")

        raw_texts = to_primitive(model, include=text_fields)
        raw_links = to_primitive(model, include=link_fields)

        values: list[dict[str, str]] = []

        for node_label, raws in [("Text", raw_texts), ("Link", raw_links)]:
            for edge_label, raw_values in raws.items():
                if not isinstance(raw_values, list):
                    raw_values = [raw_values]
                for pos, raw_value in enumerate(raw_values):
                    s.append(
                        "MERGE (node)-[e{0}:{1} {{position: {2}}}]->(v{0}:{3})".format(
                            len(values), edge_label, pos, node_label
                        )
                    )
                    s.append("ON CREATE SET v{0} = $values[{0}]".format(len(values)))
                    s.append("ON MATCH SET v{0} += $values[{0}]".format(len(values)))
                    values.append(raw_value)

        s.append(
            "WITH node, [{edges}] as edges, [{values}] as values".format(
                edges=", ".join(f"e{i}" for i in range(len(values))),
                values=", ".join(f"v{i}" for i in range(len(values))),
            )
        )
        s.append(
            """CALL {{
    WITH values
    MATCH (:{label} {{identifier: $identifier}})-[]->(gc:Text|Link)
    WHERE NOT gc IN values
    DETACH DELETE gc
    RETURN gc
}}""".format(
                label=model.entityType
            )
        )

        s.append("RETURN node, edges, values, gc;")

        return self.commit(
            "\n".join(s),
            identifier=model.identifier,
            on_match=static_node_values,
            on_create={**static_node_values, **immutable_node_values},
            values=values,
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
            (q.merge_edge(edge.label), edge.model_dump()) for edge in edges
        ]
        results = self.mcommit(*edge_statements_with_parameters)

        for result, edge in zip(results, edges):
            level = logging.INFO if result.data else logging.ERROR
            verb = "MERGED" if result.data else "FAILED"
            args = verb, edge.fromIdentifier, edge.label, edge.toStableTargetId
            logger.log(level, MERGE_EDGE_LOG_MSG, *args)

        return results

    def ingest(self, models: list[AnyExtractedModel]) -> list[Identifier]:
        """Ingest a list of models into the graph as nodes and connect all edges.

        Args:
            models: List of extracted items

        Returns:
            List of identifiers from the ingested models
        """
        for model in models:
            # TODO batch this
            self.merge_node(model)

        for model in models:
            # TODO batch this
            self.merge_edges(model)

        # TODO prune edges

        return [m.identifier for m in models]
