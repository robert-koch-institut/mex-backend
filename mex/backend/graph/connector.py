import json
import logging
from string import Template
from typing import Any

from neo4j import GraphDatabase

from mex.backend.fields import (
    LINK_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    SEARCH_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
)
from mex.backend.graph import queries as q
from mex.backend.graph.models import Result
from mex.backend.graph.transform import (
    transform_model_to_edges,
    transform_search_result_to_model,
)
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
from mex.common.types import Identifier

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
        self._seed_data()

    def _check_connectivity_and_authentication(self) -> Result:
        """Check the connectivity and authentication to the graph."""
        return self.commit(q.noop())

    def _seed_constraints(self) -> list[Result]:
        """Ensure uniqueness constraints are enabled for all entity types."""
        return [
            self.commit(q.identifier_uniqueness_constraint(entity_type))
            for entity_type in EXTRACTED_MODEL_CLASSES_BY_NAME
        ]

    def _seed_indices(self) -> Result:
        """Ensure there are full text search indices for all text fields."""
        return self.commit(
            q.full_text_search_index(**SEARCH_FIELDS_BY_CLASS_NAME),
            config={
                "fulltext.eventually_consistent": True,
                "fulltext.analyzer": "german",
            },
        )

    def _seed_data(self) -> list[Identifier]:
        """Ensure the primary source `mex` is seeded and linked to itself."""
        return self.ingest(
            [
                ExtractedPrimarySource.model_construct(
                    hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                    identifier=MEX_PRIMARY_SOURCE_IDENTIFIER,
                    identifierInPrimarySource=MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
                    stableTargetId=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                )
            ]
        )

    def close(self) -> None:
        """Close the connector's underlying requests session."""
        self.driver.close()

    def commit(self, statement: str, **parameters: Any) -> Result:
        """Send and commit a single graph transaction."""
        log_message = ("\033[95m\n%s%s\033[0m",)
        log_statement = Template(statement).safe_substitute(
            {k: json.dumps(v) for k, v in (parameters or {}).items()}
        )
        try:
            with self.driver.session(database="neo4j") as session:
                result = Result(session.run(statement, parameters))
        except Exception as error:
            logging.error(log_message, log_statement, f"\n{error}")
            raise
        if counters := result.update_counters:
            logging.info(log_message, log_statement, f"\n{json.dumps(counters)}")
        else:
            logging.info(log_message, log_statement, "")
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
        if query and stable_target_id:
            raise NotImplementedError(
                "full text query and stableTargetId cannot be combined"
            )
        if not entity_type:
            entity_type = list(EXTRACTED_MODEL_CLASSES_BY_NAME)

        if query:
            statement = q.extracted_data_query(q.full_text_match_clause())
        elif stable_target_id:
            statement = q.extracted_data_query(q.stable_target_id_match_clause())
        else:
            statement = q.extracted_data_query(q.entity_type_match_clause())

        result = self.commit(
            statement,
            query=query,
            labels=entity_type,
            stable_target_id=stable_target_id,
            skip=skip,
            limit=limit,
        )
        for item in result["items"]:
            transform_search_result_to_model(item)
        return result

    def fetch_identities(
        self,
        had_primary_source: Identifier | None = None,
        identifier_in_primary_source: str | None = None,
        stable_target_id: Identifier | None = None,
        limit: int = 1000,
    ) -> Result:
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

    def merge_node(self, model: AnyExtractedModel) -> Result:
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
            "MERGE (merged:{label} {{identifier: $stable_target_id}})".format(
                label=model.entityType.replace("Extracted", "Merged")
            )
        )
        s.append(
            "MERGE (extracted:{label} {{identifier: $identifier}})-[stableTargetId:stableTargetId {{position: 0}}]->(merged)".format(
                label=model.entityType
            )
        )
        s.append("ON CREATE SET extracted = $on_create")
        s.append("ON MATCH SET extracted += $on_match")

        raw_texts = to_primitive(model, include=set(text_fields))
        raw_links = to_primitive(model, include=set(link_fields))

        values: list[dict[str, str]] = []

        for node_label, raws in [("Text", raw_texts), ("Link", raw_links)]:
            for edge_label, raw_values in raws.items():
                if not isinstance(raw_values, list):
                    raw_values = [raw_values]
                for pos, raw_value in enumerate(raw_values):
                    s.append(
                        "MERGE (extracted)-[e{0}:{1} {{position: {2}}}]->(v{0}:{3})".format(
                            len(values), edge_label, pos, node_label
                        )
                    )
                    s.append("ON CREATE SET v{0} = $values[{0}]".format(len(values)))
                    s.append("ON MATCH SET v{0} += $values[{0}]".format(len(values)))
                    values.append(raw_value)

        s.append(
            "WITH extracted, [{edges}] as edges, [{values}] as values".format(
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
    RETURN count(gc) as pruned
}}""".format(
                label=model.entityType
            )
        )

        s.append("RETURN extracted, edges, values, pruned;")

        return self.commit(
            "\n".join(s),
            identifier=model.identifier,
            stable_target_id=model.stableTargetId,
            on_match=static_node_values,
            on_create={**static_node_values, **immutable_node_values},
            values=values,
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
        for edge in transform_model_to_edges(model):
            result = self.commit(q.merge_edge(edge.label), **edge.model_dump())
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
            # TODO batch this
            self.merge_node(model)

        for model in models:
            # TODO batch this
            self.merge_edges(model)

        # TODO prune edges

        return [m.identifier for m in models]
