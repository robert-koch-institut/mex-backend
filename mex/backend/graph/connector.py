from collections import deque
from collections.abc import Generator, Iterable, Sequence
from typing import Any

from neo4j import (
    READ_ACCESS,
    WRITE_ACCESS,
    Driver,
    GraphDatabase,
    NotificationDisabledCategory,
    Transaction,
)
from neo4j.exceptions import Neo4jError

from mex.backend.fields import (
    ALL_REFERENCE_FIELD_NAMES,
    SEARCHABLE_CLASSES,
    SEARCHABLE_FIELDS,
)
from mex.backend.graph.exceptions import (
    InconsistentGraphError,
    IngestionError,
    MatchingError,
)
from mex.backend.graph.models import IngestData, MExPrimarySource, Result
from mex.backend.graph.query import Query, QueryBuilder
from mex.backend.graph.transform import (
    expand_references_in_search_result,
    get_error_details_from_neo4j_error,
    transform_edges_into_expectations_by_edge_locator,
    transform_model_into_ingest_data,
    validate_ingested_data,
)
from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.exceptions import MExError
from mex.common.fields import (
    ALL_MODEL_CLASSES_BY_NAME,
    FINAL_FIELDS_BY_CLASS_NAME,
    LINK_FIELDS_BY_CLASS_NAME,
    MUTABLE_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
)
from mex.common.logging import logger
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
    AnyExtractedModel,
    AnyMergedModel,
    AnyRuleModel,
    AnyRuleSetResponse,
)
from mex.common.transform import ensure_prefix, to_key_and_values
from mex.common.types import AnyPrimitiveType, Identifier, Link, Text


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
            notifications_disabled_categories=[
                # mute warnings about labels used in queries but missing in graph
                NotificationDisabledCategory.UNRECOGNIZED,
            ],
            telemetry_disabled=True,
            max_connection_pool_size=settings.backend_api_parallelization,
            max_transaction_retry_time=settings.graph_session_timeout,
        )

    def _check_connectivity_and_authentication(self) -> Result:
        """Check the connectivity and authentication to the graph."""
        query_builder = QueryBuilder.get()
        result = self.commit(query_builder.fetch_database_status())
        if (status := result["currentStatus"]) != "online":
            msg = f"Database is {status}."
            raise MExError(msg) from None
        return result

    def _seed_constraints(self) -> None:
        """Ensure property constraints are created for all entity types."""
        query_builder = QueryBuilder.get()
        for label in EXTRACTED_MODEL_CLASSES_BY_NAME | MERGED_MODEL_CLASSES_BY_NAME:
            self.commit(
                query_builder.create_identifier_constraint(node_label=label),
                access_mode=WRITE_ACCESS,
            )
        logger.info("seeded identifier constraints")
        for label in EXTRACTED_MODEL_CLASSES_BY_NAME:
            self.commit(
                query_builder.create_provenance_constraint(node_label=label),
                access_mode=WRITE_ACCESS,
            )
        logger.info("seeded provenance constraints")

    def _seed_indices(self) -> Result:
        """Ensure there is a full text search index for all searchable fields."""
        query_builder = QueryBuilder.get()
        result = self.commit(
            query_builder.fetch_full_text_search_index(),
            access_mode=WRITE_ACCESS,
        )
        if (index := result.one_or_none()) and (
            set(index["node_labels"]) != set(SEARCHABLE_CLASSES)
            or set(index["search_fields"]) != set(SEARCHABLE_FIELDS)
        ):
            # only drop the index if the classes or fields have changed
            self.commit(
                query_builder.drop_full_text_search_index(),
                access_mode=WRITE_ACCESS,
            )
            logger.info("searchable fields changed: dropped indices")
        result = self.commit(
            query_builder.create_full_text_search_index(
                node_labels=SEARCHABLE_CLASSES,
                search_fields=SEARCHABLE_FIELDS,
            ),
            access_mode=WRITE_ACCESS,
            index_config={
                "fulltext.eventually_consistent": True,
                "fulltext.analyzer": "german",
            },
        )
        logger.info("created full text search index")
        return result

    def _seed_data(self) -> None:
        """Ensure the primary source `mex` is seeded and linked to itself."""
        deque(self.ingest_extracted_items([MExPrimarySource()]))
        logger.info("seeded mex primary source")

    def close(self) -> None:
        """Close the connector's underlying requests session."""
        self.driver.close()

    def commit(
        self,
        query: Query | str,
        /,
        access_mode: str = READ_ACCESS,
        **parameters: Any,  # noqa: ANN401
    ) -> Result:
        """Send and commit a single graph transaction with retry configuration.

        Args:
            query: The query string or Query object to execute
            access_mode: Whether to run the query with read or write access
            **parameters: Query parameters to substitute in the Cypher query

        Returns:
            Result object containing query execution results and metadata
        """
        with self.driver.session(default_access_mode=access_mode) as session:
            return Result(session.run(str(query), parameters))

    def _fetch_extracted_or_rule_items(  # noqa: PLR0913
        self,
        query_string: str | None,
        identifier: str | None,
        stable_target_id: str | None,
        entity_type: Sequence[str],
        referenced_identifiers: Sequence[str] | None,
        reference_field: str | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for extracted or rule items.

        Args:
            query_string: Optional full text search query term
            identifier: Optional identifier filter
            stable_target_id: Optional stable target ID filter
            entity_type: List of allowed entity types
            referenced_identifiers: Optional merged item identifiers filter
            reference_field: Optional field name to filter for
            skip: How many items to skip for pagination
            limit: How many items to return at most

        Returns:
            Graph result instance
        """
        query_builder = QueryBuilder.get()
        query = query_builder.fetch_extracted_or_rule_items(
            filter_by_query_string=bool(query_string),
            filter_by_identifier=bool(identifier),
            filter_by_stable_target_id=bool(stable_target_id),
            filter_by_referenced_identifiers=bool(referenced_identifiers),
            reference_field=reference_field,
        )
        result = self.commit(
            query,
            query_string=query_string,
            identifier=identifier,
            stable_target_id=stable_target_id,
            labels=entity_type,
            referenced_identifiers=referenced_identifiers,
            skip=skip,
            limit=limit,
        )
        for query_result in result.all():
            for item in query_result["items"]:
                item.update(expand_references_in_search_result(item.pop("_refs")))
        return result

    def fetch_extracted_items(  # noqa: PLR0913
        self,
        query_string: str | None,
        identifier: str | None,
        stable_target_id: str | None,
        entity_type: Sequence[str] | None,
        referenced_identifiers: Sequence[str] | None,
        reference_field: str | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for extracted items.

        Args:
            query_string: Optional full text search query term
            identifier: Optional identifier filter
            stable_target_id: Optional stable target ID filter
            entity_type: Optional entity type filter
            referenced_identifiers: Optional merged item identifiers filter
            reference_field: Optional field name to filter for
            skip: How many items to skip for pagination
            limit: How many items to return at most

        Returns:
            Graph result instance
        """
        return self._fetch_extracted_or_rule_items(
            query_string=query_string,
            identifier=identifier,
            stable_target_id=stable_target_id,
            entity_type=entity_type or list(EXTRACTED_MODEL_CLASSES_BY_NAME),
            referenced_identifiers=referenced_identifiers,
            reference_field=reference_field,
            skip=skip,
            limit=limit,
        )

    def fetch_rule_items(  # noqa: PLR0913
        self,
        query_string: str | None,
        identifier: str | None,
        stable_target_id: str | None,
        entity_type: Sequence[str] | None,
        referenced_identifiers: Sequence[str] | None,
        reference_field: str | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for rule items.

        Args:
            query_string: Optional full text search query term
            identifier: Optional identifier filter
            stable_target_id: Optional stable target ID filter
            entity_type: Optional entity type filter
            referenced_identifiers: Optional merged item identifiers filter
            reference_field: Optional field name to filter for
            skip: How many items to skip for pagination
            limit: How many items to return at most

        Returns:
            Graph result instance
        """
        return self._fetch_extracted_or_rule_items(
            query_string=query_string,
            identifier=identifier,
            stable_target_id=stable_target_id,
            entity_type=entity_type or list(RULE_MODEL_CLASSES_BY_NAME),
            referenced_identifiers=referenced_identifiers,
            reference_field=reference_field,
            skip=skip,
            limit=limit,
        )

    def fetch_merged_items(  # noqa: PLR0913
        self,
        query_string: str | None,
        identifier: str | None,
        entity_type: Sequence[str] | None,
        referenced_identifiers: Sequence[str] | None,
        reference_field: str | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for merged items.

        Args:
            query_string: Optional full text search query term
            identifier: Optional merged item identifier filter
            entity_type: Optional merged entity type filter
            referenced_identifiers: Optional merged item identifiers filter
            reference_field: Optional field name to filter for
            skip: How many items to skip for pagination
            limit: How many items to return at most

        Returns:
            Graph result instance
        """
        if reference_field and reference_field not in ALL_REFERENCE_FIELD_NAMES:
            msg = "Invalid field name."
            raise ValueError(msg)
        query_builder = QueryBuilder.get()
        query = query_builder.fetch_merged_items(
            filter_by_query_string=bool(query_string),
            filter_by_identifier=bool(identifier),
            filter_by_referenced_identifiers=bool(referenced_identifiers),
            reference_field=reference_field,
        )
        result = self.commit(
            query,
            query_string=query_string,
            identifier=identifier,
            labels=entity_type or list(MERGED_MODEL_CLASSES_BY_NAME),
            referenced_identifiers=referenced_identifiers,
            skip=skip,
            limit=limit,
        )
        for query_result in result.all():
            for item in query_result["items"]:
                for component in item["_components"]:
                    refs = component.pop("_refs")
                    component.update(expand_references_in_search_result(refs))
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

    def exists_item(
        self,
        identifier: Identifier,
        entity_type: str,
    ) -> bool:
        """Validate whether an item with the given identifier and entity type exists.

        Args:
            identifier: Identifier of the to-be-checked item
            entity_type: Entity type of the to-be-checked item

        Returns:
            Boolean representing the existence of the requested item
        """
        if entity_type not in ALL_MODEL_CLASSES_BY_NAME:
            return False
        query_builder = QueryBuilder.get()
        query = query_builder.exists_item(
            node_labels=[entity_type],
        )
        result = self.commit(
            query,
            identifier=identifier,
        )
        return bool(result["exists"])

    def _ingest_extracted_item_tx(self, tx: Transaction, data_in: IngestData) -> None:
        """Ingest a single item in a database transaction."""
        query_builder = QueryBuilder.get()
        query = query_builder.get_ingest_query_for_entity_type(data_in.entityType)
        try:
            tx_result = tx.run(query, data=data_in.model_dump())
            result = Result(tx_result)
            result.log_notifications()
            data_out = IngestData.model_validate(result.one())
            error_details = validate_ingested_data(data_in, data_out)
            if error_details:
                msg = (
                    f"Could not merge {data_in.entityType}"
                    f"(stableTargetId='{data_in.stableTargetId}', ...)"
                )
                raise IngestionError(msg, errors=error_details, retryable=False)
        except Neo4jError as error:
            tx.rollback()
            msg = (
                f"{type(error).__name__} caused by {data_in.entityType}"
                f"(stableTargetId='{data_in.stableTargetId}', ...)"
            )
            raise IngestionError(
                msg,
                errors=get_error_details_from_neo4j_error(data_in, error),
                retryable=error.is_retryable(),
            ) from None
        except:
            tx.rollback()
            raise
        else:
            tx.commit()

    def ingest_extracted_items(
        self,
        models: Iterable[AnyExtractedModel | MExPrimarySource],
    ) -> Generator[None, None, None]:
        """Ingest a list of extracted models into the graph."""
        settings = BackendSettings.get()
        with self.driver.session(default_access_mode=WRITE_ACCESS) as session:
            for model in models:
                data_in = transform_model_into_ingest_data(model)
                with session.begin_transaction(
                    timeout=settings.graph_tx_timeout,
                    metadata=data_in.metadata(),
                ) as tx:
                    self._ingest_extracted_item_tx(tx, data_in)
                yield

    def _merge_rule_item(  # deprecated, to be removed in MX-1957
        self,
        tx: Transaction,
        model: AnyRuleModel,
        stable_target_id: Identifier,
    ) -> None:
        """Upsert a rule item including merged item and nested objects.

        The given model is created or updated with all its inline properties.
        All nested properties (like Text or Link) are created as their own nodes
        and linked via edges. For multi-valued fields, the position of each nested
        object is stored as a property on the outbound edge.
        Any nested objects that are found in the graph, but are not present on the
        model any more are purged.
        In addition, a merged item is created (if it does not exist yet) and the
        extracted item is linked to it via an edge with the label `stableTargetId`.

        Args:
            tx: Active Neo4j transaction
            model: Model to merge into the graph as a node
            stable_target_id: Identifier the connected merged item should have
        """
        query_builder = QueryBuilder.get()

        text_fields = set(TEXT_FIELDS_BY_CLASS_NAME[model.entityType])
        link_fields = set(LINK_FIELDS_BY_CLASS_NAME[model.entityType])
        mutable_fields = set(MUTABLE_FIELDS_BY_CLASS_NAME[model.entityType])
        final_fields = set(FINAL_FIELDS_BY_CLASS_NAME[model.entityType])

        mutable_values = model.model_dump(include=mutable_fields)
        final_values = model.model_dump(include=final_fields)
        all_values = {**mutable_values, **final_values}

        text_values = model.model_dump(include=text_fields)
        link_values = model.model_dump(include=link_fields)

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

        query = query_builder.merge_rule_item(
            current_label=model.entityType,
            merged_label=ensure_prefix(model.stemType, "Merged"),
            nested_edge_labels=nested_edge_labels,
            nested_node_labels=nested_node_labels,
        )

        tx.run(
            str(query),
            {
                "stable_target_id": stable_target_id,
                "on_match": mutable_values,
                "on_create": all_values,
                "nested_values": nested_values,
                "nested_positions": nested_positions,
            },
        )

    def _merge_rule_edges(  # deprecated, to be removed in MX-1957
        self,
        tx: Transaction,
        model: AnyRuleModel,
        stable_target_id: Identifier,
    ) -> None:
        """Merge edges into the graph for all relations originating from one rule model.

        All fields containing references will be iterated over. When the referenced node
        is found and no such relation exists yet, it will be created.
        A position attribute is added to all edges, that stores the index the reference
        had in list of references on the originating model. This way, we can preserve
        the order for example of `contact` persons referenced on an activity.

        Args:
            tx: Active Neo4j transaction
            model: Model to ensure all edges are created in the graph
            stable_target_id: Identifier of the connected merged item
        """
        query_builder = QueryBuilder.get()

        ref_fields = REFERENCE_FIELDS_BY_CLASS_NAME[model.entityType]
        ref_values = model.model_dump(include=set(ref_fields))
        ref_values.update({"stableTargetId": stable_target_id})

        ref_labels: list[str] = []
        ref_identifiers: list[str] = []
        ref_positions: list[int] = []

        for field, identifiers in to_key_and_values(ref_values):
            for position, identifier in enumerate(identifiers):
                ref_identifiers.append(identifier)
                ref_positions.append(position)
                ref_labels.append(field)

        query = query_builder.merge_rule_edges(
            current_label=model.entityType,
            merged_label=ensure_prefix(model.stemType, "Merged"),
            ref_labels=ref_labels,
        )
        result = Result(
            tx.run(
                str(query),
                {
                    "stable_target_id": stable_target_id,
                    "ref_identifiers": ref_identifiers,
                    "ref_positions": ref_positions,
                },
            )
        )

        expectations_by_locator = transform_edges_into_expectations_by_edge_locator(
            start_node_type=model.entityType,
            ref_labels=ref_labels,
            ref_identifiers=ref_identifiers,
            ref_positions=ref_positions,
        )
        expected_edges = set(expectations_by_locator)
        merged_edges = set(result["edges"])

        if missing_edges := sorted(expected_edges - merged_edges):
            expectations = ", ".join(expectations_by_locator[e] for e in missing_edges)
            msg = f"failed to merge {len(missing_edges)} edges: {expectations}"
            raise InconsistentGraphError(msg)
        if unexpected_edges := sorted(merged_edges - expected_edges):
            surplus = ", ".join(unexpected_edges)
            msg = f"merged {len(unexpected_edges)} edges more than expected: {surplus}"
            raise RuntimeError(msg)

    def _ingest_rule_set_tx(  # deprecated, to be removed in MX-1957
        self,
        tx: Transaction,
        rule_set: AnyRuleSetResponse,
    ) -> None:
        """Ingest a single rule set into the graph."""
        for rule in (rule_set.additive, rule_set.subtractive, rule_set.preventive):
            self._merge_rule_item(tx, rule, rule_set.stableTargetId)
            self._merge_rule_edges(tx, rule, rule_set.stableTargetId)

    def ingest_rule_set(  # deprecated, to be removed in MX-1957
        self,
        rule_set: AnyRuleSetResponse,
    ) -> None:
        """Ingest a single rule set into the graph."""
        settings = BackendSettings.get()
        with self.driver.session(default_access_mode=WRITE_ACCESS) as session:  # noqa: SIM117
            with session.begin_transaction(
                timeout=settings.graph_tx_timeout,
                metadata={"stable_target_id": rule_set.stableTargetId},
            ) as tx:
                self._ingest_rule_set_tx(tx, rule_set)

    def match_item(
        self,
        extracted_item: AnyExtractedModel,
        merged_item: AnyMergedModel,
        old_rule_set: AnyRuleSetResponse,
        new_rule_set: AnyRuleSetResponse,
    ) -> None:
        """Match an extracted item to another merged item and clean up afterwards."""
        settings = BackendSettings.get()
        query_builder = QueryBuilder.get()
        with self.driver.session(default_access_mode=WRITE_ACCESS) as session:  # noqa: SIM117
            with session.begin_transaction(
                timeout=settings.graph_tx_timeout,
                metadata={
                    "extracted_identifier": extracted_item.identifier,
                    "old_merged_identifier": extracted_item.stableTargetId,
                    "new_merged_identifier": merged_item.identifier,
                },
            ) as tx:
                try:
                    preconditions = Result(
                        tx.run(
                            str(query_builder.check_match_preconditions()),
                            extracted_identifier=str(extracted_item.identifier),
                            merged_identifier=(extracted_item.stableTargetId),
                            blocked_types=["ExtractedPerson"],
                        )
                    )
                    if not all(preconditions.one().values()):
                        raise MatchingError(preconditions)
                    tx.run(
                        str(query_builder.update_stable_target_id()),
                        extracted_identifier=str(extracted_item.identifier),
                        merged_identifier=(extracted_item.stableTargetId),
                    )
                    self._ingest_rule_set_tx(tx, old_rule_set)
                    self._ingest_rule_set_tx(tx, new_rule_set)
                    is_merged_item_orphaned = Result(
                        tx.run(
                            str(query_builder.is_merged_item_orphaned()),
                            identifier=(extracted_item.stableTargetId),
                        )
                    )
                    if is_merged_item_orphaned["is_orphaned"] is True:
                        tx.run(
                            str(query_builder.update_all_references()),
                            old_identifier=str(extracted_item.stableTargetId),
                            new_identifier=(merged_item.identifier),
                        )
                        tx.run(
                            str(query_builder.deleted_orphaned_merged_item()),
                            identifier=(extracted_item.stableTargetId),
                        )
                except:
                    tx.rollback()
                    raise
                else:
                    tx.commit()

    def flush(self) -> None:
        """Flush the database by deleting all nodes, constraints and indexes.

        This operation only executes when debug mode is enabled in settings.
        Completely wipes the Neo4j database including all data, constraints,
        and indexes. Used for testing and development cleanup.
        """
        settings = BackendSettings.get()
        if settings.debug is True:
            with self.driver.session(default_access_mode=WRITE_ACCESS) as session:
                session.run("MATCH (n) DETACH DELETE n;")
                constraints = session.run("SHOW ALL CONSTRAINTS;")
                for row in constraints.to_eager_result().records:
                    session.run(f"DROP CONSTRAINT {row['name']};")
                indexes = session.run("SHOW ALL INDEXES;")
                for row in indexes.to_eager_result().records:
                    session.run(f"DROP INDEX {row['name']};")
        else:
            msg = "database flush was attempted outside of debug mode"
            raise MExError(msg)
