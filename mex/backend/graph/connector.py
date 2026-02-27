from collections import deque
from typing import TYPE_CHECKING, Any

from neo4j import (
    READ_ACCESS,
    WRITE_ACCESS,
    Driver,
    GraphDatabase,
    NotificationDisabledClassification,
    Transaction,
)
from neo4j.exceptions import ConstraintError, Neo4jError

from mex.backend.graph.exceptions import (
    DeletionFailedError,
    IngestionError,
    MatchingError,
)
from mex.backend.graph.models import (
    MEX_EDITOR_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE,
    ExtractedPrimarySourceWithHardcodedIdentifiers,
    IngestData,
    Result,
)
from mex.backend.graph.query import Query, QueryBuilder
from mex.backend.graph.transform import (
    expand_references_in_search_result,
    get_error_details_from_neo4j_error,
    transform_model_into_ingest_data,
    validate_ingested_data,
)
from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.exceptions import MExError
from mex.common.fields import (
    ALL_MODEL_CLASSES_BY_NAME,
    ALL_REFERENCE_FIELD_NAMES,
    SEARCHABLE_CLASSES,
    SEARCHABLE_FIELDS,
)
from mex.common.logging import logger
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID,
    RULE_MODEL_CLASSES_BY_NAME,
    AnyExtractedModel,
    AnyMergedModel,
    AnyRuleModel,
    AnyRuleSetResponse,
)

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator, Iterable, Sequence

    from mex.common.types import Identifier


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
            notifications_disabled_classifications=[
                # mute warnings about labels used in queries but missing in graph
                NotificationDisabledClassification.UNRECOGNIZED,
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
        deque(self.ingest_items([MEX_PRIMARY_SOURCE, MEX_EDITOR_PRIMARY_SOURCE]))
        logger.info("seeded primary sources 'mex' and 'mex-editor'")

    def close(self) -> None:
        """Close the connector's underlying requests session."""
        self.driver.close()

    def commit(
        self,
        query: Query,
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
            return Result(session.run(query.render(), parameters))

    def _fetch_extracted_or_rule_items(  # noqa: PLR0913
        self,
        query_string: str | None,
        identifier: str | None,
        stable_target_id: str | None,
        entity_type: Sequence[str],
        referenced_identifiers: Sequence[str | None] | None,
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
        if reference_field and reference_field not in ALL_REFERENCE_FIELD_NAMES:
            msg = "Invalid field name."
            raise ValueError(msg)
        if (
            reference_field == "hadPrimarySource"
            and referenced_identifiers
            and MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID in referenced_identifiers
        ):
            referenced_identifiers = [
                None if id_ == MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID else id_
                for id_ in referenced_identifiers
            ]
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
        filter_items_with_rules = False
        if (
            reference_field == "hadPrimarySource"
            and referenced_identifiers is not None
            and MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID in referenced_identifiers
        ):
            filter_items_with_rules = True
            referenced_identifiers = [
                id_
                for id_ in referenced_identifiers
                if id_ != MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID
            ] or None
        query_builder = QueryBuilder.get()
        query = query_builder.fetch_merged_items(
            filter_by_query_string=bool(query_string),
            filter_by_identifier=bool(identifier),
            filter_by_referenced_identifiers=bool(referenced_identifiers),
            filter_items_with_rules=filter_items_with_rules,
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
        entity_types: list[str],
    ) -> bool:
        """Validate whether an item with the given identifier and entity type exists.

        Args:
            identifier: Identifier of the to-be-checked item
            entity_types: Allowed entity types of the to-be-checked item

        Returns:
            Boolean representing the existence of the requested item
        """
        if not all(e in ALL_MODEL_CLASSES_BY_NAME for e in entity_types):
            return False
        query_builder = QueryBuilder.get()
        query = query_builder.exists_item(
            node_labels=entity_types,
        )
        result = self.commit(
            query,
            identifier=identifier,
        )
        return bool(result["exists"])

    def _run_ingest_in_transaction(
        self,
        tx: Transaction,
        model: AnyExtractedModel
        | AnyRuleSetResponse
        | ExtractedPrimarySourceWithHardcodedIdentifiers,
    ) -> None:
        """Ingest a single item in a database transaction."""
        query_builder = QueryBuilder.get()
        if isinstance(model, AnyRuleSetResponse):
            items_to_ingest: list[
                AnyExtractedModel
                | ExtractedPrimarySourceWithHardcodedIdentifiers
                | AnyRuleModel
            ] = [
                model.additive,
                model.subtractive,
                model.preventive,
            ]
        else:
            items_to_ingest = [model]

        for item in items_to_ingest:
            query = query_builder.get_ingest_query_for_entity_type(item.entityType)
            data_in = transform_model_into_ingest_data(
                item, stable_target_id=model.stableTargetId
            )
            tx_result = tx.run(query, data=data_in.model_dump())
            result = Result(tx_result)
            result.log_notifications()
            data_out = IngestData.model_validate(result.one())
            error_details = validate_ingested_data(data_in, data_out)
            if error_details:
                msg = (
                    f"Could not merge {model.entityType}"
                    f"(stableTargetId='{model.stableTargetId}', ...)"
                )
                raise IngestionError(msg, errors=error_details, retryable=False)

    def ingest_items(
        self,
        models: Iterable[
            AnyExtractedModel
            | AnyRuleSetResponse
            | ExtractedPrimarySourceWithHardcodedIdentifiers
        ],
    ) -> Generator[None]:
        """Ingest a list of extracted models or rule set responses into the graph."""
        settings = BackendSettings.get()
        with self.driver.session(default_access_mode=WRITE_ACCESS) as session:
            for model in models:
                with session.begin_transaction(
                    timeout=settings.graph_tx_timeout,
                    metadata={
                        "stableTargetId": model.stableTargetId,
                        "entityType": model.entityType,
                    },
                ) as tx:
                    try:
                        self._run_ingest_in_transaction(tx, model)
                    except Neo4jError as error:
                        tx.rollback()
                        msg = (
                            f"{type(error).__name__} caused by {model.entityType}"
                            f"(stableTargetId='{model.stableTargetId}', ...)"
                        )
                        raise IngestionError(
                            msg,
                            errors=get_error_details_from_neo4j_error(model, error),
                            retryable=error.is_retryable(),
                        ) from None
                    except:
                        tx.rollback()
                        raise
                    else:
                        tx.commit()
                yield

    def _check_match_preconditions_tx(
        self,
        tx: Transaction,
        extracted_item: AnyExtractedModel,
        merged_item: AnyMergedModel,
    ) -> None:
        """Raise an error when the preconditions for performing a match aren't met."""
        settings = BackendSettings.get()
        query_builder = QueryBuilder.get()
        check_match_preconditions_query = query_builder.check_match_preconditions()

        preconditions = Result(
            tx.run(
                check_match_preconditions_query.render(),
                extracted_identifier=str(extracted_item.identifier),
                merged_identifier=str(merged_item.identifier),
                blocked_types=[t.value for t in settings.non_matchable_types],
            )
        )
        results = preconditions.one()
        violated = sorted(
            condition for condition, is_met in results.items() if is_met is False
        )
        unverifiable = sorted(
            condition for condition, is_met in results.items() if is_met is None
        )
        if violated or unverifiable:
            parts = []
            if violated:
                parts.append(f"Violated: {', '.join(violated)}")
            if unverifiable:
                parts.append(f"Unverifiable: {', '.join(unverifiable)}")
            msg = f"Matching precondition check failed. {'. '.join(parts)}"
            raise MatchingError(msg)

    def _match_item_tx(
        self,
        tx: Transaction,
        extracted_item: AnyExtractedModel,
        merged_item: AnyMergedModel,
    ) -> None:
        """Run all required matching steps in a single transaction."""
        self._check_match_preconditions_tx(tx, extracted_item, merged_item)
        raise NotImplementedError

    def match_item(
        self,
        extracted_item: AnyExtractedModel,
        merged_item: AnyMergedModel,
    ) -> None:
        """Match an extracted item to a new merged item and clean up afterwards."""
        settings = BackendSettings.get()
        with (
            self.driver.session(default_access_mode=WRITE_ACCESS) as session,
            session.begin_transaction(
                timeout=settings.graph_tx_timeout,
                metadata={
                    "extracted_identifier": extracted_item.identifier,
                    "old_merged_identifier": extracted_item.stableTargetId,
                    "new_merged_identifier": merged_item.identifier,
                },
            ) as tx,
        ):
            try:
                self._match_item_tx(tx, extracted_item, merged_item)
            except:
                tx.rollback()
                raise
            else:
                tx.commit()

    def delete_item(self, identifier: str) -> Result:
        """Delete a merged item including all extracted items and rule-sets."""
        query_builder = QueryBuilder.get()
        query = query_builder.delete_merged_item()
        try:
            return self.commit(
                query,
                access_mode=WRITE_ACCESS,
                identifier=str(identifier),
            )
        except ConstraintError as error:
            msg = f"Deletion of MergedItem(stableTargetId='{identifier}', ...) failed."
            raise DeletionFailedError(
                msg,
                errors=get_error_details_from_neo4j_error(identifier, error),
                retryable=error.is_retryable(),
            ) from None

    def delete_rule_set(self, stable_target_id: str) -> Result:
        """Delete a rule-set by stableTargetId.

        Deletes all additive, subtractive, and preventive rules connected to the
        given stableTargetId, along with their nested items and outbound connections.
        """
        query_builder = QueryBuilder.get()
        query = query_builder.delete_rule_set()
        try:
            return self.commit(
                query,
                access_mode=WRITE_ACCESS,
                stable_target_id=str(stable_target_id),
            )
        except ConstraintError as error:
            msg = (
                f"Deletion of RuleSet(stableTargetId='{stable_target_id}', ...) failed."
            )
            raise DeletionFailedError(
                msg,
                errors=get_error_details_from_neo4j_error(stable_target_id, error),
                retryable=error.is_retryable(),
            ) from None

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
