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
    MergingError,
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
    redirect_references,
    transform_model_into_ingest_data,
    transform_reference_filters_to_raw_fields,
    transform_reference_filters_to_raw_filters,
    validate_ingested_data,
)
from mex.backend.identity.helpers import reset_identity_cache
from mex.backend.models import ReferenceFilter
from mex.backend.rules.transform import (
    transform_raw_rule_set_to_rule_set_response,
    transform_raw_rules_to_rule_set_response,
)
from mex.backend.settings import BackendSettings
from mex.backend.types import ReferenceFieldName
from mex.common.connector import BaseConnector
from mex.common.exceptions import MExError
from mex.common.fields import (
    ALL_MODEL_CLASSES_BY_NAME,
    INBOUND_REFERENCE_FIELDS_BY_CLASS_NAME,
    SEARCHABLE_CLASSES,
    SEARCHABLE_FIELDS,
)
from mex.common.logging import logger
from mex.common.merged.main import merge_rule_set_responses
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID,
    PREVIEW_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_TYPE_BY_NAME,
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyExtractedModel,
    AnyMergedModel,
    AnyRuleModel,
    AnyRuleSetResponse,
    ExtractedModelTypeAdapter,
)
from mex.common.transform import ensure_postfix
from mex.common.types import MergedPrimarySourceIdentifier

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
        result = self.commit(query_builder.get_database_status())
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
        searchable_classes = [
            label
            for label in SEARCHABLE_CLASSES
            if label not in PREVIEW_MODEL_CLASSES_BY_NAME
        ]
        result = self.commit(
            query_builder.get_full_text_search_index(),
            access_mode=WRITE_ACCESS,
        )
        if (index := result.one_or_none()) and (
            set(index["node_labels"]) != set(searchable_classes)
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
                node_labels=searchable_classes,
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

    def _run(
        self,
        query: Query,
        /,
        tx: Transaction | None = None,
        access_mode: str = READ_ACCESS,
        **parameters: Any,  # noqa: ANN401
    ) -> Result:
        """Run a query in the given transaction, or in a fresh session when none given.

        Reads that are part of a bigger write operation need to see that operation's
        uncommitted state, so they have to run in its transaction instead of opening
        a session of their own.

        Args:
            query: Query instance to render and run
            tx: Optional open transaction to run the query in
            access_mode: Access mode to use when no transaction is given
            parameters: Keyword parameters for the query

        Returns:
            Graph result instance
        """
        if tx is None:
            return self.commit(query, access_mode=access_mode, **parameters)
        return Result(tx.run(query.render(), parameters))

    def _fetch_extracted_or_rule_items(  # noqa: PLR0913, PLR0917
        self,
        query_string: str | None,
        identifier: str | None,
        entity_type: Sequence[str],
        reference_filters: Sequence[ReferenceFilter] | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for extracted or rule items.

        Args:
            query_string: Optional full text search query term
            identifier: Optional identifier filter
            entity_type: List of allowed entity types
            reference_filters: Optional reference field filters
            skip: How many items to skip for pagination
            limit: How many items to return at most

        Returns:
            Graph result instance
        """
        raw_reference_filters = transform_reference_filters_to_raw_filters(
            reference_filters
        )
        raw_reference_fields = transform_reference_filters_to_raw_fields(
            reference_filters
        )
        query_builder = QueryBuilder.get()
        query = query_builder.fetch_extracted_or_rule_items(
            filter_by_query_string=bool(query_string),
            filter_by_identifier=bool(identifier),
            filter_by_references=bool(raw_reference_filters),
            reference_fields=raw_reference_fields,
        )
        result = self.commit(
            query,
            query_string=query_string,
            identifier=identifier,
            labels=entity_type,
            reference_filters=raw_reference_filters,
            reference_fields=raw_reference_fields,
            skip=skip,
            limit=limit,
        )
        for query_result in result.all():
            for item in query_result["items"]:
                item.update(expand_references_in_search_result(item.pop("_refs")))
        return result

    def fetch_extracted_items(  # noqa: PLR0913, PLR0917
        self,
        query_string: str | None,
        identifier: str | None,
        entity_type: Sequence[str] | None,
        reference_filters: Sequence[ReferenceFilter] | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for extracted items.

        Args:
            query_string: Optional full text search query term
            identifier: Optional identifier filter
            entity_type: Optional entity type filter
            reference_filters: Optional reference field filters
            skip: How many items to skip for pagination
            limit: How many items to return at most

        Returns:
            Graph result instance
        """
        return self._fetch_extracted_or_rule_items(
            query_string=query_string,
            identifier=identifier,
            entity_type=entity_type or list(EXTRACTED_MODEL_CLASSES_BY_NAME),
            reference_filters=reference_filters,
            skip=skip,
            limit=limit,
        )

    def fetch_rule_items(  # noqa: PLR0913, PLR0917
        self,
        query_string: str | None,
        identifier: str | None,
        entity_type: Sequence[str] | None,
        reference_filters: Sequence[ReferenceFilter] | None,
        skip: int,
        limit: int,
    ) -> Result:
        """Query the graph for rule items.

        Args:
            query_string: Optional full text search query term
            identifier: Optional identifier filter
            entity_type: Optional entity type filter
            reference_filters: Optional reference field filters
            skip: How many items to skip for pagination
            limit: How many items to return at most

        Returns:
            Graph result instance
        """
        return self._fetch_extracted_or_rule_items(
            query_string=query_string,
            identifier=identifier,
            entity_type=entity_type or list(RULE_MODEL_CLASSES_BY_NAME),
            reference_filters=reference_filters,
            skip=skip,
            limit=limit,
        )

    def fetch_rule_set_response(
        self,
        stable_target_id: str,
        tx: Transaction | None = None,
    ) -> Result:
        """Query the graph for the rule set belonging to one merged item.

        Args:
            stable_target_id: Identifier of the merged item whose rule set to fetch
            tx: Optional open transaction to run the query in

        Returns:
            Graph result instance with a single rule-set-response shaped record
            (one column per rule_set_field plus stableTargetId), or no records
            when the merged item has no rule nodes
        """
        query_builder = QueryBuilder.get()
        query = query_builder.get_rule_set_response()
        result = self._run(query, tx=tx, identifier=stable_target_id)
        if record := result.one_or_none():
            for field in RULE_MODEL_CLASSES_BY_TYPE_BY_NAME:
                if (component := record.get(field)) is not None:
                    component.update(
                        expand_references_in_search_result(component.pop("_refs"))
                    )
                    # stableTargetId is present both as a node property and as the
                    # expanded stableTargetId relationship, but is not a field on
                    # rule models yet, so drop it before the component is validated
                    component.pop("stableTargetId", None)
        return result

    def fetch_merged_items(  # noqa: PLR0913, PLR0917
        self,
        query_string: str | None,
        identifier: str | None,
        entity_type: Sequence[str] | None,
        reference_filters: Sequence[ReferenceFilter] | None,
        skip: int,
        limit: int,
        tx: Transaction | None = None,
    ) -> Result:
        """Query the graph for merged items.

        Args:
            query_string: Optional full text search query term
            identifier: Optional merged item identifier filter
            entity_type: Optional merged entity type filter
            reference_filters: Optional reference field filters
            skip: How many items to skip for pagination
            limit: How many items to return at most
            tx: Optional open transaction to run the query in

        Returns:
            Graph result instance
        """
        raw_reference_filters = transform_reference_filters_to_raw_filters(
            reference_filters
        )
        raw_reference_fields = transform_reference_filters_to_raw_fields(
            reference_filters
        )
        query_builder = QueryBuilder.get()
        query = query_builder.fetch_merged_items(
            filter_by_query_string=bool(query_string),
            filter_by_identifier=bool(identifier),
            filter_by_references=bool(raw_reference_filters),
            reference_fields=raw_reference_fields,
        )
        result = self._run(
            query,
            tx=tx,
            query_string=query_string,
            identifier=identifier,
            labels=entity_type or list(MERGED_MODEL_CLASSES_BY_NAME),
            reference_filters=raw_reference_filters,
            reference_fields=raw_reference_fields,
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
        entity_types: Sequence[str],
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
        stable_target_id: str | None = None,
    ) -> None:
        """Ingest a single item in a database transaction.

        Args:
            tx: Open transaction to run the queries in
            model: Extracted model or rule set response to ingest
            stable_target_id: Merged item to attach the ingested item to, defaults to
                the model's own stable target id. Merging needs this override, because
                an extracted item that was moved to another merged item still computes
                its own stable target id from the identity provider, which does not
                know about the move until the merge is committed.
        """
        query_builder = QueryBuilder.get()
        target_id = stable_target_id or str(model.stableTargetId)
        if isinstance(model, AnyRuleSetResponse):
            items_to_ingest: list[
                AnyExtractedModel
                | ExtractedPrimarySourceWithHardcodedIdentifiers
                | AnyRuleModel
            ] = [
                model.additive,
                model.subtractive,
                model.preventive,
                model.workflow,
            ]
        else:
            items_to_ingest = [model]

        for item in items_to_ingest:
            query = query_builder.get_ingest_query_for_entity_type(item.entityType)
            data_in = transform_model_into_ingest_data(item, stable_target_id=target_id)
            tx_result = tx.run(query, data=data_in.model_dump())
            result = Result(tx_result)
            result.log_notifications()
            data_out = IngestData.model_validate(result.one())
            error_details = validate_ingested_data(data_in, data_out)
            if error_details:
                msg = (
                    f"Could not merge {model.entityType}"
                    f"(stableTargetId='{target_id}', ...)"
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

    def _check_merge_preconditions_tx(
        self,
        tx: Transaction,
        goner: AnyMergedModel,
        keeper: AnyMergedModel,
    ) -> None:
        """Raise an error when the preconditions for performing a merge aren't met."""
        settings = BackendSettings.get()
        query_builder = QueryBuilder.get()
        check_merge_preconditions_query = query_builder.check_merge_preconditions()

        preconditions = Result(
            tx.run(
                check_merge_preconditions_query.render(),
                goner_identifier=str(goner.identifier),
                keeper_identifier=str(keeper.identifier),
                non_mergeable_types=[t.value for t in settings.non_mergeable_types],
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
            msg = f"Merging precondition check failed. {'. '.join(parts)}"
            raise MergingError(msg)

    def _move_extracted_items_tx(
        self,
        tx: Transaction,
        goner: AnyMergedModel,
        keeper: AnyMergedModel,
    ) -> dict[str, Any]:
        """Move all extracted items of the goner over to the keeper.

        Args:
            tx: Open transaction to run the query in
            goner: Merged item that is merged away
            keeper: Merged item that survives the merge

        Returns:
            Record with the number of moved items and their provenance
        """
        query_builder = QueryBuilder.get()
        query = query_builder.move_extracted_items()
        result = self._run(
            query,
            tx=tx,
            goner_identifier=str(goner.identifier),
            keeper_identifier=str(keeper.identifier),
        )
        return result.one()

    def _fetch_rule_set_tx(
        self,
        tx: Transaction,
        item: AnyMergedModel,
    ) -> AnyRuleSetResponse:
        """Fetch the rule set of a merged item, or an empty one when it has none.

        Args:
            tx: Open transaction to run the query in
            item: Merged item whose rule set to fetch

        Returns:
            Rule set response of the given merged item
        """
        result = self.fetch_rule_set_response(str(item.identifier), tx=tx)
        if record := result.one_or_none():
            return transform_raw_rule_set_to_rule_set_response(record)
        response_class = RULE_SET_RESPONSE_CLASSES_BY_NAME[
            ensure_postfix(item.stemType, "RuleSetResponse")
        ]
        return response_class(stableTargetId=item.identifier)

    def _fetch_items_referencing_tx(
        self,
        tx: Transaction,
        item: AnyMergedModel,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Fetch all merged items that have a component referencing the given item.

        Reference filters are conjunctive, so one query per candidate field is needed.
        Only fields that can point at the given entity type are queried.

        Args:
            tx: Open transaction to run the queries in
            item: Merged item whose referencing items to find
            page_size: How many items to fetch per request

        Returns:
            List of merged search result items, de-duplicated by identifier
        """
        fields = sorted(
            set(INBOUND_REFERENCE_FIELDS_BY_CLASS_NAME[item.entityType])
            - {"identifier", "stableTargetId"}
        )
        items_by_identifier: dict[str, dict[str, Any]] = {}
        for field in fields:
            skip = 0
            while True:
                record = self.fetch_merged_items(
                    query_string=None,
                    identifier=None,
                    entity_type=None,
                    reference_filters=[
                        ReferenceFilter(
                            field=ReferenceFieldName(field),
                            identifiers=[item.identifier],
                        )
                    ],
                    skip=skip,
                    limit=page_size,
                    tx=tx,
                ).one()
                for referencing_item in record["items"]:
                    items_by_identifier[str(referencing_item["identifier"])] = (
                        referencing_item
                    )
                skip += page_size
                if skip >= int(record["total"]):
                    break
        return list(items_by_identifier.values())

    def _fetch_valid_primary_sources_tx(
        self,
        tx: Transaction,
        item: AnyMergedModel,
    ) -> list[MergedPrimarySourceIdentifier]:
        """Collect the primary sources contributing to the given merged item.

        These decide which preventive rule values may survive a merge: a preventive
        value is only meaningful while the primary source it blocks still contributes
        an extracted item. The mex-editor primary source is always valid, because it
        stands for the values contributed by rules.

        Args:
            tx: Open transaction to run the query in
            item: Merged item whose primary sources to collect

        Returns:
            List of primary source identifiers
        """
        record = self.fetch_merged_items(
            query_string=None,
            identifier=str(item.identifier),
            entity_type=None,
            reference_filters=None,
            skip=0,
            limit=1,
            tx=tx,
        ).one()
        primary_sources = {
            str(identifier)
            for search_result_item in record["items"]
            for component in search_result_item["_components"]
            for identifier in component.get("hadPrimarySource", [])
        }
        return [
            MergedPrimarySourceIdentifier(identifier)
            for identifier in sorted(primary_sources)
            if identifier != str(MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID)
        ] + [MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID]

    @staticmethod
    def _redirect_rule_set(
        rule_set: AnyRuleSetResponse,
        old: str,
        new: str,
        owner: str,
    ) -> AnyRuleSetResponse:
        """Point all references of a rule set from one merged item at another."""
        return type(rule_set).model_validate(
            {
                **{
                    field: redirect_references(
                        getattr(rule_set, field), old, new, owner
                    )
                    for field in RULE_MODEL_CLASSES_BY_TYPE_BY_NAME
                },
                "stableTargetId": rule_set.stableTargetId,
            }
        )

    def _relink_referencing_items_tx(
        self,
        tx: Transaction,
        referencing_items: list[dict[str, Any]],
        goner: AnyMergedModel,
        keeper: AnyMergedModel,
    ) -> None:
        """Point every item referencing the goner at the keeper instead.

        Rewritten items are simply ingested again: the ingest query replaces all of an
        item's reference relations, which also de-duplicates them and keeps their
        positions contiguous.

        The given items must have been read before the goner's extracted items were
        moved, so that their components still agree with the identity provider about
        which merged item they belong to.

        Args:
            tx: Open transaction to run the queries in
            referencing_items: Merged search result items referencing the goner
            goner: Merged item that is merged away
            keeper: Merged item that survives the merge
        """
        goner_identifier = str(goner.identifier)
        keeper_identifier = str(keeper.identifier)
        for item in referencing_items:
            item_identifier = str(item["identifier"])
            # the goner's own items belong to the keeper once they have been moved,
            # which turns their references to the goner into self-references
            owner = (
                keeper_identifier
                if item_identifier == goner_identifier
                else item_identifier
            )
            rule_components = []
            for component in item["_components"]:
                if component["entityType"] in EXTRACTED_MODEL_CLASSES_BY_NAME:
                    extracted = ExtractedModelTypeAdapter.validate_python(component)
                    redirected = redirect_references(
                        extracted, goner_identifier, keeper_identifier, owner
                    )
                    if redirected != extracted:
                        self._run_ingest_in_transaction(
                            tx, redirected, stable_target_id=owner
                        )
                elif component["entityType"] in RULE_MODEL_CLASSES_BY_NAME:
                    rule_components.append(component)
            # the rule sets of the goner and the keeper are rewritten in full later on
            if rule_components and item_identifier not in (
                goner_identifier,
                keeper_identifier,
            ):
                rule_set = transform_raw_rules_to_rule_set_response(rule_components)
                redirected_rule_set = self._redirect_rule_set(
                    rule_set, goner_identifier, keeper_identifier, owner
                )
                if redirected_rule_set != rule_set:
                    self._run_ingest_in_transaction(tx, redirected_rule_set)

    def _merge_items_tx(
        self,
        tx: Transaction,
        goner: AnyMergedModel,
        keeper: AnyMergedModel,
    ) -> dict[str, Any]:
        """Run all required merging steps in a single transaction.

        Args:
            tx: Open transaction to run the queries in
            goner: Merged item that is merged away
            keeper: Merged item that survives the merge

        Returns:
            Record describing the extracted items that were moved to the keeper
        """
        self._check_merge_preconditions_tx(tx, goner, keeper)
        goner_identifier = str(goner.identifier)
        keeper_identifier = str(keeper.identifier)

        # read everything that the rewrites below invalidate
        goner_rule_set = self._fetch_rule_set_tx(tx, goner)
        keeper_rule_set = self._fetch_rule_set_tx(tx, keeper)
        referencing_items = self._fetch_items_referencing_tx(tx, goner)

        # move the goner's extracted items over, the keeper owns them from now on
        move_result = self._move_extracted_items_tx(tx, goner, keeper)

        # point everything that still references the goner at the keeper
        self._relink_referencing_items_tx(tx, referencing_items, goner, keeper)

        # a goner that was superseded before must not pass that reference on
        goner_rule_set = type(goner_rule_set).model_validate(
            {
                **goner_rule_set.model_dump(),
                "additive": {
                    **goner_rule_set.additive.model_dump(),
                    "supersededBy": None,
                },
            }
        )

        # migrate the goner's rule values into the keeper's rule set
        merged_rule_set = merge_rule_set_responses(
            self._redirect_rule_set(
                keeper_rule_set, goner_identifier, keeper_identifier, keeper_identifier
            ),
            self._redirect_rule_set(
                goner_rule_set, goner_identifier, keeper_identifier, keeper_identifier
            ),
            self._fetch_valid_primary_sources_tx(tx, keeper),
        )
        self._run_ingest_in_transaction(tx, merged_rule_set)

        # leave the goner behind as a tombstone pointing at the keeper
        # imported here, because rule helpers import this connector in turn
        from mex.backend.rules.helpers import build_tombstone_rule_set  # noqa: PLC0415

        self._run_ingest_in_transaction(tx, build_tombstone_rule_set(goner, keeper))
        return move_result

    def merge_items(
        self,
        goner: AnyMergedModel,
        keeper: AnyMergedModel,
    ) -> None:
        """Merge a goner merged item into a keeper merged item."""
        settings = BackendSettings.get()
        with (
            self.driver.session(default_access_mode=WRITE_ACCESS) as session,
            session.begin_transaction(
                timeout=settings.graph_tx_timeout,
                metadata={
                    "goner_identifier": goner.identifier,
                    "keeper_identifier": keeper.identifier,
                },
            ) as tx,
        ):
            try:
                move_result = self._merge_items_tx(tx, goner, keeper)
            except:
                tx.rollback()
                raise
            else:
                tx.commit()
        reset_identity_cache(move_result["moved_identities"])

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

        Deletes all additive, subtractive, preventive, and workflow rules connected to
        the given stableTargetId, along with their nested items and outbound
        connections.
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
