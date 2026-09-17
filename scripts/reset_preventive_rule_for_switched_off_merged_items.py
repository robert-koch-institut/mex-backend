import click

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import MergingError
from mex.backend.merged.helpers import get_merged_item_from_graph
from mex.backend.rules.helpers import get_rule_set_from_graph, update_and_get_rule_set
from mex.common.fields import REQUIRED_FIELDS_BY_CLASS_NAME
from mex.common.logging import logger
from mex.common.models import (
    AnyRuleSetRequest,
    AnyRuleSetResponse,
    RuleSetRequestTypeAdapter,
)
from mex.common.types import Identifier, PublishingTarget


@click.command()
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    help="Default: dry run (no writing to data base). Use '--no-dry-run' for real run.",
)
def reset_preventive_rule_for_switched_off_merged_items(*, dry_run: bool) -> None:
    """Reset preventive rules for "switched off" merged items.

    Finds items which have rules. Checks if all or only some required fields are
    switched off by a preventive rule. For items with all required fields switched-off,
    and forbidden publishing targets set in workflow rule, the preventive rule is reset
    to empty.
    """
    logger.info("migration reset_preventive_rule_for_switched_off_merged_items started")
    if dry_run:
        logger.info("\nDRY RUN - not writing to database\n")

    forbidden_targets = [PublishingTarget.INVENIO, PublishingTarget.DATENKOMPASS]

    items_with_rule_ids = get_all_rule_set_ids()

    logger.info("Migrating %s", len(items_with_rule_ids))

    for index, stable_target_id in enumerate(items_with_rule_ids, start=1):
        rule_set = get_rule_set_from_graph(Identifier(stable_target_id))
        if rule_set is None:
            msg = f"no rule set found for '{stable_target_id}'"
            raise RuntimeError(msg)
        required_fields = [
            field
            for field in REQUIRED_FIELDS_BY_CLASS_NAME[f"Merged{rule_set.stemType}"]
            if field != "identifier"
        ]
        switched_off_fields = [
            field for field in required_fields if getattr(rule_set.preventive, field)
        ]

        if not switched_off_fields:
            logger.info(
                "[%s] item %s '%s' skipped because it has no switched off fields",
                index,
                rule_set.stemType,
                stable_target_id,
            )
            continue

        if not all(field in switched_off_fields for field in required_fields):
            logger.info(
                "[%s] note: not all required fields are switched off for %s '%s': "
                "switched off fields are %s.",
                index,
                rule_set.stemType,
                stable_target_id,
                switched_off_fields,
            )
            continue

        if not all(
            target in rule_set.workflow.forbiddenPublishingTarget
            for target in forbidden_targets
        ):
            msg = (
                f"all required fields are switched off but not all forbidden targets "
                f"are set for '{stable_target_id}'"
            )
            raise RuntimeError(msg)

        rule_set.preventive = rule_set.preventive.__class__.model_validate({})  # reset
        rule_set_request = transform_rule_set_response_to_request(rule_set)
        if dry_run:  # Dry run: don't call ingest_items.
            logger.info(
                "[%s] DRY RUN: would reset preventive rule for %s '%s'",
                index,
                rule_set.stemType,
                stable_target_id,
            )
        else:  # Real run: add missing targets and ingest
            update_and_get_rule_set(rule_set_request, rule_set.stableTargetId)
            logger.info(
                "[%s] success: preventive rule reset for %s '%s'",
                index,
                rule_set.stemType,
                stable_target_id,
            )

    logger.info(
        "migration reset_preventive_rule_for_switched_off_merged_items complete"
    )


def get_all_rule_set_ids() -> list[str]:
    """Get the stableTargetIds of all rule sets in the database."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_rule_items(
        query_string=None,
        identifier=None,
        entity_type=None,
        reference_filters=None,
        skip=0,
        limit=5000,
    )

    ids_merged_items_with_rules: set[str] = set()
    for item in graph_result.one()["items"]:
        stid = item["stableTargetId"][0]
        try:
            get_merged_item_from_graph(identifier=stid)
        except MergingError:  # only process if item can't be validated
            ids_merged_items_with_rules.add(stid)
    return sorted(ids_merged_items_with_rules)


def transform_rule_set_response_to_request(
    rule_set: AnyRuleSetResponse,
) -> AnyRuleSetRequest:
    """Transform a rule set response into a rule set request."""
    return RuleSetRequestTypeAdapter.validate_python(
        {
            **rule_set.model_dump(exclude={"stableTargetId"}),
            "entityType": rule_set.entityType.replace("Response", "Request"),
        }
    )


if __name__ == "__main__":
    reset_preventive_rule_for_switched_off_merged_items()
