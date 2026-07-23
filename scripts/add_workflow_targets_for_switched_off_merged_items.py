from copy import deepcopy

import click

from mex.backend.graph.connector import GraphConnector
from mex.common.backend_api.connector import BackendApiConnector
from mex.common.fields import (
    REQUIRED_FIELDS_BY_CLASS_NAME,
)
from mex.common.logging import logger
from mex.common.models import (
    MERGED_MODEL_CLASSES,
    RuleSetRequestTypeAdapter,
)
from mex.common.types import PublishingTarget


def add_workflow_targets_for_switched_off_merged_items(*, dry_run: bool) -> None:
    """Set forbidden publishing targets for "switched off" merged items.

    Finds items which exists as preview item but not as merged item (i.e. required
    fields are empty). Checks if all or only some required fields are empty. For items
    with all required fields switched-off, a workflow rule with forbidden publishing
    targets is created.
    """
    connector_backend = BackendApiConnector.get()

    forbidden_targets = [PublishingTarget.INVENIO, PublishingTarget.DATENKOMPASS]

    logger.info(
        "### MIGRATION add_workflow_targets_for_switched_off_merged_items START ###"
    )
    if dry_run:
        logger.info("\nDRY RUN - no writing to data base\n")

    for merged_class in MERGED_MODEL_CLASSES:
        if merged_class.stemType in ["PrimarySource", "Person"]:
            continue  # these models don't have required fields
        merged_class_name = merged_class.__name__

        connector = BackendApiConnector.get()
        graph_connector = GraphConnector.get()
        graph_result = graph_connector.fetch_rule_items(
            query_string=None,
            identifier=None,
            entity_type=None,
            reference_filters=None,
            skip=0,
            limit=4000,
        )
        items = graph_result.one()["items"]
        items_with_rule_ids = sorted({item["stableTargetId"][0] for item in items})

        if not items_with_rule_ids:
            logger.info(f"Migrating 0 {merged_class_name}")
            continue

        logger.info(f"Migrating {len(items_with_rule_ids)} {merged_class_name}")

        required_fields = [
            x
            for x in REQUIRED_FIELDS_BY_CLASS_NAME[merged_class_name]
            if x != "identifier"
        ]
        for stid in items_with_rule_ids:
            rule_set = connector_backend.get_rule_set(stable_target_id=stid)
            collected_fields_per_item = [
                field
                for field in required_fields
                if getattr(rule_set.preventive, field)
            ]

            if not collected_fields_per_item:
                logger.info(f"item : {merged_class_name} '{stid}'")
                continue

            if not all(field in collected_fields_per_item for field in required_fields):
                logger.info(
                    f"---- step - note: not all required fields are switched "
                    f"off for {merged_class_name} '{stid}': switched off fields "
                    f"are {collected_fields_per_item}."
                )
                continue

            targets_to_add = [
                target
                for target in forbidden_targets
                if target not in rule_set.workflow.forbiddenPublishingTarget
            ]
            if not targets_to_add:
                logger.info(
                    f"---- step - skipped: workflow rule already fully "
                    f"populated for {merged_class_name} '{stid}'"
                )
                continue
            if dry_run:  # Dry run: don't call ingest_items.
                simulated = deepcopy(rule_set)  # avoid mutating real objects
                simulated.workflow.forbiddenPublishingTarget.extend(targets_to_add)
                logger.info(
                    f"---- DRY RUN: would set workflow for {merged_class_name} "
                    f"'{stid}' to {[t.name for t in targets_to_add]}"
                )
            else:  # Real run: add missing targets and ingest
                rule_set.workflow.forbiddenPublishingTarget.extend(targets_to_add)
                rule_set_request = RuleSetRequestTypeAdapter.validate_python(
                    {
                        **rule_set.model_dump(exclude={"stableTargetId"}),
                        "entityType": rule_set.entityType.replace(
                            "Response", "Request"
                        ),
                    }
                )
                connector_backend.update_rule_set(
                    rule_set.stableTargetId, rule_set_request
                )
                logger.info(
                    f"---- step - success: workflow for {merged_class_name}"
                    f"'{stid}' set as {[t.name for t in targets_to_add]}"
                )

    logger.info(
        "### MIGRATION add_workflow_targets_for_switched_off_merged_items COMPLETE ###"
    )


@click.command()
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    help="Default: dry run (no writing to data base). Use '--no-dry-run' for real run.",
)
def main(*, dry_run: bool) -> None:
    """Set '--no-dry-run' for actually populating the writing workflow rule."""
    add_workflow_targets_for_switched_off_merged_items(dry_run=dry_run)


if __name__ == "__main__":
    main()
