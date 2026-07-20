from collections import deque
from copy import deepcopy
from typing import TYPE_CHECKING

import click

from mex.backend.graph.connector import GraphConnector
from mex.common.backend_api.connector import BackendApiConnector
from mex.common.fields import (
    REQUIRED_FIELDS_BY_CLASS_NAME,
)
from mex.common.logging import logger
from mex.common.models import (
    MERGED_MODEL_CLASSES,
    AnyPreviewModel,
)
from mex.common.types import PublishingTarget

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator


def _search_all_preview_items(
    entity_type: list[str],
) -> Generator[AnyPreviewModel]:
    """Fetch all preview items from db (handling limit of 100 per request)."""
    connector = BackendApiConnector.get()
    response = connector.fetch_preview_items(entity_type=entity_type, limit=1)
    total_item_number = response.total
    item_number_limit = 100
    for item_counter in range(0, total_item_number, item_number_limit):
        response = connector.fetch_preview_items(
            entity_type=entity_type,
            skip=item_counter,
            limit=item_number_limit,
        )
        yield from response.items


def _find_broken_item_ids(
    connector_backend: BackendApiConnector, merged_class_name: str
) -> list[str]:
    """Find merged items which are 'broken' (at least 1 required field switched off)."""
    merged_items = connector_backend.fetch_all_merged_items(
        entity_type=[merged_class_name]
    )
    merged_item_ids = {str(x.identifier) for x in merged_items}

    preview_items = _search_all_preview_items(entity_type=[merged_class_name])
    preview_items_ids = {str(x.identifier) for x in preview_items}

    return list(preview_items_ids - merged_item_ids)


def add_workflow_targets_for_switched_off_merged_items(*, dry_run: bool) -> None:
    """Set forbidden publishing targets for "switched off" merged items.

    Finds items which exists as preview item but not as merged item (i.e. required
    fields are empty). Checks if all or only some required fields are empty. For items
    with all required fields switched-off, a workflow rule with forbidden publishing
    targets is created.
    """
    connector_graph = GraphConnector.get()
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

        broken_item_ids = _find_broken_item_ids(connector_backend, merged_class_name)
        if not broken_item_ids:
            logger.info(f"Migrating 0 {merged_class_name}")
            continue

        logger.info(f"Migrating {len(broken_item_ids)} {merged_class_name}")

        required_fields = [
            x
            for x in REQUIRED_FIELDS_BY_CLASS_NAME[merged_class_name]
            if x != "identifier"
        ]
        for stid in broken_item_ids:
            rule_set = connector_backend.get_rule_set(stable_target_id=stid)
            collected_fields_per_item = [
                field
                for field in required_fields
                if getattr(rule_set.preventive, field)
            ]

            if not collected_fields_per_item:
                logger.info(
                    f"---- step - possible bug: item is broken and should be "
                    f"checked: {merged_class_name} '{stid}'"
                )
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
                deque(connector_graph.ingest_items([rule_set]))
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
