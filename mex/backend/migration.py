import re
import shutil
import subprocess
from collections import deque
from typing import TYPE_CHECKING, Any

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.backend.migrate_merged_items_to_workflow_rule import (
    merge_preview_items_with_all_required_fields_missing,
)
from mex.common.exceptions import MExError
from mex.common.logging import logger
from mex.common.models import (
    RULE_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_TYPE_BY_NAME,
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyRuleModel,
    AnyRuleSetResponse,
)
from mex.common.transform import ensure_postfix

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator, Sequence


MAX_MIGRATION_VERSION = "4.1.0"


def get_current_git_tag() -> str:
    """Get the current git tag from the current HEAD commit."""
    try:
        git_executable = shutil.which("git")
        if git_executable is None:
            msg = "Git executable not found on PATH."
            raise RuntimeError(msg)

        output = subprocess.run(  # noqa: S603
            [git_executable, "describe", "--tags", "HEAD"],
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        if match := re.search(r"(\d+\.\d+\.\d+)", output):
            return match.group(1)
        msg = "Couldn't deduce current git tag"
        raise MExError(msg)
    except subprocess.CalledProcessError as error:
        raise MExError from error


def add_workflow_rule_to_all_rule_sets_on_db() -> None:
    """Add workflow rule to all rule sets in database."""
    connector = GraphConnector.get()

    number_of_rules_after_migration = 4

    for raw_rules in get_all_raw_rules(connector):
        if len(raw_rules) == number_of_rules_after_migration:
            continue
        rule_set = transform_three_raw_rules_to_rule_set_response(raw_rules)

        deque(connector.ingest_items([rule_set]))
    logger.info("migration add_workflow_rule_to_all_rule_sets_on_db complete")


def get_all_raw_rules(connector: GraphConnector) -> Generator[list[dict[str, Any]]]:
    """Get all raw rules in database."""
    for item in get_all_merged_item_results(connector):
        raw_rules = [
            component
            for component in item["_components"]
            if component["entityType"] in RULE_MODEL_CLASSES_BY_NAME
        ]
        if raw_rules:
            yield raw_rules


def get_all_merged_item_results(connector: GraphConnector) -> Generator[dict[str, Any]]:
    """Get all merged items from database."""
    result = connector.fetch_merged_items(
        query_string=None,
        identifier=None,
        entity_type=None,
        reference_filters=None,
        skip=0,
        limit=1,
    )
    total_item_number = result["total"]
    item_number_limit = 100  # 100 is the maximum possible number per get-request
    for item_counter in range(0, total_item_number, item_number_limit):
        result = connector.fetch_merged_items(
            query_string=None,
            identifier=None,
            entity_type=None,
            reference_filters=None,
            skip=item_counter,
            limit=item_number_limit,
        )
        yield from result["items"]


def transform_three_raw_rules_to_rule_set_response(
    raw_rules: Sequence[dict[str, Any]],
) -> AnyRuleSetResponse:
    """Transform a set of plain rules into a rule set response."""
    stem_types: list[str] = []
    stable_target_ids: list[str] = []
    response: dict[str, Any] = {}
    model: type[AnyRuleModel] | None

    number_of_rules_before_migration = 3

    if (num_raw_rules := len(raw_rules)) != number_of_rules_before_migration:
        msg = f"inconsistent number of rules found: {num_raw_rules}, expected 3."
        raise InconsistentGraphError(msg)

    for rule in raw_rules:
        for (
            field_name,
            model_class_lookup,
        ) in RULE_MODEL_CLASSES_BY_TYPE_BY_NAME.items():
            if model := model_class_lookup.get(str(rule.get("entityType"))):
                response[field_name] = rule
                stem_types.append(model.stemType)
                stable_target_ids.extend(rule.pop("stableTargetId", []))

    if len(set(stem_types)) != 1:
        msg = "inconsistent rule item stem types"
        raise InconsistentGraphError(msg)
    if len(set(stable_target_ids)) != 1:
        msg = f"inconsistent rule item stableTargetIds: {', '.join(stable_target_ids)}"
        raise InconsistentGraphError(msg)

    response["stableTargetId"] = stable_target_ids[0]
    response_class_name = ensure_postfix(stem_types[0], "RuleSetResponse")
    response_class = RULE_SET_RESPONSE_CLASSES_BY_NAME[response_class_name]
    return response_class.model_validate(response)  # here an empty workflow rule is set


def migrate() -> None:
    """Run migrations."""
    if get_current_git_tag() <= MAX_MIGRATION_VERSION:
        add_workflow_rule_to_all_rule_sets_on_db()
        merge_preview_items_with_all_required_fields_missing()
