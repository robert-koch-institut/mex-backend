from collections import defaultdict
from typing import Any, TypedDict

from pydantic import BaseModel as PydanticBaseModel

from mex.backend.extracted.models import AnyExtractedModel
from mex.backend.fields import REFERENCE_FIELDS_BY_CLASS_NAME
from mex.backend.graph.hydrate import dehydrate, hydrate
from mex.backend.transform import to_primitive
from mex.common.identity import Identity
from mex.common.models import EXTRACTED_MODEL_CLASSES_BY_NAME, BaseModel, MExModel


class MergableNode(PydanticBaseModel):
    """Helper class for merging nodes into the graph."""

    on_create: dict[str, str | list[str]]
    on_match: dict[str, str | list[str]]


def transform_model_to_node(model: BaseModel) -> MergableNode:
    """Transform a pydantic model into a node that can be merged into the graph."""
    raw_model = to_primitive(
        model,
        exclude=REFERENCE_FIELDS_BY_CLASS_NAME[model.__class__.__name__]
        | {"entityType"},
    )
    on_create = dehydrate(raw_model)

    on_match = on_create.copy()
    on_match.pop("identifier")
    on_match.pop("stableTargetId")
    on_match.pop("identifierInPrimarySource")

    return MergableNode(on_create=on_create, on_match=on_match)


class MergableEdge(PydanticBaseModel):
    """Helper class for merging edges into the graph."""

    label: str
    parameters: dict[str, Any]
    log_message: str


def transform_model_to_edges(model: MExModel) -> list[MergableEdge]:
    """Transform a model to a list of edges."""
    raw_model = to_primitive(
        model,
        exclude={"entityType"},
        include=REFERENCE_FIELDS_BY_CLASS_NAME[model.__class__.__name__],
    )
    # TODO: add support for link fields in nested dicts, eg. for rules
    edges = []
    for field, stable_target_ids in raw_model.items():
        if not isinstance(stable_target_ids, list):
            stable_target_ids = [stable_target_ids]
        from_id = str(model.identifier)
        for stable_target_id in stable_target_ids:
            stable_target_id = str(stable_target_id)
            parameters = {"fromID": from_id, "toSTI": stable_target_id}
            edges.append(
                MergableEdge(
                    label=field,
                    parameters=parameters,
                    log_message=f"({from_id})-[:{field}]→({stable_target_id})",
                )
            )
    return edges


class SearchResultReference(TypedDict):
    """Type definition for references returned by search query."""

    key: str  # label of the edge, e.g. parentUnit or hadPrimarySource
    value: list[str] | str  # stableTargetId of the referenced Node


def transform_search_result_to_model(
    search_result: dict[str, Any]
) -> AnyExtractedModel:
    """Transform a graph search result to an extracted item."""
    model_class_name: str = search_result["l"]
    flattened_dict: dict[str, Any] = search_result["n"]
    references: list[SearchResultReference] = search_result["r"]
    model_class = EXTRACTED_MODEL_CLASSES_BY_NAME[model_class_name]
    raw_model = hydrate(flattened_dict, model_class)

    # duplicate references can occur because we link
    # rule-sets and extracted-items, not merged-items
    deduplicated_references: dict[str, set[str]] = defaultdict(set)
    for reference in references:
        reference_ids = (
            reference["value"]
            if isinstance(reference["value"], list)
            else [reference["value"]]
        )
        deduplicated_references[reference["key"]].update(reference_ids)
    sorted_deduplicated_reference_key_values = {
        reference_type: sorted(reference_ids)
        for reference_type, reference_ids in deduplicated_references.items()
    }
    raw_model.update(sorted_deduplicated_reference_key_values)  # type: ignore[arg-type]

    return model_class.model_validate(raw_model)


def transform_identity_result_to_identity(identity_result: dict[str, Any]) -> Identity:
    """Transform the result from an identity query into an Identity instance."""
    return Identity.model_validate(identity_result["i"])
