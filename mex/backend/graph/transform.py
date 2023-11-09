from typing import Any

from pydantic import BaseModel as PydanticBaseModel

from mex.backend.extracted.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    AnyExtractedModel,
)
from mex.backend.fields import REFERENCE_FIELDS_BY_CLASS_NAME
from mex.backend.graph.hydrate import dehydrate, hydrate
from mex.backend.transform import to_primitive
from mex.common.identity import Identity
from mex.common.models import BaseModel, MExModel


class MergableNode(PydanticBaseModel):
    """Helper class for merging nodes into the graph."""

    on_create: dict[str, str | list[str]]
    on_match: dict[str, str | list[str]]


def transform_model_to_node(model: BaseModel) -> MergableNode:
    """Transform a pydantic model into a node that can be merged into the graph."""
    raw_model = to_primitive(
        model,
        exclude=REFERENCE_FIELDS_BY_CLASS_NAME[model.__class__.__name__] | {"$type"},
    )
    on_create = dehydrate(raw_model)

    on_match = on_create.copy()
    on_match.pop("identifier")
    on_match.pop("stableTargetId")

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
        exclude={"$type"},
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


def transform_search_result_to_model(
    search_result: dict[str, Any]
) -> AnyExtractedModel:
    """Transform a graph search result to an extracted item."""
    label = search_result["l"]
    node = search_result["n"]
    refs = search_result["r"]
    model_class = EXTRACTED_MODEL_CLASSES_BY_NAME[label]
    raw_model = hydrate(node, model_class)

    # duplicate references can occur because we link
    # rule-sets and extracted-items, not merged-items
    for key, value in ((r["key"], r["value"]) for r in refs):
        if not isinstance(value, list):
            value = [value]
        raw_model[key] = sorted(set(raw_model.get(key, [])) | set(value))  # type: ignore[arg-type,type-var]

    return model_class.parse_obj(raw_model)


def transform_identity_result_to_identity(identity_result: dict[str, Any]) -> Identity:
    """Transform the result from an identity query into an Identity instance."""
    return Identity.parse_obj(identity_result["n"])
