from typing import Any

from pydantic import BaseModel, Field

from mex.backend.fields import (
    REFERENCE_FIELDS_BY_CLASS_NAME,
)
from mex.backend.transform import to_primitive
from mex.common.models import EXTRACTED_MODEL_CLASSES_BY_NAME, AnyExtractedModel


class MergableEdge(BaseModel):
    """Helper class for merging edges into the graph."""

    label: str = Field(exclude=False)
    fromIdentifier: str
    toStableTargetId: str
    position: int


def transform_model_to_edges(model: AnyExtractedModel) -> list[MergableEdge]:
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
        for pos, stable_target_id in enumerate(stable_target_ids):
            edges.append(
                MergableEdge(
                    position=pos,
                    label=field,
                    fromIdentifier=str(model.identifier),
                    toStableTargetId=str(stable_target_id),
                )
            )
    return edges


def transform_search_result_to_model(node: dict[str, Any]) -> AnyExtractedModel:
    """Transform a graph search result to an extracted item."""
    node_dict = node.copy()
    refs = node_dict.pop("_refs")
    label = node_dict.pop("_label")

    for ref in refs:
        target_list = node_dict.setdefault(ref["label"], [None])
        length_needed = 1 + ref["position"] - len(target_list)
        target_list.extend([None] * length_needed)
        target_list[ref["position"]] = ref["value"]

    model_class = EXTRACTED_MODEL_CLASSES_BY_NAME[label]
    return model_class.model_validate(node_dict)
