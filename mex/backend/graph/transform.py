from typing import Any, Generator, TypedDict

from mex.backend.fields import REFERENCE_FIELDS_BY_CLASS_NAME
from mex.backend.transform import to_primitive
from mex.common.models import AnyExtractedModel


class MergeEdgeParameters(TypedDict):
    """Helper class for merging edges into the graph."""

    source_node: str
    target_node: str
    position: int


def transform_model_to_edges(
    model: AnyExtractedModel,
) -> Generator[tuple[str, MergeEdgeParameters], None, None]:
    ref_fields = REFERENCE_FIELDS_BY_CLASS_NAME[model.entityType]
    raw_model = to_primitive(
        model,
        include=set(ref_fields) | {"stableTargetId"},  # we add this during node-merge
    )
    # TODO: add support for link fields in nested dicts, eg. for rules

    for field, stable_target_ids in raw_model.items():
        if stable_target_ids is None:
            continue
        if not isinstance(stable_target_ids, list):
            stable_target_ids = [stable_target_ids]
        for pos, stable_target_id in enumerate(stable_target_ids):
            yield field, MergeEdgeParameters(
                position=pos,
                source_node=str(model.identifier),
                target_node=str(stable_target_id),
            )


def transform_search_result_to_model(node_dict: dict[str, Any]) -> None:
    # TODO rename to highlight inplace-ness
    for ref in node_dict.pop("_refs"):
        target_list = node_dict.setdefault(ref["label"], [None])
        length_needed = 1 + ref["position"] - len(target_list)
        target_list.extend([None] * length_needed)
        target_list[ref["position"]] = ref["value"]
