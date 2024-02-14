from typing import Any, Generator, TypedDict, cast

from mex.backend.fields import REFERENCE_FIELDS_BY_CLASS_NAME
from mex.backend.transform import to_primitive
from mex.common.models import AnyExtractedModel
from mex.common.transform import to_key_and_values


class MergeEdgeParameters(TypedDict):
    """Helper class for merging edges into the graph."""

    source_node: str  # the node from which the edge starts
    target_node: str  # the node to which the edge leads
    position: int  # the order in a list for labels with multiple edges


def transform_model_to_labels_and_parameters(
    model: AnyExtractedModel,
) -> Generator[tuple[str, MergeEdgeParameters], None, None]:
    """Transform a model into tuples of edge labels and parameters for the merge query.

    All reference fields except `stableTargetId` are converted to label and parameters.
    """
    ref_fields = REFERENCE_FIELDS_BY_CLASS_NAME[model.entityType]
    raw_model = to_primitive(
        model,
        include=set(ref_fields) | {"stableTargetId"},  # we add this during node-merge
    )
    for field, stable_target_ids in to_key_and_values(raw_model):
        for position, stable_target_id in enumerate(stable_target_ids):
            yield field, MergeEdgeParameters(
                position=position,
                source_node=str(model.identifier),
                target_node=str(stable_target_id),
            )


class SearchResultReference(TypedDict):
    """Helper class to show the structure of search result references."""

    label: str  # corresponds to the field name of the pydantic model
    position: int  # if the field in pydantic is a list this helps keep its order
    value: str | dict[str, str | None]  # this can be a raw Identifier, Text or Link


def expand_references_in_search_result(item: dict[str, Any]) -> None:
    """Expand the `_refs` collection in a search result item.

    Each item in a search result has a collection of `_refs` in the form of
    `SearchResultReference`. Before parsing them into pydantic, we need to inline
    the references back into the `item` dictionary.
    """
    # XXX if we can use `apoc`, we might do this step directly in the cypher query
    for ref in cast(list[SearchResultReference], item.pop("_refs")):
        target_list = item.setdefault(ref["label"], [None])
        length_needed = 1 + ref["position"] - len(target_list)
        target_list.extend([None] * length_needed)
        target_list[ref["position"]] = ref["value"]
