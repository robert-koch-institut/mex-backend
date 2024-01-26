from mex.backend.graph.models import GraphResult
from mex.backend.graph.transform import transform_search_result_to_model
from mex.backend.merged.models import MergedItemSearchResponse
from mex.common.models import MERGED_MODEL_CLASSES_BY_NAME


def transform_graph_result_to_merged_item_search_response_facade(
    graph_result: GraphResult,
) -> MergedItemSearchResponse:
    """Transform graph result to extracted item search results.

    We just pretend they are merged items as a stopgap for MX-1382.

    Args:
        graph_result: Results of a search and a count query

    Returns:
        Search response instance
    """
    total = graph_result.data[0]["total"]
    items = []
    for result in graph_result.data[0]["items"]:
        model = transform_search_result_to_model(result)
        model_dict = model.model_dump(
            exclude={"hadPrimarySource", "identifierInPrimarySource"}
        )
        # create a MergedModel class with the dictionary
        model_dict["entityType"] = model_dict["entityType"].replace(
            "Extracted", "Merged"
        )
        model_class = MERGED_MODEL_CLASSES_BY_NAME[model_dict["entityType"]]
        items.append(model_class.model_validate(model_dict))

    # TODO: merge extracted items with rule sets
    return MergedItemSearchResponse.model_validate({"items": items, "total": total})
