from mex.backend.extracted.models import ExtractedItemSearchResponse
from mex.backend.graph.models import GraphResult
from mex.backend.graph.transform import transform_search_result_to_model


def transform_graph_result_to_extracted_item_search_response(
    graph_result: GraphResult,
) -> ExtractedItemSearchResponse:
    """Transform graph result to extracted item search results.

    Args:
        graph_result: Results of a search and a count query

    Returns:
        Search response instance
    """
    total = graph_result.data[0]["total"]
    items = []
    for result in graph_result.data[0]["items"]:
        model = transform_search_result_to_model(result)
        items.append(model)
    return ExtractedItemSearchResponse.model_construct(items=items, total=total)
