import json

from neo4j.exceptions import Neo4jError

from mex.backend.extracted.models import ExtractedItemSearchResponse
from mex.backend.graph.models import GraphResult
from mex.backend.graph.transform import transform_search_result_to_model
from mex.common.logging import logger


def transform_graph_results_to_extracted_item_search_response(
    graph_results: list[GraphResult],
) -> ExtractedItemSearchResponse:
    """Transform graph results to extracted item search results.

    Args:
        graph_results: Results of a search and a count query

    Returns:
        Search response instance
    """
    search_result, count_result = graph_results
    total = count_result.data[0]["c"]
    items = []
    for result in search_result.data:
        try:
            model = transform_search_result_to_model(result)
            items.append(model)
        except Neo4jError as error:  # pragma: no cover
            logger.exception(
                "%s\n__node__\n  %s\n__refs__\n%s\n",
                error,
                "  \n".join(
                    "{}: {}".format(k, json.dumps(v, separators=(",", ":")))
                    for k, v in result["n"].items()
                ),
                "  \n".join("()-[{key}]->({value})".format(**r) for r in result["r"]),
                exc_info=False,
            )
    return ExtractedItemSearchResponse.construct(items=items, total=total)
