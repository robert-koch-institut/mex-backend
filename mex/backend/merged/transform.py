import json

from neo4j.exceptions import Neo4jError

from mex.backend.graph.models import GraphResult
from mex.backend.graph.transform import transform_search_result_to_model
from mex.backend.merged.models import MergedItemSearchResponse
from mex.common.logging import logger
from mex.common.models import MERGED_MODEL_CLASSES_BY_NAME


def transform_graph_results_to_merged_item_search_response_facade(
    graph_results: list[GraphResult],
) -> MergedItemSearchResponse:
    """Transform graph results to extracted item search results.

    We just pretend they are merged items as a stopgap for MX-1382.

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
            model_dict = model.model_dump()
            # create a MergedModel class with the dictionary
            model_dict["entityType"] = f"Merged{model_dict['entityType']}"
            del model_dict["hadPrimarySource"]
            del model_dict["identifierInPrimarySource"]
            model_class = MERGED_MODEL_CLASSES_BY_NAME[model_dict["entityType"]]
            items.append(model_class.model_validate(model_dict))
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
    # TODO merge extracted items with rule set
    return MergedItemSearchResponse.model_validate({"items": items, "total": total})
