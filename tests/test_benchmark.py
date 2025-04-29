import json

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.helpers import EXTRACTED_MODEL_ADAPTER
from mex.backend.settings import BackendSettings

Payload = dict[str, list[dict[str, Any]]]
from mex.common.backend_api.connector import BackendApiConnector


def test_bulk_insert() -> None:
    files = [
        r"C:\Users\esinsj\code\mex-extractors\ExtractedPrimarySource.ndjson",
        r"C:\Users\esinsj\code\mex-extractors\ExtractedOrganization.ndjson",
        r"C:\Users\esinsj\code\mex-extractors\ExtractedOrganizationalUnit.ndjson",
        r"C:\Users\esinsj\code\mex-extractors\ExtractedResource.ndjson",
        r"C:\Users\esinsj\code\mex-extractors\ExtractedVariableGroup.ndjson",
    ]
    settings = BackendSettings.get()

    connector = BackendApiConnector.get()
    graph_connector = GraphConnector.get()
    graph_connector.flush()
    graph_connector.close()
    for f in files:
        with open(f) as fh:
            post_payload = []
            for l in fh:
                post_payload.append(
                    EXTRACTED_MODEL_ADAPTER.validate_python(json.loads(l))
                )

            # post the dummy data to the ingest endpoint
            connector.ingest(post_payload)
    connector.close()


if __name__ == "__main__":
    test_bulk_insert()
