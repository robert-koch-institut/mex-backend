import pytest

from mex.backend.exceptions import BackendError
from mex.backend.extracted.helpers import get_extracted_item_from_graph
from mex.common.models import AnyExtractedModel
from mex.common.types import Identifier


@pytest.mark.integration
def test_get_extracted_item_from_graph(
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    organization_1 = load_dummy_data["organization_1"]
    assert get_extracted_item_from_graph(organization_1.identifier) == organization_1


@pytest.mark.integration
def test_get_extracted_item_from_graph_not_found() -> None:
    with pytest.raises(BackendError, match="Extracted item was not found"):
        get_extracted_item_from_graph(Identifier("notARealIdentifier"))
