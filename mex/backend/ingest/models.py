from typing import TYPE_CHECKING

from pydantic import ConfigDict, create_model

from mex.backend.extracted.models import AnyExtractedModel
from mex.common.models import EXTRACTED_MODEL_CLASSES_BY_NAME, BaseModel
from mex.common.types import Identifier


class _BaseBulkIngestRequest(BaseModel):
    """Request body for the bulk ingestion endpoint."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "ExtractedPerson": [
                        {
                            "hadPrimarySource": "000001111122222",
                            "identifierInPrimarySource": "jimmy",
                            "email": ["jimmy@example.com"],
                            "givenName": "Jimmy",
                            "memberOf": ["111112222233333"],
                        }
                    ],
                    "ExtractedContactPoint": [
                        {
                            "hadPrimarySource": "000001111122222",
                            "identifierInPrimarySource": "sales",
                            "email": ["sales@example.com"],
                        },
                        {
                            "hadPrimarySource": "000001111122222",
                            "identifierInPrimarySource": "hr",
                            "email": ["hr@example.com"],
                        },
                    ],
                }
            ]
        }
    )

    def get_all(self) -> list[AnyExtractedModel]:
        return [data for name in self.model_fields for data in getattr(self, name)]


if TYPE_CHECKING:  # pragma: no cover
    BulkIngestRequest = _BaseBulkIngestRequest
else:
    BulkIngestRequest = create_model(
        "BulkIngestRequest",
        __base__=_BaseBulkIngestRequest,
        __module__=__name__,
        **{
            name: (list[model], [])
            for name, model in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
        },
    )


class BulkIngestResponse(BaseModel):
    """Response body for the bulk ingestion endpoint."""

    identifiers: list[Identifier]
