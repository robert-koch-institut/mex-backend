from mex.backend.graph.transform import transform_identity_result_to_identity
from mex.common.identity import Identity
from mex.common.types import Identifier, PrimarySourceID


def test_transform_identity_result_to_identity() -> None:
    assert transform_identity_result_to_identity(
        {
            "i": {
                "identifier": "90200009120910",
                "hadPrimarySource": "7827287287287287",
                "identifierInPrimarySource": "one",
                "stableTargetId": "6536536536536536536536",
            }
        }
    ) == Identity(
        identifier=Identifier("90200009120910"),
        hadPrimarySource=PrimarySourceID("7827287287287287"),
        identifierInPrimarySource="one",
        stableTargetId=Identifier("6536536536536536536536"),
    )
