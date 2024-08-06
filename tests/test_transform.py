from mex.backend.serialization import to_primitive
from mex.common.types import APIType, MergedActivityIdentifier, YearMonth


def test_to_primitive_uses_custom_encoders() -> None:
    primitive = to_primitive(
        dict(
            api=APIType["RPC"],
            activity=MergedActivityIdentifier.generate(seed=99),
            month=YearMonth(2005, 11),
        )
    )
    assert primitive == {
        "api": "https://mex.rki.de/item/api-type-5",
        "activity": "bFQoRhcVH5DHV1",
        "month": "2005-11",
    }
