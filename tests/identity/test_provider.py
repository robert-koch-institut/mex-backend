from typing import Any
from unittest.mock import MagicMock

import pytest

from mex.backend.identity.provider import GraphIdentityProvider
from mex.common.exceptions import MExError
from mex.common.models import (
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
)
from mex.common.types import Identifier, PrimarySourceID


@pytest.mark.parametrize(
    ("mocked_return", "had_primary_source", "identifier_in_primary_source", "expected"),
    [
        (
            [],
            PrimarySourceID("psSti00000000001"),
            "new-item",
            {
                "hadPrimarySource": "psSti00000000001",
                "identifier": "bFQoRhcVH5DHUq",
                "identifierInPrimarySource": "new-item",
                "stableTargetId": "bFQoRhcVH5DHUr",
            },
        ),
        (
            [
                {
                    "i": {
                        "hadPrimarySource": "psSti00000000001",
                        "identifier": "cpId000000000002",
                        "identifierInPrimarySource": "existing-item",
                        "stableTargetId": "cpSti00000000002",
                    }
                }
            ],
            PrimarySourceID("psSti00000000001"),
            "existing-item",
            {
                "hadPrimarySource": "psSti00000000001",
                "identifier": "cpId000000000002",
                "identifierInPrimarySource": "existing-item",
                "stableTargetId": "cpSti00000000002",
            },
        ),
    ],
    ids=["new item", "existing item"],
)
def test_assign_identity_mocked(
    mocked_graph: MagicMock,
    mocked_return: list[dict[str, str]],
    had_primary_source: PrimarySourceID,
    identifier_in_primary_source: str,
    expected: dict[str, Any],
) -> None:
    mocked_graph.return_value = mocked_return
    provider = GraphIdentityProvider.get()
    identity = provider.assign(
        had_primary_source=had_primary_source,
        identifier_in_primary_source=identifier_in_primary_source,
    )
    assert identity.model_dump() == expected


def test_assign_identity_inconsistency_mocked(
    mocked_graph: MagicMock,
) -> None:
    mocked_graph.return_value = [
        {
            "i": {
                "hadPrimarySource": "psSti00000000001",
                "identifier": "cpId000000000002",
                "identifierInPrimarySource": "existing-item",
                "stableTargetId": "cpSti00000000002",
            }
        },
        {
            "i": {
                "hadPrimarySource": "psSti00000000001",
                "identifier": "cpId000000000098",
                "identifierInPrimarySource": "existing-item",
                "stableTargetId": "cpSti00000000099",
            }
        },
    ]
    provider = GraphIdentityProvider.get()
    with pytest.raises(MExError, match="graph inconsistency"):
        provider.assign(
            had_primary_source=PrimarySourceID("psSti00000000001"),
            identifier_in_primary_source="existing-item",
        )


@pytest.mark.parametrize(
    ("had_primary_source", "identifier_in_primary_source", "expected"),
    [
        (
            "bFQoRhcVH5DHUr",
            "new-item",
            {
                "hadPrimarySource": "bFQoRhcVH5DHUr",
                "identifier": "bFQoRhcVH5DHUC",
                "identifierInPrimarySource": "new-item",
                "stableTargetId": "bFQoRhcVH5DHUD",
            },
        ),
        (
            "bFQoRhcVH5DHUr",
            "cp-2",
            {
                "identifier": "bFQoRhcVH5DHUw",
                "hadPrimarySource": "bFQoRhcVH5DHUr",
                "identifierInPrimarySource": "cp-2",
                "stableTargetId": "bFQoRhcVH5DHUx",
            },
        ),
        (
            MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
            {
                "hadPrimarySource": MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                "identifier": MEX_PRIMARY_SOURCE_IDENTIFIER,
                "identifierInPrimarySource": MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
                "stableTargetId": MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            },
        ),
    ],
    ids=["new item", "existing contact point", "mex primary source"],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_assign_identity(
    had_primary_source: PrimarySourceID,
    identifier_in_primary_source: str,
    expected: dict[str, Any],
) -> None:
    provider = GraphIdentityProvider.get()
    identity = provider.assign(
        had_primary_source=had_primary_source,
        identifier_in_primary_source=identifier_in_primary_source,
    )
    assert identity.model_dump() == expected


@pytest.mark.parametrize(
    (
        "mocked_return",
        "had_primary_source",
        "identifier_in_primary_source",
        "stable_target_id",
        "expected",
    ),
    [
        ([], None, None, Identifier("thisDoesNotExist"), []),
        (
            [
                {
                    "i": {
                        "hadPrimarySource": "28282828282828",
                        "identifier": "7878787878787878777",
                        "identifierInPrimarySource": "one",
                        "stableTargetId": "949494949494949494",
                    }
                }
            ],
            PrimarySourceID("28282828282828"),
            "one",
            None,
            [
                {
                    "hadPrimarySource": "28282828282828",
                    "identifier": "7878787878787878777",
                    "identifierInPrimarySource": "one",
                    "stableTargetId": "949494949494949494",
                },
            ],
        ),
        (
            [
                {
                    "i": {
                        "hadPrimarySource": "28282828282828",
                        "identifier": "62626262626266262",
                        "identifierInPrimarySource": "two",
                        "stableTargetId": "949494949494949494",
                    }
                },
                {
                    "i": {
                        "hadPrimarySource": "39393939393939",
                        "identifier": "7878787878787878777",
                        "identifierInPrimarySource": "duo",
                        "stableTargetId": "949494949494949494",
                    }
                },
            ],
            None,
            None,
            Identifier("949494949494949494"),
            [
                {
                    "hadPrimarySource": "28282828282828",
                    "identifier": "62626262626266262",
                    "identifierInPrimarySource": "two",
                    "stableTargetId": "949494949494949494",
                },
                {
                    "hadPrimarySource": "39393939393939",
                    "identifier": "7878787878787878777",
                    "identifierInPrimarySource": "duo",
                    "stableTargetId": "949494949494949494",
                },
            ],
        ),
    ],
    ids=["nothing found", "one item", "two items"],
)
def test_fetch_identities_mocked(
    mocked_graph: MagicMock,
    mocked_return: list[dict[str, str]],
    had_primary_source: PrimarySourceID | None,
    identifier_in_primary_source: str | None,
    stable_target_id: Identifier | None,
    expected: list[dict[str, Any]],
) -> None:
    mocked_graph.return_value = mocked_return
    provider = GraphIdentityProvider.get()
    identities = provider.fetch(
        had_primary_source=had_primary_source,
        identifier_in_primary_source=identifier_in_primary_source,
        stable_target_id=stable_target_id,
    )
    assert [i.model_dump() for i in identities] == expected


@pytest.mark.parametrize(
    (
        "had_primary_source",
        "identifier_in_primary_source",
        "stable_target_id",
        "expected",
    ),
    [
        (None, None, Identifier("thisDoesNotExist"), []),
        (
            PrimarySourceID("00000000000000"),
            "ps-1",
            None,
            [
                {
                    "hadPrimarySource": "00000000000000",
                    "identifier": "bFQoRhcVH5DHUq",
                    "identifierInPrimarySource": "ps-1",
                    "stableTargetId": "bFQoRhcVH5DHUr",
                },
            ],
        ),
        (
            None,
            None,
            Identifier("bFQoRhcVH5DHUz"),
            [
                {
                    "identifier": "bFQoRhcVH5DHUy",
                    "hadPrimarySource": "bFQoRhcVH5DHUt",
                    "identifierInPrimarySource": "ou-1",
                    "stableTargetId": "bFQoRhcVH5DHUz",
                }
            ],
        ),
    ],
    ids=[
        "nothing found",
        "by hadPrimarySource and identifierInPrimarySource",
        "by stableTargetId",
    ],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_fetch_identities(
    had_primary_source: PrimarySourceID | None,
    identifier_in_primary_source: str | None,
    stable_target_id: Identifier | None,
    expected: list[dict[str, Any]],
) -> None:
    provider = GraphIdentityProvider.get()
    identities = provider.fetch(
        had_primary_source=had_primary_source,
        identifier_in_primary_source=identifier_in_primary_source,
        stable_target_id=stable_target_id,
    )
    assert [i.model_dump() for i in identities] == expected
