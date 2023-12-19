import pytest
from pydantic import BaseModel as PydanticBaseModel

from mex.backend.graph.hydrate import (
    NULL_VALUE,
    FlatDict,
    NestedDict,
    _get_base_model_from_field,
    are_instances,
    dehydrate,
    dehydrate_value,
    hydrate,
    hydrate_value,
)
from mex.common.models import BaseModel


def test_are_instances() -> None:
    assert are_instances(None, str) is False
    assert are_instances([], str) is True
    assert are_instances([3, 4], str) is False
    assert are_instances(["foo", "bar"], str) is True


def test_dehydrate_value() -> None:
    assert dehydrate_value(None) == NULL_VALUE
    assert dehydrate_value("foo") == "foo"
    assert dehydrate_value(["foo", None]) == ["foo", NULL_VALUE]
    with pytest.raises(TypeError):
        assert dehydrate_value([1.3, object()])  # type: ignore


def test_hydrate_value() -> None:
    assert hydrate_value(NULL_VALUE) is None
    assert hydrate_value("foo") == "foo"
    assert hydrate_value(["foo", NULL_VALUE]) == ["foo", None]
    with pytest.raises(TypeError):
        assert hydrate_value([1.3, object()])  # type: ignore


class Leaf(BaseModel):
    color: str | None
    veins: list[str | None]


class Branch(BaseModel):
    leaf: Leaf
    leaves: list[Leaf]


class Tree(BaseModel):
    branch: Branch
    branches: list[Branch]


class Caterpillar(BaseModel):
    home: Leaf | None


@pytest.mark.parametrize(
    ("model", "attribute", "expected"),
    [
        (Tree, "branch", Branch),
        (Branch, "leaves", Leaf),
        (Leaf, "color", "cannot hydrate paths with non base models"),
        (Leaf, "veins", "cannot hydrate paths with non base models"),
        (Caterpillar, "home", Leaf),
    ],
)
def test_get_base_model_from_field(
    model: type[BaseModel], attribute: str, expected: type[BaseModel] | str
) -> None:
    field = model.model_fields[attribute]
    if isinstance(expected, str):
        with pytest.raises(TypeError, match=expected):
            _get_base_model_from_field(field)
    else:
        assert _get_base_model_from_field(field) is expected


@pytest.mark.parametrize(
    ("nested", "flat", "model"),
    [
        ({}, {}, Leaf),
        (
            {"color": None, "veins": ["primary", None]},
            {
                "color": "",
                "veins": ["primary", ""],
            },
            Leaf,
        ),
        (
            {"color": "green", "veins": ["primary", "secondary"]},
            {
                "color": "green",
                "veins": ["primary", "secondary"],
            },
            Leaf,
        ),
        (
            {
                "leaf": {"color": None, "veins": [None, "secondary"]},
                "leaves": [{"color": "red"}, {"color": None}],
            },
            {
                "leaf_color": "",
                "leaf_veins": ["", "secondary"],
                "leaves_color": ["red", ""],
            },
            Branch,
        ),
        (
            {
                "leaf": {"color": "red", "veins": ["primary", "secondary"]},
                "leaves": [{"color": "red"}, {"color": "brown"}, {"color": "green"}],
            },
            {
                "leaf_color": "red",
                "leaf_veins": ["primary", "secondary"],
                "leaves_color": ["red", "brown", "green"],
            },
            Branch,
        ),
        (
            {
                "branch": {
                    "leaf": {"veins": [None, "secondary"]},
                    "leaves": [{"color": "red"}, {"color": None}],
                },
                "branches": [
                    {"leaf": {"color": None}},
                    {"leaf": {"color": "gold"}},
                ],
            },
            {
                "branch_leaf_veins": ["", "secondary"],
                "branch_leaves_color": ["red", ""],
                "branches_leaf_color": ["", "gold"],
            },
            Tree,
        ),
        (
            {
                "branch": {
                    "leaf": {"veins": ["primary", "secondary"]},
                    "leaves": [{"color": "red"}, {"color": "yellow"}],
                },
                "branches": [
                    {"leaf": {"color": "red"}},
                    {"leaf": {"color": "gold"}},
                    {"leaf": {"color": "yellow"}},
                ],
            },
            {
                "branch_leaf_veins": ["primary", "secondary"],
                "branch_leaves_color": ["red", "yellow"],
                "branches_leaf_color": ["red", "gold", "yellow"],
            },
            Tree,
        ),
    ],
    ids=[
        "leaf-empty",
        "leaf-sparse",
        "leaf-full",
        "branch-sparse",
        "branch-full",
        "tree-sparse",
        "tree-full",
    ],
)
def test_de_hydration_roundtrip(
    nested: NestedDict, flat: FlatDict, model: type[BaseModel]
) -> None:
    assert dehydrate(nested) == flat
    assert hydrate(flat, model) == nested


class NonMExModel(PydanticBaseModel):
    tree: Tree


@pytest.mark.parametrize(
    ("flat", "model", "error"),
    [
        (
            {"branch_leaf_color": "blue"},
            NonMExModel,
            "cannot hydrate paths with non base models",
        ),
        (
            {"veins": [42, object(), 1.3]},
            Leaf,
            "can only hydrate strings or lists of strings",
        ),
        (
            {
                "branches_leaves_color": ["red", "green"],
            },
            Branch,
            "flat dict does not align with target model",
        ),
        (
            {
                "branches_leaves_color": ["red", "green"],
                "branches_leaf_veins": ["primary", "secondary"],
            },
            Tree,
            "cannot handle multiple list branches",
        ),
        (
            {"leaves_color": "yellow"},
            Branch,
            "cannot hydrate non-list to list",
        ),
    ],
    ids=[
        "non base model",
        "disallowed objects",
        "model misalignment",
        "multiple list branches",
        "non list incompatibility",
    ],
)
def test_hydration_errors(flat: FlatDict, model: type[BaseModel], error: str) -> None:
    with pytest.raises(Exception, match=error):
        hydrate(flat, model)


@pytest.mark.parametrize(
    ("nested", "error"),
    [
        (
            {"leaves": [{"veins": ["primary"]}, {"veins": ["secondary"]}]},
            "can only handle one list per path",
        )
    ],
    ids=[
        "multiple lists per path",
    ],
)
def test_dehydration_errors(nested: NestedDict, error: str) -> None:
    with pytest.raises(Exception, match=error):
        assert dehydrate(nested)
