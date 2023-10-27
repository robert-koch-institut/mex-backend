from mex.backend.models import TypeSerializer


class DummyModel(TypeSerializer):
    foo: str


def test_type_serializer_to_dict() -> None:
    dummy = DummyModel(foo="foo")

    assert dummy.dict() == {"foo": "foo", "$type": "DummyModel"}
    assert dummy.dict(include={"foo"}) == {"foo": "foo", "$type": "DummyModel"}
    assert dummy.dict(include={"foo", "$type"}) == {"foo": "foo", "$type": "DummyModel"}
    assert dummy.dict(exclude={"foo"}) == {"$type": "DummyModel"}
    assert dummy.dict(exclude={"foo", "$type"}) == {}
    assert dummy.dict(exclude={"$type"}) == {"foo": "foo"}
