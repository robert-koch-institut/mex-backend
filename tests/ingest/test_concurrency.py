"""Behavioral tests for the concurrency and cancellation properties of `/v0/ingest`.

The ingest endpoint advances a blocking neo4j generator in a worker thread, so that
long ingests keep the event loop free, stop when the client goes away and stop when
uvicorn shuts down. These tests cover those three properties.
"""

import contextlib
import logging
import socket
import threading
import time
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, Mock

import anyio
import httpx
import pytest
import uvicorn
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.main import app
from mex.common.logging import logger

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator

    from pytest import LogCaptureFixture, MonkeyPatch
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    from tests.conftest import MockedGraph

DUMMY_IDENTITY = {
    "hadPrimarySource": "28282828282828",
    "identifier": "7878787878787878777",
    "identifierInPrimarySource": "one",
    "stableTargetId": "949494949494949494",
}


@pytest.fixture
def anyio_backend() -> str:
    """Run `@pytest.mark.anyio` tests on asyncio."""
    return "asyncio"


def extracted_contact_points(count: int) -> list[dict[str, Any]]:
    """Return a batch of valid extracted contact points to post to `/v0/ingest`."""
    return [
        {
            "$type": "ExtractedContactPoint",
            "hadPrimarySource": "00000000000000",
            "identifierInPrimarySource": f"cp-{index}",
            "email": [f"{index}@test.tld"],
        }
        for index in range(count)
    ]


def mock_ingest_items(
    *,
    item: Mock,
    started: threading.Event,
    closed: threading.Event,
) -> MagicMock:
    """Return a stand-in for `GraphConnector.ingest_items`.

    `item` is called once per consumed item, so its `side_effect` is the per-item hook
    and its `call_count` is how far the ingest got. `started` is set as soon as the
    first item is consumed, `closed` once the generator is closed or exhausted.

    The returned mock keeps a reference to the generator it handed out. That matters:
    without one, refcounting closes the generator as soon as the endpoint's frame goes
    away, which would mask an endpoint that never closes it explicitly.
    """
    mock = MagicMock()

    def generate(models: list[object]) -> Generator[None]:
        try:
            for _ in models:
                started.set()
                item()
                yield None
        except GeneratorExit:
            closed.set()
            raise
        closed.set()

    def ingest_items(models: list[object]) -> Generator[None]:
        generator = generate(models)
        mock.generator = generator
        return generator

    mock.side_effect = ingest_items
    return mock


@pytest.mark.anyio
async def test_ingest_does_not_block_other_requests(
    mocked_graph: MockedGraph,
    monkeypatch: MonkeyPatch,
) -> None:
    # both requests are driven through one ASGI transport, so they share a single
    # event loop. a `TestClient` would open a separate portal per call and could not
    # tell a non-blocking endpoint from a blocking one.
    mocked_graph.return_value = [DUMMY_IDENTITY]
    started = threading.Event()
    closed = threading.Event()
    release = threading.Event()
    items_to_ingest = 3

    def block_until_probed() -> None:
        # this runs in a worker thread. if the ingest were blocking the event loop,
        # the probe below could never run and never release us.
        assert release.wait(5), "event loop was blocked by the running ingest"

    item = Mock(side_effect=block_until_probed)
    monkeypatch.setattr(
        GraphConnector,
        "ingest_items",
        mock_ingest_items(item=item, started=started, closed=closed),
    )

    responses: dict[str, httpx.Response] = {}
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:

        async def ingest() -> None:
            responses["ingest"] = await client.post(
                "/v0/ingest",
                json={"items": extracted_contact_points(items_to_ingest)},
                headers={"X-API-Key": "write_key"},
            )

        async def probe() -> None:
            # wait for the ingest to be under way, without blocking the event loop
            assert await anyio.to_thread.run_sync(started.wait, 5)
            responses["probe"] = await client.get(
                "/v0/identity",
                params={"stableTargetId": DUMMY_IDENTITY["stableTargetId"]},
                headers={"X-API-Key": "read_key"},
            )
            release.set()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(ingest)
            task_group.start_soon(probe)

    assert responses["probe"].status_code == status.HTTP_200_OK, responses["probe"].text
    assert responses["probe"].json() == {"items": [DUMMY_IDENTITY], "total": 1}
    assert responses["ingest"].status_code == status.HTTP_204_NO_CONTENT
    assert item.call_count == items_to_ingest


def disconnect_when_set(app: ASGIApp, disconnected: threading.Event) -> ASGIApp:
    """Wrap an ASGI app so it reports a client disconnect once `disconnected` is set."""

    async def wrapper(scope: Scope, receive: Receive, send: Send) -> None:
        async def patched_receive() -> Message:
            if disconnected.is_set():
                return {"type": "http.disconnect"}
            return await receive()

        await app(scope, patched_receive, send)

    return wrapper


@pytest.mark.anyio
@pytest.mark.usefixtures("mocked_graph")
async def test_ingest_stops_on_client_disconnect(
    monkeypatch: MonkeyPatch,
    caplog: LogCaptureFixture,
) -> None:
    # `httpx.ASGITransport` never emits an `http.disconnect` while the endpoint is
    # still running, so the wrapper above injects one into the receive channel.
    started = threading.Event()
    closed = threading.Event()
    disconnected = threading.Event()

    def disconnect_after_two_items() -> None:
        # a mock counts the call before running its side effect, so this fires while
        # the second item is being ingested
        if item.call_count == 2:
            disconnected.set()

    item = Mock(side_effect=disconnect_after_two_items)
    monkeypatch.setattr(
        GraphConnector,
        "ingest_items",
        mock_ingest_items(item=item, started=started, closed=closed),
    )

    transport = httpx.ASGITransport(app=disconnect_when_set(app, disconnected))

    with caplog.at_level(logging.WARNING, logger=logger.name):
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            response = await client.post(
                "/v0/ingest",
                json={"items": extracted_contact_points(50)},
                headers={"X-API-Key": "write_key"},
            )

    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert item.call_count == 2, "ingest continued after the client disconnected"
    assert closed.is_set(), "generator was not closed, neo4j session leaks"
    assert "client disconnected after 2 items were ingested" in caplog.text


@pytest.mark.usefixtures("mocked_graph")
def test_ingest_stops_on_server_shutdown(monkeypatch: MonkeyPatch) -> None:
    # a real uvicorn server is needed here: it cancels in-flight request tasks with a
    # raw `asyncio.Task.cancel()` after the graceful shutdown timeout, which an
    # anyio-level cancellation around the ASGI app would not reproduce.
    started = threading.Event()
    closed = threading.Event()
    item = Mock(side_effect=lambda: time.sleep(0.02))
    monkeypatch.setattr(
        GraphConnector,
        "ingest_items",
        mock_ingest_items(item=item, started=started, closed=closed),
    )

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, log_config=None, timeout_graceful_shutdown=1)
    )
    server_thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True
    )
    server_thread.start()
    try:
        while not server.started:
            time.sleep(0.01)

        def post_long_ingest() -> None:
            # the server goes away mid-request, so any transport error is expected
            with (
                httpx.Client(base_url=f"http://127.0.0.1:{port}") as client,
                contextlib.suppress(httpx.HTTPError),
            ):
                client.post(
                    "/v0/ingest",
                    json={"items": extracted_contact_points(5000)},
                    headers={"X-API-Key": "write_key"},
                    timeout=30,
                )

        client_thread = threading.Thread(target=post_long_ingest, daemon=True)
        client_thread.start()

        assert started.wait(10), "ingest never started"
        time.sleep(0.2)  # let a few items through before pulling the rug
        server.should_exit = True
        server_thread.join(30)
        client_thread.join(30)
    finally:
        server.should_exit = True
        server_thread.join(30)

    consumed_at_shutdown = item.call_count
    time.sleep(0.5)

    assert item.call_count == consumed_at_shutdown, "ingest continued past shutdown"
    assert consumed_at_shutdown < 5000, "ingest was not interrupted at all"
    assert closed.wait(5), "generator was not closed, neo4j session leaks"
