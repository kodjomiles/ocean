"""Tests for reading and saving incremental sync cursors through the Port API."""

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from port_ocean.clients.port.mixins.integrations import IntegrationClientMixin

API_URL = "https://api.getport.test/v1"
CURSOR_URL = f"{API_URL}/integration/my-integration/cursor"
CURSOR = datetime(2026, 3, 8, 17, 45, tzinfo=timezone.utc)
AUTH = "Bearer token"

Handler = Callable[[httpx.Request], httpx.Response]


class RecordingTransport(httpx.MockTransport):
    def __init__(self, handler: Handler) -> None:
        self.requests: list[httpx.Request] = []

        def record(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            return handler(request)

        super().__init__(record)

    @property
    def methods(self) -> list[str]:
        return [request.method for request in self.requests]


def respond_by_method(**responses: tuple[int, Any]) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        status, body = responses[request.method.lower()]
        return httpx.Response(status, json=body)

    return handler


def make_client(handler: Handler) -> tuple[IntegrationClientMixin, RecordingTransport]:
    transport = RecordingTransport(handler)
    auth = MagicMock()
    auth.api_url = API_URL
    auth.headers = AsyncMock(return_value={"Authorization": AUTH})
    client = IntegrationClientMixin(
        integration_identifier="my-integration",
        integration_version="1.0.0",
        auth=auth,
        client=httpx.AsyncClient(transport=transport),
    )
    return client, transport


def query(request: httpx.Request) -> dict[str, str]:
    return dict(request.url.params)


class TestReadingCursors:
    async def test_cursor_that_was_never_saved_reads_as_none(self) -> None:
        client, transport = make_client(
            respond_by_method(get=(200, {"ok": True, "cursor": None}))
        )

        assert await client.get_integration_cursor("issue", 2) is None

        [request] = transport.requests
        assert request.method == "GET"
        assert str(request.url).startswith(CURSOR_URL)
        assert query(request) == {"kind": "issue", "index": "2"}
        assert request.headers["Authorization"] == AUTH

    async def test_cursor_document_without_a_value_reads_as_none(self) -> None:
        client, _ = make_client(
            respond_by_method(get=(200, {"ok": True, "cursor": {"primary": None}}))
        )

        assert await client.get_integration_cursor("issue", 0) is None

    async def test_saved_cursor_is_returned_as_an_aware_datetime(self) -> None:
        client, _ = make_client(
            respond_by_method(
                get=(200, {"ok": True, "cursor": {"primary": CURSOR.isoformat()}})
            )
        )

        cursor = await client.get_integration_cursor("issue", 0)

        assert cursor == CURSOR
        assert cursor is not None and cursor.utcoffset() == timedelta(0)

    async def test_failure_to_read_a_cursor_is_raised(self) -> None:
        """A read failure must not look like "no cursor yet": that would silently
        restart the kind from the default look-back window."""
        client, _ = make_client(respond_by_method(get=(503, {"ok": False})))

        with pytest.raises(httpx.HTTPStatusError):
            await client.get_integration_cursor("issue", 0)


class TestSavingCursors:
    async def test_existing_cursor_is_updated_in_place(self) -> None:
        client, transport = make_client(respond_by_method(put=(200, {"ok": True})))

        await client.upsert_integration_cursor("pull-request", 3, CURSOR)

        [request] = transport.requests
        assert request.method == "PUT"
        assert str(request.url).startswith(CURSOR_URL)
        assert query(request) == {"kind": "pull-request", "index": "3"}
        assert json.loads(request.content) == {"primary": "2026-03-08T17:45:00+00:00"}

    async def test_missing_cursor_is_created(self) -> None:
        client, transport = make_client(
            respond_by_method(put=(404, {"ok": False}), post=(201, {"ok": True}))
        )

        await client.upsert_integration_cursor("pull-request", 3, CURSOR)

        assert transport.methods == ["PUT", "POST"]
        create = transport.requests[1]
        assert str(create.url).startswith(CURSOR_URL)
        assert query(create) == {"kind": "pull-request", "index": "3"}
        assert json.loads(create.content) == {"primary": CURSOR.isoformat()}
        assert all(r.headers["Authorization"] == AUTH for r in transport.requests)

    async def test_update_errors_are_raised_without_creating_a_cursor(self) -> None:
        client, transport = make_client(
            respond_by_method(put=(500, {"ok": False}), post=(201, {"ok": True}))
        )

        with pytest.raises(httpx.HTTPStatusError):
            await client.upsert_integration_cursor("issue", 0, CURSOR)

        assert transport.methods == ["PUT"]

    async def test_create_errors_are_raised(self) -> None:
        client, transport = make_client(
            respond_by_method(put=(404, {"ok": False}), post=(422, {"ok": False}))
        )

        with pytest.raises(httpx.HTTPStatusError):
            await client.upsert_integration_cursor("issue", 0, CURSOR)

        assert transport.methods == ["PUT", "POST"]

    async def test_saved_cursor_reads_back_unchanged(self) -> None:
        stored: dict[tuple[str, str], str] = {}

        def cursor_api(request: httpx.Request) -> httpx.Response:
            key = (request.url.params["kind"], request.url.params["index"])
            if request.method == "GET":
                value = stored.get(key)
                return httpx.Response(
                    200, json={"cursor": {"primary": value} if value else None}
                )
            if request.method == "PUT" and key not in stored:
                return httpx.Response(404, json={"ok": False})
            stored[key] = json.loads(request.content)["primary"]
            return httpx.Response(200, json={"ok": True})

        client, _ = make_client(cursor_api)
        eastern = timezone(timedelta(hours=-5))
        value = datetime(2026, 3, 8, 12, 45, 30, 123456, tzinfo=eastern)

        await client.upsert_integration_cursor("issue", 1, value)

        assert await client.get_integration_cursor("issue", 1) == value
        assert await client.get_integration_cursor("issue", 0) is None
