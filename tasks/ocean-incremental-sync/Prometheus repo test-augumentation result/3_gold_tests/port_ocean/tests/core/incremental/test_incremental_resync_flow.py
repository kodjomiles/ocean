"""Integration tests for incremental resyncs.

These run ``sync_incremental`` through the real resync pipeline (port app
config handler, JQ entity processor, HTTP state applier, Port client) and keep
incremental cursors in an in-memory stand-in for Port's integration cursor
endpoints, so each test can check what an integration's handlers saw and what
progress was persisted.
"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Generator
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from port_ocean.clients.port.client import PortClient
from port_ocean.context.ocean import PortOceanContext
from port_ocean.core.handlers.entities_state_applier.port.applier import (
    HttpEntitiesStateApplier,
)
from port_ocean.core.handlers.entity_processor.jq_entity_processor import (
    JQEntityProcessor,
)
from port_ocean.core.handlers.port_app_config.api import APIPortAppConfig
from port_ocean.core.incremental import active_incremental_cursor
from port_ocean.core.integrations.mixins import SyncRawMixin
from port_ocean.core.ocean_types import ASYNC_GENERATOR_RESYNC_TYPE
from port_ocean.ocean import Ocean

INTERVAL_SECONDS = 15 * 60
API_URL = "https://api.getport.test/v1"
LAST_WEEK = datetime(2026, 3, 2, 8, 30, tzinfo=timezone.utc)
YESTERDAY = datetime(2026, 3, 8, 17, 45, tzinfo=timezone.utc)

SetMapping = Callable[..., None]


class _InProcessExecutor(ThreadPoolExecutor):
    """Runs JQ mapping in threads instead of forked worker processes."""

    def __init__(self, *args: Any, mp_context: Any = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class FakeCursorApi:
    """In-memory version of Port's ``/integration/{id}/cursor`` endpoints.

    GET returns the stored cursor (or ``null``), PUT updates an existing cursor
    and answers 404 for unknown ones, POST creates a cursor and answers 409 if
    it already exists.
    """

    def __init__(self) -> None:
        self.cursors: dict[tuple[str, int], str] = {}
        self.calls: list[tuple[str, str, int]] = []
        self.rejected_kinds: set[str] = set()

    def seed(self, kind: str, index: int, value: datetime) -> None:
        self.cursors[(kind, index)] = value.isoformat()

    def stored(self, kind: str, index: int) -> datetime | None:
        raw = self.cursors.get((kind, index))
        return datetime.fromisoformat(raw) if raw else None

    def writes(self) -> list[tuple[str, str, int]]:
        return [call for call in self.calls if call[0] != "GET"]

    def respond(
        self, method: str, url: str, params: dict[str, Any], body: Any
    ) -> httpx.Response:
        request = httpx.Request(method, url, params=params, json=body)
        query = request.url.params
        if "kind" not in query or "index" not in query:
            return httpx.Response(400, json={"ok": False}, request=request)
        key = (query["kind"], int(query["index"]))
        self.calls.append((method, *key))

        if method == "GET":
            raw = self.cursors.get(key)
            cursor = {"primary": raw} if raw else None
            return httpx.Response(
                200, json={"ok": True, "cursor": cursor}, request=request
            )
        if key[0] in self.rejected_kinds:
            return httpx.Response(500, json={"ok": False}, request=request)

        value = json.loads(request.content)["primary"]
        if method == "PUT" and key not in self.cursors:
            return httpx.Response(404, json={"ok": False}, request=request)
        if method == "POST" and key in self.cursors:
            return httpx.Response(409, json={"ok": False}, request=request)
        self.cursors[key] = value
        return httpx.Response(200, json={"ok": True}, request=request)


@pytest.fixture(autouse=True)
def lightweight_resync_runtime() -> Generator[None, None, None]:
    sync_raw = "port_ocean.core.integrations.mixins.sync_raw"
    with (
        patch(f"{sync_raw}.start_monitoring", AsyncMock()),
        patch(f"{sync_raw}.stop_monitoring", AsyncMock()),
        patch(f"{sync_raw}.start_kind_tracking", MagicMock()),
        patch(f"{sync_raw}.stop_kind_tracking", MagicMock()),
        patch(
            "port_ocean.core.handlers.entity_processor.jq_entity_processor.ProcessPoolExecutor",
            _InProcessExecutor,
        ),
    ):
        yield


@pytest.fixture
def cursor_api(
    mock_port_client: PortClient, mock_http_client: MagicMock
) -> FakeCursorApi:
    api = FakeCursorApi()
    mock_port_client.auth.api_url = API_URL
    upsert_entities = mock_http_client.post.side_effect

    async def get(url: str, **kwargs: Any) -> httpx.Response:
        return api.respond("GET", url, kwargs.get("params", {}), None)

    async def put(url: str, **kwargs: Any) -> httpx.Response:
        return api.respond("PUT", url, kwargs.get("params", {}), kwargs.get("json"))

    async def post(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        if url.endswith("/cursor"):
            return api.respond(
                "POST", url, kwargs.get("params", {}), kwargs.get("json")
            )
        return await upsert_entities(url, *args, **kwargs)

    mock_http_client.get = AsyncMock(side_effect=get)
    mock_http_client.put = AsyncMock(side_effect=put)
    mock_http_client.post = AsyncMock(side_effect=post)
    return api


@pytest.fixture
def set_mapping(mock_port_client: PortClient) -> SetMapping:
    """Sets the integration's port-app-config, as Port would return it."""

    def _set(*resources: dict[str, Any]) -> None:
        mock_port_client.get_current_integration = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "config": {
                    "enableMergeEntity": True,
                    "deleteDependentEntities": False,
                    "createMissingRelatedEntities": False,
                    "resources": list(resources),
                }
            }
        )

    return _set


@pytest.fixture
def integration(
    mock_ocean: Ocean, mock_context: PortOceanContext, cursor_api: FakeCursorApi
) -> SyncRawMixin:
    mock_ocean.config.integration.identifier = "my-integration"
    mock_ocean.config.integration.type = "fake-integration"
    mock_ocean.config.send_raw_data_examples = False
    mock_ocean.config.upsert_entities_batch_max_length = 20
    mock_ocean.config.upsert_entities_batch_max_size_in_bytes = 1024 * 1024

    integration = SyncRawMixin()
    integration._entity_processor = JQEntityProcessor(mock_context)
    integration._entities_state_applier = HttpEntitiesStateApplier(mock_context)
    integration._port_app_config_handler = APIPortAppConfig(mock_context)
    return integration


def mapping(kind: str, query: str = "true") -> dict[str, Any]:
    return {
        "kind": kind,
        "selector": {"query": query},
        "port": {
            "entity": {
                "mappings": {
                    "identifier": ".id",
                    "title": ".title",
                    "blueprint": f'"{kind}"',
                    "properties": {},
                }
            }
        },
    }


def upserted_identifiers(mock_http_client: MagicMock) -> list[str]:
    return [
        entity["identifier"]
        for call in mock_http_client.post.await_args_list
        if not call.args[0].endswith("/cursor")
        for entity in call.kwargs["json"]["entities"]
    ]


class IncrementalHandler:
    """An ``on_incremental_resync`` handler that records what it saw."""

    def __init__(
        self,
        items: list[dict[str, Any]] | None = None,
        fails_with: Exception | None = None,
    ) -> None:
        self.items = items if items is not None else [{"id": "1", "title": "one"}]
        self.fails_with = fails_with
        self.cursors: list[datetime | None] = []
        self.fetch_started_at: list[datetime] = []

    async def fetch(self, kind: str) -> ASYNC_GENERATOR_RESYNC_TYPE:
        self.fetch_started_at.append(datetime.now(timezone.utc))
        self.cursors.append(active_incremental_cursor())
        # Simulate API latency so the fetch has a measurable duration.
        await asyncio.sleep(0.05)
        if self.fails_with is not None:
            raise self.fails_with
        yield self.items

    def register(self, integration: SyncRawMixin, kind: str) -> "IncrementalHandler":
        integration.on_incremental_resync(self.fetch, kind)
        return self


class TestCursorProgression:
    async def test_first_run_looks_back_one_interval(
        self, integration: SyncRawMixin, set_mapping: SetMapping
    ) -> None:
        set_mapping(mapping("issue"))
        handler = IncrementalHandler().register(integration, "issue")

        before = datetime.now(timezone.utc)
        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)
        after = datetime.now(timezone.utc)

        assert len(handler.cursors) == 1
        cursor = handler.cursors[0]
        assert cursor is not None
        interval = timedelta(seconds=INTERVAL_SECONDS)
        assert before - interval <= cursor <= after - interval

    async def test_successful_run_upserts_items_and_persists_cursor(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
        mock_http_client: MagicMock,
    ) -> None:
        set_mapping(mapping("issue"))
        IncrementalHandler(
            items=[{"id": "ISSUE-1", "title": "a"}, {"id": "ISSUE-2", "title": "b"}]
        ).register(integration, "issue")

        before = datetime.now(timezone.utc)
        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert sorted(upserted_identifiers(mock_http_client)) == ["ISSUE-1", "ISSUE-2"]
        saved = cursor_api.stored("issue", 0)
        assert saved is not None
        assert saved >= before
        assert saved.tzinfo is not None

    async def test_saved_cursor_is_taken_before_fetching(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        """Items updated while the run is fetching must be picked up next time,
        so the persisted cursor may not be later than the fetch start."""
        set_mapping(mapping("issue"))
        handler = IncrementalHandler().register(integration, "issue")

        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        saved = cursor_api.stored("issue", 0)
        assert saved is not None
        assert saved <= handler.fetch_started_at[0]

    async def test_next_run_resumes_from_stored_cursor(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        cursor_api.seed("issue", 0, YESTERDAY)
        set_mapping(mapping("issue"))
        handler = IncrementalHandler().register(integration, "issue")

        before = datetime.now(timezone.utc)
        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert handler.cursors == [YESTERDAY]
        saved = cursor_api.stored("issue", 0)
        assert saved is not None and saved >= before
        assert [method for method, *_ in cursor_api.writes()] == ["PUT"]

    async def test_consecutive_runs_chain_cursors(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        set_mapping(mapping("issue"))
        handler = IncrementalHandler().register(integration, "issue")

        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)
        first_saved = cursor_api.stored("issue", 0)
        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert first_saved is not None
        assert handler.cursors[1] == first_saved
        assert [method for method, *_ in cursor_api.writes()] == ["PUT", "POST", "PUT"]

    async def test_cursor_is_not_visible_after_the_run(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        cursor_api.seed("issue", 0, YESTERDAY)
        set_mapping(mapping("issue"))
        IncrementalHandler().register(integration, "issue")

        assert active_incremental_cursor() is None
        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert active_incremental_cursor() is None


class TestFailedKinds:
    async def test_failing_kind_keeps_cursor_and_halts_the_run(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
        mock_http_client: MagicMock,
    ) -> None:
        cursor_api.seed("issue", 0, LAST_WEEK)
        cursor_api.seed("pull-request", 1, YESTERDAY)
        set_mapping(mapping("issue"), mapping("pull-request"))
        IncrementalHandler(fails_with=RuntimeError("rate limited")).register(
            integration, "issue"
        )
        pull_requests = IncrementalHandler(
            items=[{"id": "PR-7", "title": "fix"}]
        ).register(integration, "pull-request")

        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert cursor_api.stored("issue", 0) == LAST_WEEK
        assert cursor_api.stored("pull-request", 1) == YESTERDAY
        assert cursor_api.writes() == []
        assert pull_requests.cursors == []
        assert upserted_identifiers(mock_http_client) == []

    async def test_kinds_finished_before_a_failure_keep_their_progress(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        cursor_api.seed("issue", 0, LAST_WEEK)
        cursor_api.seed("pull-request", 1, LAST_WEEK)
        cursor_api.seed("deployment", 2, LAST_WEEK)
        set_mapping(mapping("issue"), mapping("pull-request"), mapping("deployment"))
        IncrementalHandler().register(integration, "issue")
        IncrementalHandler(fails_with=RuntimeError("boom")).register(
            integration, "pull-request"
        )
        deployments = IncrementalHandler().register(integration, "deployment")

        before = datetime.now(timezone.utc)
        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        issue_cursor = cursor_api.stored("issue", 0)
        assert issue_cursor is not None and issue_cursor >= before
        assert cursor_api.stored("pull-request", 1) == LAST_WEEK
        assert cursor_api.stored("deployment", 2) == LAST_WEEK
        assert deployments.cursors == []

    async def test_partially_failed_kind_keeps_its_cursor(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
        mock_http_client: MagicMock,
    ) -> None:
        """One of two fetchers for the kind fails: whatever the other fetched is
        still upserted, but the window must be retried on the next run."""
        cursor_api.seed("issue", 0, LAST_WEEK)
        cursor_api.seed("pull-request", 1, LAST_WEEK)
        set_mapping(mapping("issue"), mapping("pull-request"))
        IncrementalHandler(items=[{"id": "ISSUE-1", "title": "a"}]).register(
            integration, "issue"
        )
        archive_calls: list[str] = []

        async def archived_issues(kind: str) -> list[dict[str, Any]]:
            archive_calls.append(kind)
            raise RuntimeError("archive unavailable")

        integration.on_incremental_resync(archived_issues, "issue")
        pull_requests = IncrementalHandler().register(integration, "pull-request")

        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert archive_calls == ["issue"]
        assert upserted_identifiers(mock_http_client) == ["ISSUE-1"]
        assert cursor_api.stored("issue", 0) == LAST_WEEK
        assert pull_requests.cursors == []

    async def test_failing_to_persist_progress_halts_the_run(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        cursor_api.seed("issue", 0, LAST_WEEK)
        cursor_api.seed("pull-request", 1, LAST_WEEK)
        cursor_api.rejected_kinds.add("issue")
        set_mapping(mapping("issue"), mapping("pull-request"))
        IncrementalHandler().register(integration, "issue")
        pull_requests = IncrementalHandler().register(integration, "pull-request")

        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert cursor_api.stored("issue", 0) == LAST_WEEK
        assert pull_requests.cursors == []
        assert cursor_api.stored("pull-request", 1) == LAST_WEEK


class TestMultipleMappings:
    async def test_each_mapping_of_a_kind_has_its_own_cursor(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        cursor_api.seed("issue", 0, LAST_WEEK)
        cursor_api.seed("issue", 1, YESTERDAY)
        set_mapping(
            mapping("issue", query='.state == "open"'),
            mapping("issue", query='.state == "closed"'),
        )
        handler = IncrementalHandler().register(integration, "issue")

        before = datetime.now(timezone.utc)
        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert handler.cursors == [LAST_WEEK, YESTERDAY]
        for index in (0, 1):
            saved = cursor_api.stored("issue", index)
            assert saved is not None and saved >= before

    async def test_kinds_without_incremental_support_are_skipped(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        cursor_api.seed("issue", 1, YESTERDAY)
        set_mapping(mapping("repository"), mapping("issue"), mapping("team"))
        full_resync_only = AsyncMock(return_value=[{"id": "r1", "title": "repo"}])
        integration.on_resync(full_resync_only, "repository")
        integration.on_resync(full_resync_only, "team")
        issues = IncrementalHandler().register(integration, "issue")

        before = datetime.now(timezone.utc)
        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        full_resync_only.assert_not_called()
        assert issues.cursors == [YESTERDAY]
        saved = cursor_api.stored("issue", 1)
        assert saved is not None and saved >= before
        assert {(kind, index) for _, kind, index in cursor_api.calls} == {("issue", 1)}

    async def test_catch_all_full_resync_handlers_are_not_called(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
        mock_http_client: MagicMock,
    ) -> None:
        set_mapping(mapping("issue"))
        full_resync_calls: list[str] = []

        async def resync_everything(kind: str) -> list[dict[str, Any]]:
            full_resync_calls.append(kind)
            return [{"id": "OLD-1", "title": "x"}]

        integration.on_resync(resync_everything)
        IncrementalHandler(items=[{"id": "ISSUE-9", "title": "new"}]).register(
            integration, "issue"
        )

        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        assert full_resync_calls == []
        assert upserted_identifiers(mock_http_client) == ["ISSUE-9"]
        assert cursor_api.stored("issue", 0) is not None

    async def test_run_without_incremental_kinds_does_nothing(
        self,
        integration: SyncRawMixin,
        set_mapping: SetMapping,
        cursor_api: FakeCursorApi,
    ) -> None:
        set_mapping(mapping("repository"))
        full_resync_only = AsyncMock(return_value=[{"id": "r1", "title": "repo"}])
        integration.on_resync(full_resync_only, "repository")

        await integration.sync_incremental(interval_seconds=INTERVAL_SECONDS)

        full_resync_only.assert_not_called()
        assert cursor_api.calls == []
