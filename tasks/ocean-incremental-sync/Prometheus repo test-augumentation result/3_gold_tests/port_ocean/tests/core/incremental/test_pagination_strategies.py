"""Tests for the helpers integrations use to fetch only what changed since the
incremental cursor: server-side timestamp filters and client-side cut-off."""

from datetime import datetime, timezone
from typing import Any, AsyncIterator

from port_ocean.core.incremental import (
    ClientSideCutoffStrategy,
    IncrementalStrategy,
    ServerSideTimestampStrategy,
    paginate_with_strategy,
)

CURSOR = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)

Page = list[dict[str, Any]]


class PagedEndpoint:
    """A paginated API that records how many pages were requested."""

    def __init__(self, *pages: Page) -> None:
        self.pages = pages
        self.requested = 0

    async def fetch(self) -> AsyncIterator[Page]:
        for page in self.pages:
            self.requested += 1
            yield page


async def collect(
    endpoint: PagedEndpoint,
    cursor: datetime | None,
    strategy: IncrementalStrategy | None,
) -> list[Page]:
    return [
        batch
        async for batch in paginate_with_strategy(
            endpoint.fetch(), cursor=cursor, strategy=strategy
        )
    ]


def pr(number: int, updated_at: str | None) -> dict[str, Any]:
    item: dict[str, Any] = {"number": number}
    if updated_at is not None:
        item["updated_at"] = updated_at
    return item


def newest_first() -> ClientSideCutoffStrategy:
    return ClientSideCutoffStrategy(
        stop_field="updated_at",
        query_params={"sort": "updated", "direction": "desc"},
    )


class TestServerSideFilter:
    def test_full_resync_keeps_request_params_as_is(self) -> None:
        strategy = ServerSideTimestampStrategy(param_key="since")
        base = {"state": "all", "per_page": 100}

        assert strategy.merge_params(base, None) == {"state": "all", "per_page": 100}

    def test_cursor_is_sent_as_iso_timestamp_by_default(self) -> None:
        strategy = ServerSideTimestampStrategy(param_key="since")

        params = strategy.merge_params({"state": "all"}, CURSOR)

        assert params == {"state": "all", "since": "2026-03-08T12:00:00+00:00"}

    def test_cursor_formatting_follows_the_api(self) -> None:
        strategy = ServerSideTimestampStrategy(
            param_key="created", date_format="%Y-%m-%dT%H:%M:%SZ", value_prefix=">="
        )

        params = strategy.merge_params({"per_page": 50}, CURSOR)

        assert params == {"per_page": 50, "created": ">=2026-03-08T12:00:00Z"}

    def test_cursor_wins_over_a_configured_value_for_the_same_param(self) -> None:
        strategy = ServerSideTimestampStrategy(param_key="since")

        params = strategy.merge_params({"since": "2020-01-01T00:00:00Z"}, CURSOR)

        assert params == {"since": CURSOR.isoformat()}

    def test_superseded_filters_are_dropped_only_when_there_is_a_cursor(
        self,
    ) -> None:
        strategy = ServerSideTimestampStrategy(param_key="updated_after")
        base = {"updated_before": "2026-01-01", "state": "opened"}

        incremental = strategy.merge_params_replacing(
            base, CURSOR, replaced_keys=["updated_before"]
        )
        full = strategy.merge_params_replacing(
            base, None, replaced_keys=["updated_before"]
        )

        assert incremental == {
            "state": "opened",
            "updated_after": CURSOR.isoformat(),
        }
        assert full == {"updated_before": "2026-01-01", "state": "opened"}
        assert base == {"updated_before": "2026-01-01", "state": "opened"}

    async def test_server_filtered_pages_are_all_consumed(self) -> None:
        endpoint = PagedEndpoint(
            [pr(1, "2026-03-08T13:00:00Z")], [pr(2, "2026-03-01T00:00:00Z")]
        )

        batches = await collect(
            endpoint, CURSOR, ServerSideTimestampStrategy(param_key="since")
        )

        assert batches == [
            [pr(1, "2026-03-08T13:00:00Z")],
            [pr(2, "2026-03-01T00:00:00Z")],
        ]
        assert endpoint.requested == 2


class TestClientSideCutoff:
    def test_sort_order_is_requested_only_for_incremental_runs(self) -> None:
        strategy = newest_first()
        base = {"state": "all"}

        assert strategy.merge_params(base, CURSOR) == {
            "state": "all",
            "sort": "updated",
            "direction": "desc",
        }
        assert strategy.merge_params(base, None) == {"state": "all"}

    async def test_api_that_is_already_newest_first_needs_no_extra_params(
        self,
    ) -> None:
        strategy = ClientSideCutoffStrategy(stop_field="updated_at")
        endpoint = PagedEndpoint(
            [pr(2, "2026-03-08T13:00:00Z"), pr(1, "2026-03-08T11:00:00Z")],
            [pr(0, "2026-03-08T10:00:00Z")],
        )

        assert strategy.merge_params({"state": "all"}, CURSOR) == {"state": "all"}
        assert await collect(endpoint, CURSOR, strategy) == [
            [pr(2, "2026-03-08T13:00:00Z")]
        ]
        assert endpoint.requested == 1

    async def test_stops_after_the_page_that_reaches_the_cursor(self) -> None:
        endpoint = PagedEndpoint(
            [pr(9, "2026-03-08T15:00:00Z"), pr(8, "2026-03-08T14:00:00Z")],
            [
                pr(7, "2026-03-08T13:00:00Z"),
                pr(6, "2026-03-08T12:00:00Z"),
                pr(5, "2026-03-08T11:59:59Z"),
            ],
            [pr(4, "2026-03-08T16:00:00Z")],
        )

        batches = await collect(endpoint, CURSOR, newest_first())

        assert batches == [
            [pr(9, "2026-03-08T15:00:00Z"), pr(8, "2026-03-08T14:00:00Z")],
            [pr(7, "2026-03-08T13:00:00Z"), pr(6, "2026-03-08T12:00:00Z")],
        ]
        assert endpoint.requested == 2

    async def test_page_of_only_stale_items_yields_nothing_and_stops(self) -> None:
        endpoint = PagedEndpoint(
            [pr(3, "2026-03-07T00:00:00Z"), pr(2, "2026-03-06T00:00:00Z")],
            [pr(1, "2026-03-05T00:00:00Z")],
        )

        batches = await collect(endpoint, CURSOR, newest_first())

        assert batches == []
        assert endpoint.requested == 1

    async def test_items_without_a_timestamp_are_kept(self) -> None:
        endpoint = PagedEndpoint(
            [pr(3, "2026-03-08T13:00:00Z"), pr(2, None)],
            [pr(1, "")],
        )

        batches = await collect(endpoint, CURSOR, newest_first())

        assert batches == [[pr(3, "2026-03-08T13:00:00Z"), pr(2, None)], [pr(1, "")]]
        assert endpoint.requested == 2

    async def test_timestamps_with_utc_offsets_are_compared_in_utc(self) -> None:
        fresh = pr(2, "2026-03-08T09:30:00-04:00")  # 13:30 UTC
        stale = pr(1, "2026-03-08T13:30:00+02:00")  # 11:30 UTC
        endpoint = PagedEndpoint([fresh, stale], [pr(0, "2026-03-08T16:00:00Z")])

        batches = await collect(endpoint, CURSOR, newest_first())

        assert batches == [[fresh]]
        assert endpoint.requested == 1

    async def test_full_resync_passes_every_page_through(self) -> None:
        pages = [
            [pr(2, "2026-03-08T13:00:00Z"), pr(1, "2020-01-01T00:00:00Z")],
            [pr(0, "2019-01-01T00:00:00Z")],
        ]
        endpoint = PagedEndpoint(*pages)

        batches = await collect(endpoint, None, newest_first())

        assert batches == pages
        assert endpoint.requested == 2


async def test_pages_pass_through_untouched_without_a_strategy() -> None:
    pages = [[pr(2, "2020-01-01T00:00:00Z")], [], [pr(1, None)]]
    endpoint = PagedEndpoint(*pages)

    batches = await collect(endpoint, CURSOR, None)

    assert batches == pages
    assert endpoint.requested == 3
