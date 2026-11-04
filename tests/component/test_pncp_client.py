"""Component tests for the PNCP HTTP client."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pytest

from pncp_etl.contracts import QueryWindow, SafetyLimits
from pncp_etl.pncp.client import PncpClient
from pncp_etl.pncp.errors import (
    ContractError,
    PermanentSourceError,
    RateLimitDeferred,
    TransientSourceError,
)

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


def _envelope(
    *,
    page: int,
    total_pages: int,
    total_records: int,
    data: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "data": data,
        "totalRegistros": total_records,
        "totalPaginas": total_pages,
        "numeroPagina": page,
        "paginasRestantes": max(total_pages - page, 0),
        "empty": not data,
    }


def _window() -> QueryWindow:
    return QueryWindow(start=date(2026, 11, 1), end=date(2026, 11, 1), cnpj="12345678000195")


def _limits(**changes: object) -> SafetyLimits:
    values: dict[str, object] = {
        "page_size": 10,
        "max_pages": 10,
        "max_records": 100,
        "max_duration_seconds": 60,
        "max_artifact_bytes": 1_000_000,
    }
    values.update(changes)
    return SafetyLimits(**values)


def _client(
    handler: httpx.MockTransportHandler,
    *,
    sleeps: list[float] | None = None,
) -> PncpClient:
    return PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=(sleeps if sleeps is not None else []).append,
        jitter=lambda _: 0.0,
    )


def test_iterates_two_pages_with_exact_parameters() -> None:
    """Every announced page is fetched once with the configured query."""
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        seen.append(params)
        page = int(params["pagina"])
        return httpx.Response(
            200,
            json=_envelope(
                page=page,
                total_pages=2,
                total_records=3,
                data=[{"id": page * 10 + index} for index in range(2 if page == 1 else 1)],
            ),
        )

    pages = list(_client(handler).iter_pages(_window(), _limits()))

    assert [page.number for page in pages] == [1, 2]
    assert [len(page.records) for page in pages] == [2, 1]
    assert [item["pagina"] for item in seen] == ["1", "2"]
    assert all(item["dataInicial"] == "20261101" for item in seen)
    assert all(item["dataFinal"] == "20261101" for item in seen)
    assert all(item["tamanhoPagina"] == "10" for item in seen)
    assert all(item["cnpj"] == "12345678000195" for item in seen)


def test_initial_204_is_a_valid_empty_result() -> None:
    """A 204 on the first page represents a complete empty extraction."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    assert list(_client(handler).iter_pages(_window(), _limits())) == []


def test_initial_coherent_empty_envelope_is_valid() -> None:
    """The source may represent an empty first page with a JSON envelope."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=0, total_records=0, data=[]),
        )

    assert list(_client(handler).iter_pages(_window(), _limits())) == []


@pytest.mark.parametrize("status", [400, 401, 422])
def test_permanent_status_is_not_retried(status: int) -> None:
    """Deterministic client errors must make exactly one request."""
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, json={"message": "invalid"})

    with pytest.raises(PermanentSourceError, match=str(status)):
        list(_client(handler).iter_pages(_window(), _limits()))

    assert calls == 1


def test_transient_500_retries_then_succeeds() -> None:
    """A transient response uses finite backoff and keeps the same page."""
    calls = 0
    sleeps: list[float] = []

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(500)
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=1, total_records=1, data=[{"id": 1}]),
        )

    pages = list(_client(handler, sleeps=sleeps).iter_pages(_window(), _limits()))

    assert calls == 2
    assert sleeps == [1.0]
    assert pages[0].records == ({"id": 1},)


def test_transport_timeout_retries_then_succeeds() -> None:
    """Transport exceptions use the same finite retry policy as 5xx."""
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=1, total_records=1, data=[{"id": 1}]),
        )

    pages = list(_client(handler, sleeps=sleeps).iter_pages(_window(), _limits()))

    assert calls == 2
    assert sleeps == [1.0]
    assert pages[0].records == ({"id": 1},)


def test_transient_status_exhausts_exactly_three_attempts() -> None:
    """A persistent 5xx becomes a transient error after three total attempts."""
    calls = 0
    sleeps: list[float] = []

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    with pytest.raises(TransientSourceError, match="503"):
        list(_client(handler, sleeps=sleeps).iter_pages(_window(), _limits()))

    assert calls == 3
    assert sleeps == [1.0, 2.0]


def test_long_retry_after_defers_without_a_second_request() -> None:
    """A valid server delay above the inline budget is never truncated."""
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "120"})

    with pytest.raises(RateLimitDeferred) as raised:
        list(_client(handler).iter_pages(_window(), _limits()))

    assert calls == 1
    assert raised.value.retry_not_before == NOW + timedelta(seconds=120)


def test_short_retry_after_waits_before_retrying() -> None:
    """A short valid Retry-After is honored exactly before the next call."""
    calls = 0
    sleeps: list[float] = []

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=1, total_records=1, data=[{"id": 1}]),
        )

    list(_client(handler, sleeps=sleeps).iter_pages(_window(), _limits()))

    assert calls == 2
    assert sleeps == [5.0]


def test_retry_after_http_date_is_honored() -> None:
    """An RFC HTTP-date is converted to a minimum wait using the UTC clock."""
    calls = 0
    sleeps: list[float] = []

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                headers={"Retry-After": "Mon, 02 Nov 2026 12:00:05 GMT"},
            )
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=1, total_records=1, data=[{"id": 1}]),
        )

    list(_client(handler, sleeps=sleeps).iter_pages(_window(), _limits()))

    assert calls == 2
    assert sleeps == [5.0]


def test_invalid_retry_after_falls_back_to_local_backoff() -> None:
    """An invalid server header does not disable the finite retry policy."""
    calls = 0
    sleeps: list[float] = []

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "not-a-date"})
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=1, total_records=1, data=[{"id": 1}]),
        )

    list(_client(handler, sleeps=sleeps).iter_pages(_window(), _limits()))

    assert calls == 2
    assert sleeps == [1.0]


def test_invalid_json_is_a_contract_error_without_retry() -> None:
    """A successful status with invalid JSON is not treated as empty or transient."""
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"not-json")

    with pytest.raises(ContractError, match="JSON"):
        list(_client(handler).iter_pages(_window(), _limits()))

    assert calls == 1


def test_changed_pagination_metadata_fails_completeness() -> None:
    """Totals cannot change silently between pages."""

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["pagina"])
        total_records = 2 if page == 1 else 3
        return httpx.Response(
            200,
            json=_envelope(
                page=page,
                total_pages=2,
                total_records=total_records,
                data=[{"id": page}],
            ),
        )

    with pytest.raises(ContractError, match="changed"):
        list(_client(handler).iter_pages(_window(), _limits()))


def test_announced_pages_above_guardrail_fail_after_first_request() -> None:
    """The first envelope is a preflight guard and never yields partial success."""
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=20, total_records=20, data=[{"id": 1}]),
        )

    with pytest.raises(ContractError, match="max_pages"):
        list(_client(handler).iter_pages(_window(), _limits(max_pages=10)))

    assert calls == 1


def test_announced_records_above_guardrail_fail_after_first_request() -> None:
    """The record ceiling is checked from the first envelope before yielding."""
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=2, total_records=101, data=[{"id": 1}]),
        )

    with pytest.raises(ContractError, match="max_records"):
        list(_client(handler).iter_pages(_window(), _limits(max_records=100)))

    assert calls == 1


def test_invalid_window_and_limits_fail_before_network() -> None:
    """Invalid local configuration never reaches the public API."""
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(204)

    with pytest.raises(ValueError, match="start"):
        QueryWindow(start=date(2026, 11, 2), end=date(2026, 11, 1))
    with pytest.raises(ValueError, match="page_size"):
        _limits(page_size=9)

    assert calls == 0


def test_duration_ceiling_stops_before_requesting_next_page() -> None:
    """A period that exceeds its time budget is incomplete, never truncated as success."""
    calls = 0
    clock = iter([0.0, 0.0, 0.0, 61.0])

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json=_envelope(page=1, total_pages=2, total_records=2, data=[{"id": 1}]),
        )

    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
        monotonic=lambda: next(clock),
    )

    with pytest.raises(ContractError, match="max_duration_seconds"):
        list(client.iter_pages(_window(), _limits()))

    assert calls == 1
