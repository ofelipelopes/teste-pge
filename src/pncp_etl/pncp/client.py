"""HTTP client interface for PNCP price registration minutes."""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from pncp_etl.contracts import QueryWindow, SafetyLimits, SourcePage
from pncp_etl.pncp.errors import (
    ContractError,
    PermanentSourceError,
    RateLimitDeferred,
    TransientSourceError,
)

PNCP_MINUTES_URL = "https://pncp.gov.br/api/consulta/v1/atas"
DEFAULT_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)


class PncpClient:
    """Read and validate sequential pages from the PNCP API."""

    def __init__(
        self,
        *,
        http_client: httpx.Client,
        now: Callable[[], datetime],
        sleep: Callable[[float], None],
        jitter: Callable[[float], float],
        monotonic: Callable[[], float] = time.monotonic,
        max_attempts: int = 3,
        inline_wait_budget: float = 60.0,
        base_url: str = PNCP_MINUTES_URL,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if inline_wait_budget < 0:
            raise ValueError("inline_wait_budget must be non-negative")
        self._http_client = http_client
        self._now = now
        self._sleep = sleep
        self._jitter = jitter
        self._monotonic = monotonic
        self._max_attempts = max_attempts
        self._inline_wait_budget = inline_wait_budget
        self._base_url = base_url

    def iter_pages(
        self,
        window: QueryWindow,
        limits: SafetyLimits,
    ) -> Iterator[SourcePage]:
        """Yield complete source pages without retaining the whole period."""
        started_at = self._monotonic()
        expected_total_pages: int | None = None
        expected_total_records: int | None = None
        records_read = 0
        page_number = 1

        while True:
            self._check_duration(started_at, limits)
            response = self._request_page(
                window=window,
                limits=limits,
                page_number=page_number,
                started_at=started_at,
            )
            if response.status_code == 204:
                if page_number != 1:
                    raise ContractError("HTTP 204 is only valid for the initial empty page")
                return

            page = self._parse_page(response, requested_page=page_number)
            if page_number == 1:
                expected_total_pages = page.total_pages
                expected_total_records = page.total_records
                self._check_preflight(page, limits)
                if not page.records:
                    if page.total_records != 0 or page.total_pages not in {0, 1}:
                        raise ContractError("initial empty envelope has inconsistent totals")
                    return
            elif (
                page.total_pages != expected_total_pages
                or page.total_records != expected_total_records
            ):
                raise ContractError("pagination totals changed between pages")

            if not page.records:
                raise ContractError("intermediate page cannot be empty")

            records_read += len(page.records)
            if records_read > limits.max_records:
                raise ContractError("max_records exceeded during extraction")
            yield page

            if expected_total_pages is None or expected_total_records is None:
                raise ContractError("pagination metadata was not initialized")
            if page_number >= expected_total_pages:
                if records_read != expected_total_records:
                    raise ContractError(
                        "records read do not match totalRegistros: "
                        f"{records_read} != {expected_total_records}"
                    )
                return
            page_number += 1

    def _request_page(
        self,
        *,
        window: QueryWindow,
        limits: SafetyLimits,
        page_number: int,
        started_at: float,
    ) -> httpx.Response:
        """Request one page under the finite HTTP retry policy."""
        params = window.query_params(page=page_number, page_size=limits.page_size)
        for attempt in range(1, self._max_attempts + 1):
            self._check_duration(started_at, limits)
            try:
                response = self._http_client.get(
                    self._base_url,
                    params=params,
                    timeout=DEFAULT_TIMEOUT,
                    headers={"User-Agent": "pge-ce-pncp-etl/0.1"},
                )
            except httpx.TransportError as error:
                if attempt == self._max_attempts:
                    raise TransientSourceError(
                        f"transport failure after {attempt} attempts: {type(error).__name__}"
                    ) from error
                self._wait_for_retry(
                    attempt=attempt,
                    retry_after=None,
                    started_at=started_at,
                    limits=limits,
                )
                continue

            status = response.status_code
            if status == 204 or status == 200:
                return response
            if status == 429 or 500 <= status <= 599:
                if attempt == self._max_attempts:
                    raise TransientSourceError(
                        f"HTTP {status} after {attempt} attempts on page {page_number}"
                    )
                self._wait_for_retry(
                    attempt=attempt,
                    retry_after=response.headers.get("Retry-After") if status == 429 else None,
                    started_at=started_at,
                    limits=limits,
                )
                continue
            raise PermanentSourceError(f"HTTP {status} on page {page_number}")

        raise AssertionError("finite request loop exited unexpectedly")

    def _wait_for_retry(
        self,
        *,
        attempt: int,
        retry_after: str | None,
        started_at: float,
        limits: SafetyLimits,
    ) -> None:
        """Wait locally or defer when a valid minimum delay exceeds the budget."""
        deadline = self._parse_retry_after(retry_after)
        elapsed = self._monotonic() - started_at
        remaining = limits.max_duration_seconds - elapsed
        if deadline is not None:
            delay = max((deadline - self._utc_now()).total_seconds(), 0.0)
            if delay > self._inline_wait_budget or delay > remaining:
                raise RateLimitDeferred(deadline)
        else:
            base_delay = min(float(2 ** (attempt - 1)), 30.0)
            delay = base_delay + max(self._jitter(base_delay), 0.0)
            if delay > remaining:
                raise TransientSourceError("max_duration_seconds would be exceeded by backoff")
        self._sleep(delay)

    def _parse_retry_after(self, value: str | None) -> datetime | None:
        """Return a future UTC deadline for valid delta-seconds or HTTP-date."""
        if value is None:
            return None
        stripped = value.strip()
        if stripped.isdigit():
            return self._utc_now() + timedelta(seconds=int(stripped))
        try:
            parsed = parsedate_to_datetime(stripped)
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        deadline = parsed.astimezone(UTC)
        if deadline <= self._utc_now():
            return None
        return deadline

    def _utc_now(self) -> datetime:
        """Read an aware UTC timestamp from the injected clock."""
        current = self._now()
        if current.tzinfo is None:
            raise ContractError("injected clock must return a timezone-aware datetime")
        return current.astimezone(UTC)

    def _check_duration(self, started_at: float, limits: SafetyLimits) -> None:
        """Fail explicitly after the configured extraction duration."""
        if self._monotonic() - started_at > limits.max_duration_seconds:
            raise ContractError("max_duration_seconds exceeded")

    @staticmethod
    def _check_preflight(page: SourcePage, limits: SafetyLimits) -> None:
        """Reject announced totals that already exceed local ceilings."""
        if page.total_pages > limits.max_pages:
            raise ContractError("announced totalPaginas exceeds max_pages")
        if page.total_records > limits.max_records:
            raise ContractError("announced totalRegistros exceeds max_records")

    @staticmethod
    def _parse_page(response: httpx.Response, *, requested_page: int) -> SourcePage:
        """Validate one successful JSON envelope without coercing its fields."""
        try:
            body: Any = response.json()
        except ValueError as error:
            raise ContractError("response body is not valid JSON") from error
        if not isinstance(body, dict):
            raise ContractError("JSON envelope must be an object")

        required = {
            "data",
            "totalRegistros",
            "totalPaginas",
            "numeroPagina",
            "paginasRestantes",
            "empty",
        }
        missing = required.difference(body)
        if missing:
            raise ContractError(f"JSON envelope is missing fields: {sorted(missing)}")

        data = body["data"]
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise ContractError("data must be a list of JSON objects")
        integer_fields = (
            "totalRegistros",
            "totalPaginas",
            "numeroPagina",
            "paginasRestantes",
        )
        for field_name in integer_fields:
            value = body[field_name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ContractError(f"{field_name} must be a non-negative integer")
        if not isinstance(body["empty"], bool):
            raise ContractError("empty must be boolean")

        total_records = body["totalRegistros"]
        total_pages = body["totalPaginas"]
        page_number = body["numeroPagina"]
        remaining_pages = body["paginasRestantes"]
        if page_number != requested_page:
            raise ContractError(
                f"numeroPagina {page_number} does not match requested page {requested_page}"
            )
        if page_number < 1:
            raise ContractError("numeroPagina must be at least 1")
        if remaining_pages != max(total_pages - page_number, 0):
            raise ContractError("paginasRestantes is inconsistent with totalPaginas")
        if body["empty"] != (len(data) == 0):
            raise ContractError("empty is inconsistent with data")
        if data and (total_pages < page_number or total_records < len(data)):
            raise ContractError("page data is inconsistent with announced totals")

        return SourcePage(
            number=page_number,
            total_pages=total_pages,
            total_records=total_records,
            records=tuple(data),
        )


def default_jitter(delay: float) -> float:
    """Return a small positive random component for production backoff."""
    return random.uniform(0.0, delay * 0.2)
