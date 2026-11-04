"""Errors raised while reading PNCP source data."""

from __future__ import annotations

from datetime import datetime


class PncpError(Exception):
    """Base source integration error."""


class PermanentSourceError(PncpError):
    """A deterministic source response that should not be retried."""


class TransientSourceError(PncpError):
    """A source failure that exhausted inline retries."""


class ContractError(PncpError):
    """A response or configuration that violates an extraction contract."""


class RateLimitDeferred(TransientSourceError):
    """A valid source delay that must be resumed by task orchestration."""

    def __init__(self, retry_not_before: datetime) -> None:
        super().__init__(f"retry not before {retry_not_before.isoformat()}")
        self.retry_not_before = retry_not_before
