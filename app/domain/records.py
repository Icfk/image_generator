"""Persistence-free records exchanged through application repository ports."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.domain.entities import TaskStatus


@dataclass
class UserRecord:
    """User state; never a SQLAlchemy instance."""

    id: int
    external_id: str
    secret_hash: str
    balance: int


@dataclass
class GenerationRecord:
    """Generation state tracked by one application UnitOfWork."""

    id: str
    user_id: int
    kind: str
    prompt: str
    image_url: str | None
    status: TaskStatus
    result: dict | None
    error: str | None
    cost: int
    created_at: datetime
    queued_at: datetime | None
    processing_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    provider_job_id: str | None
    idempotency_key: str | None
    webhook_url: str | None
    run_token: str | None
    provider_context: dict | None


@dataclass
class DeliveryRecord:
    """Durable webhook attempt state."""

    id: int
    generation_id: str
    url: str
    status: str
    attempts: int
    retry_job_id: str | None
    recovery_checked_at: datetime | None
    last_error: str | None
    next_attempt_at: datetime | None
    preparation_expires_at: datetime | None


@dataclass
class PublicationRecord:
    """Durable queue publication state."""

    id: int
    generation_id: str
    job_id: str
    status: str
    attempts: int
    last_error: str | None
    created_at: datetime
    next_attempt_at: datetime | None


@dataclass
class LedgerRecord:
    """One immutable-in-practice balance ledger entry."""

    id: int
    user_id: int
    generation_id: str | None
    external_key: str | None
    operation: str
    amount: int
    created_at: datetime


@dataclass
class PaymentRecord:
    """A payment idempotency event."""

    id: int
    external_user_id: str
    idempotency_key: str
    amount: int
    created_at: datetime


Record = (
    UserRecord
    | GenerationRecord
    | DeliveryRecord
    | PublicationRecord
    | LedgerRecord
    | PaymentRecord
)
Payload = dict[str, Any]
