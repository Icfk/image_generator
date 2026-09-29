"""Data mapper separating application records from SQLAlchemy's identity map."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import fields
from typing import Any

from app.domain.records import (
    DeliveryRecord,
    GenerationRecord,
    LedgerRecord,
    PaymentRecord,
    PublicationRecord,
    Record,
    UserRecord,
)
from app.infrastructure.models import (
    BalanceTransactionModel,
    GenerationModel,
    JobPublicationModel,
    PaymentEventModel,
    UserModel,
    WebhookDeliveryModel,
)

RECORD_TYPES = {
    UserModel: UserRecord,
    GenerationModel: GenerationRecord,
    WebhookDeliveryModel: DeliveryRecord,
    JobPublicationModel: PublicationRecord,
    BalanceTransactionModel: LedgerRecord,
    PaymentEventModel: PaymentRecord,
}


class RecordMap:
    """Track domain edits; never write unchanged fields over repository SQL updates."""

    def __init__(self) -> None:
        self.entries: dict[tuple[type, Any], tuple[Any, Record, dict]] = {}

    def convert(self, value: Any) -> Any:
        """Translate ORM results to detached domain records within one UoW."""
        if isinstance(value, list):
            return [self.convert(item) for item in value]
        record_type = RECORD_TYPES.get(type(value))
        if record_type is None:
            return value
        key = (type(value), value.id)
        if key not in self.entries:
            data = {
                field.name: deepcopy(getattr(value, field.name))
                for field in fields(record_type)
            }
            record = record_type(**deepcopy(data))
            self.entries[key] = value, record, data
        return self.entries[key][1]

    def apply_changes(self) -> None:
        """Copy domain edits before the surrounding transaction commits."""
        for model, record, baseline in self.entries.values():
            for name, original in baseline.items():
                value = getattr(record, name)
                if value != original:
                    setattr(model, name, deepcopy(value))


class RepositoryAdapter:
    """Keep repository method contracts while mapping every returned entity."""

    def __init__(self, repository: Any, records: RecordMap) -> None:
        self._repository = repository
        self._records = records

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_") or name == "session":
            raise AttributeError(name)
        method = getattr(self._repository, name)
        if not callable(method):
            raise AttributeError(name)

        async def invoke(*args: Any, **kwargs: Any) -> Any:
            return self._records.convert(await method(*args, **kwargs))

        return invoke


class ApplicationUnitOfWork:
    """Expose domain records and atomic use-case operations, but no ORM session."""

    def __init__(self, persistence: Any, records: RecordMap) -> None:
        self._persistence = persistence
        for name in (
            "users",
            "generations",
            "balance",
            "payments",
            "deliveries",
            "publications",
        ):
            setattr(self, name, RepositoryAdapter(getattr(persistence, name), records))

    async def create_reserved_generation(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate the atomic reservation operation to persistence."""
        return await self._persistence.create_reserved_generation(*args, **kwargs)

    async def apply_payment(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate one transactional payment operation to persistence."""
        return await self._persistence.apply_payment(*args, **kwargs)


class ApplicationUnitOfWorkFactory:
    """Adapt the existing UoW without changing its transaction/locking rules."""

    def __init__(self, persistence_factory: Any) -> None:
        self.persistence_factory = persistence_factory

    def for_application(self) -> "ApplicationUnitOfWorkFactory":
        """Already exposes only application records."""
        return self

    @asynccontextmanager
    async def create(self) -> AsyncIterator[ApplicationUnitOfWork]:
        """Discard record edits on errors; apply them before a successful commit."""
        async with self.persistence_factory.create() as persistence:
            records = RecordMap()
            yield ApplicationUnitOfWork(persistence, records)
            records.apply_changes()
