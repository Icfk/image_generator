"""Application transaction and rate-limiter contracts, without persistence imports."""

from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from app.domain.generation import GenerationCreation
from app.domain.payment import PaymentReceipt


class UnitOfWork(Protocol):
    """A transaction exposes repository operations, never a SQLAlchemy session."""

    users: Any
    generations: Any
    balance: Any
    payments: Any
    deliveries: Any
    publications: Any

    async def create_reserved_generation(
        self, *args: Any, **kwargs: Any
    ) -> GenerationCreation:
        """Create reservation, generation and publication atomically."""
        ...

    async def apply_payment(
        self, external_id: str, amount: int, key: str
    ) -> PaymentReceipt:
        """Apply one idempotent payment atomically."""
        ...


class UnitOfWorkFactory(Protocol):
    """Supply application UoWs independently of a concrete database."""

    def for_application(self) -> "UnitOfWorkFactory":
        """Expose only domain records to callers."""
        ...

    def create(self) -> AbstractAsyncContextManager[UnitOfWork]:
        """Begin a transaction; persist dirty records on successful exit."""
        ...


class RateLimiter(Protocol):
    """Store-independent per-user request budget."""

    async def check(self, user_id: int, limit: int = 10, window: int = 60) -> bool:
        """Return whether the request may proceed."""
        ...
