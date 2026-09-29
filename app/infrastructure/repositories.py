"""Database repositories used by application services."""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.entities import TaskStatus
from app.domain.generation import GenerationCreation
from app.domain.payment import PaymentConflict, PaymentReceipt
from app.infrastructure.models import (
    BalanceTransactionModel,
    GenerationModel,
    JobPublicationModel,
    PaymentEventModel,
    UserModel,
    WebhookDeliveryModel,
)


def secret_hash(value: str) -> str:
    """Return the one-way representation used for API keys."""
    return hashlib.sha256(value.encode()).hexdigest()


class UserRepository:
    """Persistence operations for users."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, user_id: int, for_update: bool = False) -> UserModel | None:
        query = select(UserModel).where(UserModel.id == user_id)
        if for_update:
            query = query.with_for_update()
        return (await self.session.execute(query)).scalar_one_or_none()

    async def by_external_id(
        self, external_id: str, *, for_update: bool = False
    ) -> UserModel | None:
        """Find a user, optionally serializing all balance changes on their row."""
        query = select(UserModel).where(UserModel.external_id == external_id)
        if for_update:
            query = query.with_for_update()
        return (await self.session.execute(query)).scalar_one_or_none()

    async def by_api_key(self, api_key: str) -> UserModel | None:
        query = select(UserModel).where(UserModel.secret_hash == secret_hash(api_key))
        return (await self.session.execute(query)).scalar_one_or_none()

    async def add(self, user: UserModel) -> None:
        self.session.add(user)
        await self.session.flush()

    async def create(self, external_id: str, secret_hash_value: str) -> UserModel:
        user = UserModel(
            external_id=external_id,
            secret_hash=secret_hash_value,
            balance=0,
        )
        await self.add(user)
        return user


class GenerationRepository:
    """Persistence operations for generation jobs."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, task_id: str) -> GenerationModel | None:
        return await self.session.get(GenerationModel, task_id)

    async def lock(
        self, task_id: str, *, skip_locked: bool = False
    ) -> GenerationModel | None:
        """Lock the generation before lifecycle or recovery changes."""
        query = (
            select(GenerationModel)
            .where(GenerationModel.id == task_id)
            .with_for_update(skip_locked=skip_locked)
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def claim(
        self, task_id: str, token: str, now: datetime
    ) -> GenerationModel | None:
        """Atomically start a queued task; terminal tasks cannot be restarted."""
        query = (
            update(GenerationModel)
            .where(
                GenerationModel.id == task_id,
                GenerationModel.status == TaskStatus.queued,
            )
            .values(status=TaskStatus.processing, processing_at=now, run_token=token)
            .returning(GenerationModel)
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def checkpoint(self, task_id: str, token: str, context: dict) -> bool:
        """Persist submission identity only while this worker owns the task."""
        result = await self.session.execute(
            update(GenerationModel)
            .where(
                GenerationModel.id == task_id,
                GenerationModel.run_token == token,
                GenerationModel.status == TaskStatus.processing,
            )
            .values(provider_job_id=context["request_id"], provider_context=context)
        )
        return result.rowcount == 1

    async def recovery_candidates(
        self, now: datetime, cutoff: datetime, limit: int
    ) -> list[str]:
        """Scan queued work (including lost Redis jobs) and expired processing."""
        query = (
            select(GenerationModel.id)
            .outerjoin(
                JobPublicationModel,
                JobPublicationModel.generation_id == GenerationModel.id,
            )
            .where(
                or_(
                    and_(
                        GenerationModel.status == TaskStatus.queued,
                        or_(
                            JobPublicationModel.next_attempt_at.is_(None),
                            JobPublicationModel.next_attempt_at <= now,
                        ),
                    ),
                    and_(
                        GenerationModel.status == TaskStatus.processing,
                        or_(
                            GenerationModel.processing_at.is_(None),
                            GenerationModel.processing_at <= cutoff,
                        ),
                    ),
                )
            )
            .order_by(
                JobPublicationModel.next_attempt_at.asc().nulls_first(),
                GenerationModel.created_at,
            )
            .limit(limit)
        )
        return list((await self.session.execute(query)).scalars())

    async def terminal_without_delivery(self, limit: int) -> list[str]:
        """Find the crash window between committing a result and delivery creation."""
        query = (
            select(GenerationModel.id)
            .outerjoin(
                WebhookDeliveryModel,
                WebhookDeliveryModel.generation_id == GenerationModel.id,
            )
            .where(
                GenerationModel.status.in_((TaskStatus.completed, TaskStatus.failed)),
                GenerationModel.webhook_url.is_not(None),
                WebhookDeliveryModel.id.is_(None),
            )
            .limit(limit)
        )
        return list((await self.session.execute(query)).scalars())

    async def get_for_user(self, task_id: str, user_id: int) -> GenerationModel | None:
        query = select(GenerationModel).where(
            GenerationModel.id == task_id, GenerationModel.user_id == user_id
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def get_by_idempotency_key(
        self, user_id: int, idempotency_key: str
    ) -> GenerationModel | None:
        query = select(GenerationModel).where(
            GenerationModel.user_id == user_id,
            GenerationModel.idempotency_key == idempotency_key,
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def add(self, task: GenerationModel) -> None:
        self.session.add(task)
        await self.session.flush()

    async def create(self, **values: Any) -> GenerationModel:
        task = GenerationModel(**values)
        await self.add(task)
        return task

    async def save(self, task: GenerationModel) -> None:
        await self.session.flush()


class BalanceRepository:
    """Atomic balance reservation and ledger operations."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def reserve(self, user_id: int, cost: int) -> UserModel:
        user = await UserRepository(self.session).get(user_id, for_update=True)
        if user is None or user.balance < cost:
            raise ValueError("Insufficient balance")
        user.balance -= cost
        return user

    async def has_external_key(self, external_key: str) -> bool:
        query = select(BalanceTransactionModel).where(
            BalanceTransactionModel.external_key == external_key
        )
        return (await self.session.execute(query)).scalar_one_or_none() is not None

    async def add_transaction(self, **values: Any) -> BalanceTransactionModel:
        transaction = BalanceTransactionModel(**values)
        self.session.add(transaction)
        await self.session.flush()
        return transaction

    async def refund_once(
        self, user_id: int, generation_id: str, amount: int, external_key: str
    ) -> bool:
        user = await UserRepository(self.session).get(user_id, for_update=True)
        if user is None:
            raise ValueError("User not found")
        if await self.has_external_key(external_key):
            return False
        try:
            async with self.session.begin_nested():
                user.balance += amount
                await self.add_transaction(
                    user_id=user_id,
                    generation_id=generation_id,
                    operation="REFUND",
                    amount=amount,
                    external_key=external_key,
                )
        except IntegrityError as exc:
            original = exc.orig
            cause = getattr(original, "__cause__", None)
            constraint = getattr(original, "constraint_name", None) or getattr(
                cause, "constraint_name", None
            )
            duplicate = constraint == "balance_transactions_external_key_key" or (
                "UNIQUE constraint failed: balance_transactions.external_key"
                in str(original)
            )
            # A savepoint protects balance, but unrelated failures must also
            # abort the outer failed-status transaction. Verify the winner.
            if not duplicate or not await self.has_external_key(external_key):
                raise
            return False
        return True


class PaymentRepository:
    """Persistence operations for idempotent payment events."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def is_key_conflict(exc: IntegrityError) -> bool:
        """Recognize only the payment-key constraint, never unrelated failures."""
        original = exc.orig
        cause = getattr(original, "__cause__", None)
        constraint = getattr(original, "constraint_name", None) or getattr(
            cause, "constraint_name", None
        )
        return constraint == "payment_events_idempotency_key_key" or (
            "UNIQUE constraint failed: payment_events.idempotency_key" in str(original)
        )

    async def get_by_key(self, key: str) -> PaymentEventModel | None:
        query = select(PaymentEventModel).where(
            PaymentEventModel.idempotency_key == key
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def add(self, event: PaymentEventModel) -> None:
        self.session.add(event)
        await self.session.flush()

    async def create(
        self, external_user_id: str, key: str, amount: int
    ) -> PaymentEventModel:
        event = PaymentEventModel(
            external_user_id=external_user_id,
            idempotency_key=key,
            amount=amount,
        )
        await self.add(event)
        return event


class WebhookDeliveryRepository:
    """Persistence operations for webhook delivery state."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_generation(
        self, generation_id: str
    ) -> WebhookDeliveryModel | None:
        query = select(WebhookDeliveryModel).where(
            WebhookDeliveryModel.generation_id == generation_id
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def add(self, delivery: WebhookDeliveryModel) -> None:
        self.session.add(delivery)
        await self.session.flush()

    async def lock(self, generation_id: str) -> WebhookDeliveryModel | None:
        """Skip rows owned by another recovery publisher or delivery worker."""
        query = (
            select(WebhookDeliveryModel)
            .where(WebhookDeliveryModel.generation_id == generation_id)
            .with_for_update(skip_locked=True)
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def due_ids(self, now: datetime, limit: int) -> list[str]:
        """Return only due pending deliveries, never uncertain sending rows."""
        query = (
            select(WebhookDeliveryModel.generation_id)
            .where(
                WebhookDeliveryModel.status == "pending",
                WebhookDeliveryModel.attempts < 5,
                or_(
                    WebhookDeliveryModel.next_attempt_at.is_(None),
                    WebhookDeliveryModel.next_attempt_at <= now,
                ),
            )
            .order_by(
                WebhookDeliveryModel.recovery_checked_at.asc().nulls_first(),
                WebhookDeliveryModel.id,
            )
            .limit(limit)
        )
        return list((await self.session.execute(query)).scalars())

    async def get_or_create(self, generation_id: str, url: str) -> WebhookDeliveryModel:
        """Insert under a savepoint; a concurrent winner keeps its original URL."""
        delivery = WebhookDeliveryModel(
            generation_id=generation_id, url=url, status="pending", attempts=0
        )
        try:
            async with self.session.begin_nested():
                await self.add(delivery)
        except IntegrityError:
            delivery = await self.get_by_generation(generation_id)
            if delivery is None:
                raise
        return delivery

    async def claim_attempt(
        self,
        generation_id: str,
        now: datetime,
        max_attempts: int,
        preparation_seconds: int = 30,
    ) -> WebhookDeliveryModel | None:
        """CAS the due pending row into preparing and increment atomically.

        The caller must commit before performing HTTP. The attempt number is
        also a fencing token for completion; no lease can expire mid-request.
        """
        statement = (
            update(WebhookDeliveryModel)
            .where(
                WebhookDeliveryModel.generation_id == generation_id,
                WebhookDeliveryModel.status == "pending",
                WebhookDeliveryModel.attempts < max_attempts,
                or_(
                    WebhookDeliveryModel.next_attempt_at.is_(None),
                    WebhookDeliveryModel.next_attempt_at <= now,
                ),
            )
            .values(
                status="preparing",
                preparation_expires_at=now + timedelta(seconds=preparation_seconds),
                attempts=WebhookDeliveryModel.attempts + 1,
                next_attempt_at=None,
                retry_job_id=None,
            )
            .returning(WebhookDeliveryModel)
        )
        return (await self.session.execute(statement)).scalar_one_or_none()

    async def begin_sending(
        self, generation_id: str, attempt: int, now: datetime
    ) -> bool:
        """Fence expired preparation owners before granting HTTP permission."""
        statement = (
            update(WebhookDeliveryModel)
            .where(
                WebhookDeliveryModel.generation_id == generation_id,
                WebhookDeliveryModel.status == "preparing",
                WebhookDeliveryModel.attempts == attempt,
                WebhookDeliveryModel.preparation_expires_at > now,
            )
            .values(status="sending", preparation_expires_at=None)
            .returning(WebhookDeliveryModel.id)
        )
        return (await self.session.execute(statement)).scalar_one_or_none() is not None

    async def recover_preparations(
        self, now: datetime, max_attempts: int, interval: int, limit: int
    ) -> int:
        """Repair expired pre-HTTP claims without stealing uncertain HTTP sends."""
        query = (
            select(WebhookDeliveryModel)
            .where(
                WebhookDeliveryModel.status == "preparing",
                WebhookDeliveryModel.preparation_expires_at <= now,
            )
            .order_by(WebhookDeliveryModel.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = list((await self.session.execute(query)).scalars())
        recovered = 0
        for row in rows:
            status = "failed" if row.attempts >= max_attempts else "pending"
            result = await self.session.execute(
                update(WebhookDeliveryModel)
                .where(
                    WebhookDeliveryModel.id == row.id,
                    WebhookDeliveryModel.status == "preparing",
                    WebhookDeliveryModel.attempts == row.attempts,
                    WebhookDeliveryModel.preparation_expires_at <= now,
                )
                .values(
                    status=status,
                    preparation_expires_at=None,
                    last_error="Webhook preparation expired before HTTP",
                    next_attempt_at=(
                        now + timedelta(seconds=interval)
                        if status == "pending"
                        else None
                    ),
                )
                .returning(WebhookDeliveryModel.id)
                .execution_options(synchronize_session=False)
            )
            recovered += result.scalar_one_or_none() is not None
        return recovered

    async def finish_attempt(
        self,
        generation_id: str,
        attempt: int,
        status: str,
        error: str | None,
        next_attempt_at: datetime | None,
    ) -> None:
        """Only the owner of this sending attempt may persist its outcome."""
        await self.session.execute(
            update(WebhookDeliveryModel)
            .where(
                WebhookDeliveryModel.generation_id == generation_id,
                WebhookDeliveryModel.status.in_(("preparing", "sending")),
                WebhookDeliveryModel.attempts == attempt,
            )
            .values(
                status=status,
                last_error=error,
                next_attempt_at=next_attempt_at,
                preparation_expires_at=None,
            )
        )

    async def create_if_missing(
        self, generation_id: str, url: str
    ) -> WebhookDeliveryModel:
        """Backward-compatible alias for get_or_create."""
        return await self.get_or_create(generation_id, url)

    async def update_attempt(
        self, generation_id: str, **values: Any
    ) -> WebhookDeliveryModel:
        """Persist one delivery attempt and its next state."""
        return await self.update(generation_id, **values)

    async def get_pending(self) -> list[WebhookDeliveryModel]:
        """Return deliveries that can be retried."""
        query = select(WebhookDeliveryModel).where(
            WebhookDeliveryModel.status == "pending"
        )
        return list((await self.session.execute(query)).scalars().all())

    async def update(self, generation_id: str, **values: Any) -> WebhookDeliveryModel:
        delivery = await self.get_by_generation(generation_id)
        if delivery is None:
            raise ValueError("Delivery not found")
        for name, value in values.items():
            setattr(delivery, name, value)
        await self.session.flush()
        return delivery


class JobPublicationRepository:
    """Persist the state of publishing a generation job to Redis."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, generation_id: str) -> JobPublicationModel:
        job = JobPublicationModel(
            generation_id=generation_id,
            job_id=f"generation:{generation_id}",
            status="pending",
            attempts=0,
        )
        self.session.add(job)
        await self.session.flush()
        return job

    async def get(self, generation_id: str) -> JobPublicationModel | None:
        query = select(JobPublicationModel).where(
            JobPublicationModel.generation_id == generation_id
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def update(self, generation_id: str, **values: Any) -> JobPublicationModel:
        job = await self.get(generation_id)
        if job is None:
            raise ValueError("Job publication not found")
        for name, value in values.items():
            setattr(job, name, value)
        await self.session.flush()
        return job

    async def record_attempt(
        self, generation_id: str, status: str, error: str | None
    ) -> None:
        """Persist publication acknowledgement with one atomic update."""
        await self.session.execute(
            update(JobPublicationModel)
            .where(JobPublicationModel.generation_id == generation_id)
            .values(
                status=status,
                attempts=JobPublicationModel.attempts + 1,
                last_error=error,
            )
        )

    async def get_recoverable(self) -> list[JobPublicationModel]:
        query = select(JobPublicationModel).where(
            JobPublicationModel.status.in_(("pending", "unknown"))
        )
        return list((await self.session.execute(query)).scalars().all())


class UnitOfWork:
    """Own one SQLAlchemy session and its transaction boundary."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.users = UserRepository(session)
        self.generations = GenerationRepository(session)
        self.balance = BalanceRepository(session)
        self.payments = PaymentRepository(session)
        self.deliveries = WebhookDeliveryRepository(session)
        self.publications = JobPublicationRepository(session)

    async def apply_payment(
        self, external_id: str, amount: int, key: str
    ) -> PaymentReceipt:
        """Commit one event, deposit and balance change or validate a replay.

        The unique insert happens first: concurrent requests wait for its outcome
        before they may touch a balance. A row lock also serializes different
        payments, reserves and refunds for the same user.
        """
        try:
            async with self.session.begin_nested():
                await self.payments.create(external_id, key, amount)
                user = await self.users.by_external_id(external_id, for_update=True)
                if user is None:
                    raise LookupError("User not found")
                user.balance += amount
                await self.balance.add_transaction(
                    user_id=user.id,
                    operation="DEPOSIT",
                    amount=amount,
                    external_key=f"deposit:{key}",
                )
                receipt = PaymentReceipt(user.external_id, user.balance, False)
        except IntegrityError as exc:
            if not self.payments.is_key_conflict(exc):
                raise
            event = await self.payments.get_by_key(key)
            if event is None:
                raise
            # Validate ownership BEFORE looking up or returning any balance.
            if event.external_user_id != external_id or event.amount != amount:
                raise PaymentConflict("Idempotency key conflict") from exc
            user = await self.users.by_external_id(external_id)
            if user is None:
                raise LookupError("User not found") from exc
            return PaymentReceipt(user.external_id, user.balance, True)
        return receipt

    async def create_reserved_generation(
        self,
        user_id: int,
        kind: str,
        prompt: str,
        image_url: str | None,
        idempotency_key: str | None,
        webhook_url: str | None = None,
    ) -> GenerationCreation:
        """Atomically create a task, reserve and outbox, or read the winning insert.

        Insert before reserving: PostgreSQL's unique constraint waits for the
        competing transaction, even when the user has only one remaining token.
        Any failure rolls back the whole savepoint, including the balance change.
        """
        task_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        try:
            async with self.session.begin_nested():
                task = await self.generations.create(
                    id=task_id,
                    user_id=user_id,
                    kind=kind,
                    prompt=prompt,
                    image_url=image_url,
                    idempotency_key=idempotency_key,
                    status=TaskStatus.created,
                    cost=1,
                    created_at=now,
                    webhook_url=webhook_url,
                )
                try:
                    await self.balance.reserve(user_id, task.cost)
                except ValueError as exc:
                    raise PermissionError(str(exc)) from exc
                await self.balance.add_transaction(
                    user_id=user_id,
                    generation_id=task.id,
                    operation="RESERVE",
                    amount=-task.cost,
                    external_key=f"reserve:{task.id}",
                )
                task.status = TaskStatus.queued
                task.queued_at = now
                await self.publications.create(task.id)
        except IntegrityError as exc:
            # Do not disguise unrelated FK, ledger or outbox integrity errors.
            original = exc.orig
            diagnostic = getattr(original, "__cause__", original)
            constraint = getattr(diagnostic, "constraint_name", None)
            sqlite_duplicate = (
                "UNIQUE constraint failed: generations.user_id, "
                "generations.idempotency_key"
            ) in str(original)
            if idempotency_key is None or not (
                constraint == "uq_generation_idempotency" or sqlite_duplicate
            ):
                raise
            existing = await self.generations.get_by_idempotency_key(
                user_id, idempotency_key
            )
            if existing is None:
                raise
            return GenerationCreation(existing.id, existing.status.value, False)
        return GenerationCreation(task.id, task.status.value, True)

    async def __aenter__(self) -> "UnitOfWork":
        await self.session.begin()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type:
            await self.session.rollback()
        else:
            await self.session.commit()
