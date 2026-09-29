from datetime import datetime

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.domain.entities import TaskStatus


class Base(DeclarativeBase):
    pass


class UserModel(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    external_id: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    secret_hash: Mapped[str] = mapped_column(String(128))
    balance: Mapped[int] = mapped_column(Integer, default=0)


class GenerationModel(Base):
    __tablename__ = "generations"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "idempotency_key", name="uq_generation_idempotency"
        ),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[int] = mapped_column(index=True)
    kind: Mapped[str] = mapped_column(String(20))
    prompt: Mapped[str] = mapped_column(Text)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[TaskStatus] = mapped_column(
        SAEnum(TaskStatus, native_enum=False, length=20), default=TaskStatus.created
    )
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    cost: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    queued_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    processing_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    provider_job_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    webhook_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    provider_context: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class BalanceTransactionModel(Base):
    __tablename__ = "balance_transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(index=True)
    generation_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True
    )
    external_key: Mapped[str | None] = mapped_column(
        String(200), nullable=True, unique=True
    )
    operation: Mapped[str] = mapped_column(String(20))
    amount: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class PaymentEventModel(Base):
    __tablename__ = "payment_events"
    id: Mapped[int] = mapped_column(primary_key=True)
    external_user_id: Mapped[str] = mapped_column(String(100), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(200), unique=True)
    amount: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class WebhookDeliveryModel(Base):
    __tablename__ = "webhook_deliveries"
    id: Mapped[int] = mapped_column(primary_key=True)
    generation_id: Mapped[str] = mapped_column(String(36), unique=True)
    url: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    retry_job_id: Mapped[str | None] = mapped_column(String(150), nullable=True)
    recovery_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    preparation_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class JobPublicationModel(Base):
    __tablename__ = "job_publications"
    id: Mapped[int] = mapped_column(primary_key=True)
    generation_id: Mapped[str] = mapped_column(String(36), unique=True)
    job_id: Mapped[str] = mapped_column(String(100), unique=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
