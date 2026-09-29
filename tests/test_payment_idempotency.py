"""Payment API tests with real SQLAlchemy transactions and concurrent sessions."""

import asyncio
import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import OperationalError

from app.application.services import UserService
from app.infrastructure.config import settings
from app.infrastructure.models import (
    BalanceTransactionModel,
    PaymentEventModel,
    UserModel,
)
from app.infrastructure.repositories import PaymentRepository, UnitOfWork
from app.main import app
from app.presentation import router as routes
from app.presentation.schemas import TopUpRequest


@pytest.fixture
async def payment_case(database, monkeypatch) -> AsyncIterator[SimpleNamespace]:
    """Keep both users and every HTTP request in the fixture's isolated DB."""
    service = UserService(database.factory)
    monkeypatch.setattr(routes, "user_service", service)
    monkeypatch.setattr(settings, "require_webhook_signature", False)
    async with database.factory.create() as uow:
        other = await uow.users.create("other-user", "other-hash")
        other.balance = 50
        other_id = other.id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        yield SimpleNamespace(
            database=database,
            service=service,
            client=client,
            other_id=other_id,
            payload={
                "external_user_id": "user",
                "amount": 5,
                "transaction_id": "pay-1",
            },
        )


async def assert_payment_state(case, *, credited: bool = True) -> None:
    """Read committed balance/event/ledger state from a fresh session."""
    async with case.database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == (6 if credited else 1)
        assert (await session.get(UserModel, case.other_id)).balance == 50
        for model in (PaymentEventModel, BalanceTransactionModel):
            assert await session.scalar(select(func.count()).select_from(model)) == int(
                credited
            )
        if credited:
            event = await session.scalar(select(PaymentEventModel))
            assert (event.idempotency_key, event.external_user_id, event.amount) == (
                "pay-1",
                "user",
                5,
            )
            ledger = await session.scalar(select(BalanceTransactionModel))
            assert (
                ledger.user_id,
                ledger.external_key,
                ledger.operation,
                ledger.amount,
            ) == (1, "deposit:pay-1", "DEPOSIT", 5)


@pytest.mark.parametrize(
    "keys",
    [
        {},
        {"transaction_id": None},
        {"transaction_id": ""},
        {"idempotency_key": "  "},
        {"idempotency_key": "x" * 193},
    ],
)
async def test_missing_or_invalid_external_key(payment_case, keys) -> None:
    case = payment_case
    response = await case.client.post(
        "/webhooks/top-up", json={"external_user_id": "user", "amount": 5, **keys}
    )
    assert response.status_code == 422
    await assert_payment_state(case, credited=False)


async def test_service_never_generates_a_missing_key(payment_case) -> None:
    case = payment_case
    with pytest.raises(ValueError, match="required"):
        await case.service.top_up("user", 5, None)
    await assert_payment_state(case, credited=False)


async def test_duplicate_webhook_after_commit_and_key_alias(payment_case) -> None:
    case = payment_case
    first = await case.client.post("/webhooks/top-up", json=case.payload)
    replay = {"external_user_id": "user", "amount": 5, "idempotency_key": "pay-1"}
    second = await case.client.post("/webhooks/top-up", json=replay)
    assert first.status_code == second.status_code == 200
    assert first.json() == {
        "external_user_id": "user",
        "balance": 6,
        "duplicate": False,
    }
    assert second.json() == {
        "external_user_id": "user",
        "balance": 6,
        "duplicate": True,
    }
    await assert_payment_state(case)


@pytest.mark.parametrize(
    "change",
    [
        {"external_user_id": "other-user"},
        {"external_user_id": "missing-user"},
        {"amount": 10},
    ],
)
async def test_conflicts_do_not_leak_balance(payment_case, change) -> None:
    case = payment_case
    await case.client.post("/webhooks/top-up", json=case.payload)
    response = await case.client.post(
        "/webhooks/top-up", json={**case.payload, **change}
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "Idempotency key conflict"}
    await assert_payment_state(case)


async def test_concurrent_duplicate_webhooks(payment_case, monkeypatch) -> None:
    case = payment_case
    reads = []
    original = PaymentRepository.get_by_key

    async def reread(self, key):
        reads.append(key)
        return await original(self, key)

    monkeypatch.setattr(PaymentRepository, "get_by_key", reread)
    responses = await asyncio.gather(
        *(case.client.post("/webhooks/top-up", json=case.payload) for _ in range(4))
    )
    assert all(response.status_code == 200 for response in responses)
    assert sorted(response.json()["duplicate"] for response in responses) == [
        False,
        True,
        True,
        True,
    ]
    assert reads == ["pay-1"] * 3
    await assert_payment_state(case)


async def test_loser_waits_for_first_payment_commit(payment_case, monkeypatch) -> None:
    case = payment_case
    inserted, release = asyncio.Event(), asyncio.Event()
    original = PaymentRepository.create
    first_insert = True

    async def hold(self, *args, **kwargs):
        nonlocal first_insert
        event = await original(self, *args, **kwargs)
        if first_insert:
            first_insert = False
            inserted.set()
            await release.wait()
        return event

    monkeypatch.setattr(PaymentRepository, "create", hold)
    first = asyncio.create_task(case.service.top_up("user", 5, "pay-1"))
    second = None
    try:
        await asyncio.wait_for(inserted.wait(), 2)
        second = asyncio.create_task(case.service.top_up("user", 5, "pay-1"))
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(second), 0.05)
    finally:
        release.set()
        await first
        if second is not None:
            await second
    assert not first.result()["duplicate"]
    assert second.result()["duplicate"]
    await assert_payment_state(case)


async def test_different_payments_do_not_lose_balance_updates(payment_case) -> None:
    case = payment_case
    responses = await asyncio.gather(
        case.service.top_up("user", 3, "first"),
        case.service.top_up("user", 7, "second"),
    )
    assert not any(response["duplicate"] for response in responses)
    async with case.database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == 11
        assert (
            await session.scalar(select(func.count()).select_from(PaymentEventModel))
            == 2
        )


async def test_concurrent_conflicting_users_credit_only_winner(payment_case) -> None:
    case = payment_case
    responses = await asyncio.gather(
        case.client.post("/webhooks/top-up", json=case.payload),
        case.client.post(
            "/webhooks/top-up", json={**case.payload, "external_user_id": "other-user"}
        ),
    )
    assert sorted(response.status_code for response in responses) == [200, 409]
    async with case.database.sessions() as session:
        first_balance = (await session.get(UserModel, 1)).balance
        other_balance = (await session.get(UserModel, case.other_id)).balance
        assert (first_balance, other_balance) in ((6, 50), (1, 55))
        assert (
            await session.scalar(select(func.count()).select_from(PaymentEventModel))
            == 1
        )


async def test_database_error_before_commit_rolls_back_everything(
    payment_case, monkeypatch
) -> None:
    case = payment_case
    original = UnitOfWork.__aexit__
    failing = True

    async def fail_before_commit(self, exc_type, exc, traceback):
        nonlocal failing
        if exc_type is None and failing:
            failing = False
            # Genuine PostgreSQL/SQLite statement failure after flushing payment
            # changes but BEFORE COMMIT; PostgreSQL marks the transaction aborted.
            await self.session.execute(
                text("SELECT * FROM payment_test_missing_relation")
            )
        return await original(self, exc_type, exc, traceback)

    monkeypatch.setattr(UnitOfWork, "__aexit__", fail_before_commit)
    response = await case.client.post("/webhooks/top-up", json=case.payload)
    assert response.status_code == 500
    await assert_payment_state(case, credited=False)
    replay = await case.client.post("/webhooks/top-up", json=case.payload)
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is False
    await assert_payment_state(case)


@pytest.mark.parametrize("committed", [False, True])
async def test_retry_after_unknown_commit_result(
    payment_case, monkeypatch, committed
) -> None:
    case = payment_case
    original = UnitOfWork.__aexit__
    failing = True

    async def lose_commit_ack(self, exc_type, exc, traceback):
        nonlocal failing
        if exc_type is None and failing:
            failing = False
            if committed:
                await original(self, exc_type, exc, traceback)
            else:
                await self.session.rollback()
            raise OperationalError(
                "COMMIT",
                {},
                ConnectionResetError("commit acknowledgement lost"),
                connection_invalidated=True,
            )
        return await original(self, exc_type, exc, traceback)

    monkeypatch.setattr(UnitOfWork, "__aexit__", lose_commit_ack)
    response = await case.client.post("/webhooks/top-up", json=case.payload)
    assert response.status_code == 500
    await assert_payment_state(case, credited=committed)
    replay = await case.client.post("/webhooks/top-up", json=case.payload)
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is committed
    await assert_payment_state(case)


async def test_savepoint_preserves_outer_transaction_on_duplicate(payment_case) -> None:
    case = payment_case
    await case.service.top_up("user", 5, "pay-1")
    async with case.database.factory.create() as uow:
        await uow.users.create("outer-transaction", "outer-hash")
        receipt = await uow.apply_payment("user", 5, "pay-1")
        assert receipt.duplicate
    async with case.database.factory.create() as uow:
        assert await uow.users.by_external_id("outer-transaction") is not None
    await assert_payment_state(case)


async def test_unrelated_ledger_integrity_error_is_not_duplicate(payment_case) -> None:
    case = payment_case
    async with case.database.factory.create() as uow:
        await uow.balance.add_transaction(
            user_id=case.other_id,
            operation="DEPOSIT",
            amount=50,
            external_key="deposit:pay-1",
        )
    response = await case.client.post("/webhooks/top-up", json=case.payload)
    assert response.status_code == 500
    async with case.database.sessions() as session:
        assert (await session.get(UserModel, 1)).balance == 1
        assert (
            await session.scalar(select(func.count()).select_from(PaymentEventModel))
            == 0
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(BalanceTransactionModel)
            )
            == 1
        )


async def test_unknown_user_does_not_leave_an_event(payment_case) -> None:
    case = payment_case
    response = await case.client.post(
        "/webhooks/top-up", json={**case.payload, "external_user_id": "unknown"}
    )
    assert response.status_code == 404
    await assert_payment_state(case, credited=False)


async def test_required_signature_preserves_existing_canonical_format(
    payment_case, monkeypatch
) -> None:
    case = payment_case
    monkeypatch.setattr(settings, "require_webhook_signature", True)
    monkeypatch.setattr(settings, "auth_secret", "test-webhook-secret")
    body = TopUpRequest(**case.payload).model_dump()
    signature = hmac.new(
        b"test-webhook-secret",
        json.dumps(body, sort_keys=True).encode(),
        hashlib.sha256,
    ).hexdigest()
    for headers in ({}, {"X-Webhook-Signature": "invalid"}):
        response = await case.client.post(
            "/webhooks/top-up", json=case.payload, headers=headers
        )
        assert response.status_code == 401
    await assert_payment_state(case, credited=False)
    for duplicate in (False, True):
        response = await case.client.post(
            "/webhooks/top-up",
            json=case.payload,
            headers={"X-Webhook-Signature": signature},
        )
        assert response.status_code == 200
        assert response.json()["duplicate"] is duplicate
    changed = await case.client.post(
        "/webhooks/top-up",
        json={**case.payload, "amount": 9},
        headers={"X-Webhook-Signature": signature},
    )
    assert changed.status_code == 401
    await assert_payment_state(case)


async def test_disabled_signature_and_both_keys_keep_existing_precedence(
    payment_case,
) -> None:
    case = payment_case
    response = await case.client.post(
        "/webhooks/top-up",
        json={
            **case.payload,
            "transaction_id": "secondary",
            "idempotency_key": "pay-1",
        },
        headers={"X-Webhook-Signature": "ignored-when-disabled"},
    )
    assert response.status_code == 200
    await assert_payment_state(case)
