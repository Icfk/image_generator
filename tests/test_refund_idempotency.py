import asyncio

import pytest


class FakeRefundLedger:
    """Small deterministic model of the repository refund contract."""

    def __init__(self, balance=0):
        self.balance = balance
        self.keys = set()
        self.lock = asyncio.Lock()
        self.fail_insert = False

    async def refund_once(self, key, amount):
        async with self.lock:
            if key in self.keys:
                return False
            if self.fail_insert:
                self.fail_insert = False
                raise RuntimeError("duplicate ledger insert")
            self.balance += amount
            self.keys.add(key)
            return True


@pytest.mark.asyncio
async def test_two_parallel_refunds_change_balance_once():
    ledger = FakeRefundLedger(balance=0)
    results = await asyncio.gather(
        ledger.refund_once("refund:generation-1", 1),
        ledger.refund_once("refund:generation-1", 1),
    )

    assert sorted(results) == [False, True]
    assert ledger.balance == 1
    assert len(ledger.keys) == 1


@pytest.mark.asyncio
async def test_reprocessing_failed_generation_is_idempotent():
    ledger = FakeRefundLedger(balance=0)

    await ledger.refund_once("refund:generation-2", 1)
    await ledger.refund_once("refund:generation-2", 1)
    await ledger.refund_once("refund:generation-2", 1)

    assert ledger.balance == 1


@pytest.mark.asyncio
async def test_duplicate_refund_key_does_not_change_balance():
    ledger = FakeRefundLedger(balance=4)
    ledger.keys.add("refund:generation-3")

    created = await ledger.refund_once("refund:generation-3", 1)

    assert created is False
    assert ledger.balance == 4


@pytest.mark.asyncio
async def test_failed_ledger_insert_does_not_change_balance():
    ledger = FakeRefundLedger(balance=7)
    ledger.fail_insert = True

    with pytest.raises(RuntimeError):
        await ledger.refund_once("refund:generation-4", 1)

    assert ledger.balance == 7
    assert "refund:generation-4" not in ledger.keys
