import pytest

from app import worker
from app.domain.entities import RetryableProviderError


class FakeWebhook:
    def __init__(self):
        self.calls = []
        self.row = type("Delivery", (), {"status": "pending", "attempts": 0})()

    async def get(self, task_id):
        return self.row

    async def deliver(self, task_id, url, payload=None):
        payload = await self.payload_loader(task_id)
        self.calls.append(payload)
        self.row.attempts += 1
        return False, self.row.attempts


class FakeGenerationService:
    def __init__(self):
        self.webhook = FakeWebhook()
        self.webhook.payload_loader = self.get_payload
        self.refunded = []

    async def run(self, task_id, webhook_url):
        raise RetryableProviderError("temporary provider failure")

    async def fail_and_refund(self, task_id, error):
        self.refunded.append((task_id, error))

    async def get_payload(self, task_id):
        return {
            "id": task_id,
            "status": "failed",
            "result": None,
            "error": "provider retries exhausted",
        }


class FakePool:
    async def enqueue_job(self, *args, **kwargs):
        return None

    async def aclose(self):
        return None


async def fake_create_pool(*_args):
    return FakePool()


@pytest.mark.asyncio
async def test_final_worker_retry_sends_failed_payload(monkeypatch):
    service = FakeGenerationService()
    monkeypatch.setattr(worker, "generation_service", service)
    monkeypatch.setattr(worker, "create_pool", fake_create_pool)

    await worker.run_generation({"job_try": 3}, "task-1", "https://example.test/hook")

    assert service.refunded == [("task-1", "provider retries exhausted")]
    assert service.webhook.calls == [
        {
            "id": "task-1",
            "status": "failed",
            "result": None,
            "error": "provider retries exhausted",
        }
    ]


@pytest.mark.asyncio
async def test_retry_delivery_uses_fresh_payload_loader(monkeypatch):
    service = FakeGenerationService()
    monkeypatch.setattr(worker, "generation_service", service)
    monkeypatch.setattr(worker, "create_pool", fake_create_pool)

    await worker.retry_delivery(
        {}, "task-2", "https://example.test/hook", {"stale": True}
    )

    assert service.webhook.calls == [await service.get_payload("task-2")]
