"""Queue boundary used by durable recovery use cases."""

from typing import Protocol


class RecoveryQueue(Protocol):
    """Inspect and enqueue deterministic job IDs without exposing Redis."""

    async def status(self, job_id: str) -> str:
        """Return queued/deferred/in_progress/complete/not_found."""
        ...

    async def enqueue(self, function: str, job_id: str, *args: object) -> None:
        """Ensure this job ID exists; duplicate enqueue is a successful no-op."""
        ...
