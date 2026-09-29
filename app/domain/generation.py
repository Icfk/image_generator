"""Persistence-independent outcome of an idempotent creation request."""

from dataclasses import dataclass


@dataclass(frozen=True)
class GenerationCreation:
    """Distinguish a newly committed task from a replay without changing HTTP JSON."""

    id: str
    status: str
    created: bool

    def response(self) -> dict[str, str]:
        """Return the existing public response shape."""
        return {"id": self.id, "status": self.status}
