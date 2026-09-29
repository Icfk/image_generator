"""Payment use-case results without ORM dependencies."""

from dataclasses import dataclass


class PaymentConflict(ValueError):
    """An external payment key belongs to another user or amount."""


@dataclass(frozen=True)
class PaymentReceipt:
    """Committed payment response, preserving the existing HTTP contract."""

    external_user_id: str
    balance: int
    duplicate: bool

    def response(self) -> dict[str, str | int | bool]:
        """Return the stable API response fields."""
        return {
            "external_user_id": self.external_user_id,
            "balance": self.balance,
            "duplicate": self.duplicate,
        }
