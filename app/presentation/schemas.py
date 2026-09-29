from pydantic import BaseModel, Field, field_validator, model_validator

from app.domain.webhooks import validate_webhook_url


class GenerateRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=4000)
    image_url: str | None = None
    webhook_url: str | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)

    @field_validator("webhook_url")
    @classmethod
    def check_webhook_url(cls, value: str | None) -> str | None:
        """Reject unsafe destinations before reserving a generation token."""
        return validate_webhook_url(value) if value is not None else None


class VideoGenerateRequest(GenerateRequest):
    """Both configured Wan video endpoints accept prompts up to 1500 characters."""

    prompt: str = Field(min_length=1, max_length=1500)


class TopUpRequest(BaseModel):
    """An external key is mandatory; preserve values used by HMAC signing."""

    external_user_id: str
    amount: int = Field(gt=0, le=1_000_000)
    transaction_id: str | None = Field(default=None, min_length=1, max_length=192)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=192)

    @field_validator("transaction_id", "idempotency_key")
    @classmethod
    def reject_blank_key(cls, value: str | None) -> str | None:
        """Reject whitespace-only keys without normalizing signed content."""
        if value is not None and not value.strip():
            raise ValueError("Payment key must not be blank")
        return value

    @model_validator(mode="after")
    def require_external_key(self) -> "TopUpRequest":
        """Reject missing transaction/idempotency keys at the API boundary."""
        if self.transaction_id is None and self.idempotency_key is None:
            raise ValueError("transaction_id or idempotency_key is required")
        return self
