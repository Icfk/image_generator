from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_env: Literal["production", "development", "test"] = "production"
    database_url: str = "postgresql+asyncpg://app:app@localhost:5432/content"
    redis_url: str = "redis://localhost:6379/0"
    auth_secret: str = "change-me"
    generation_provider: Literal["mock", "fal"] = "mock"
    fal_api_key: str | None = None
    fal_model_image: str = "fal-ai/flux/dev"
    fal_model_video: str = "fal-ai/wan-25-preview/image-to-video"
    fal_model_image_to_image: str = "fal-ai/flux/dev/image-to-image"
    fal_model_text_to_video: str = "fal-ai/wan-25-preview/text-to-video"
    fal_fallback_api_key: str | None = None
    fal_fallback_models: dict[str, str] = Field(default_factory=dict)
    api_timeout_seconds: float = Field(default=4.5, ge=0.1, le=4.5)
    enqueue_timeout_seconds: float = Field(default=1.0, ge=0.01, le=2.0)
    provider_failure_threshold: int = 3
    provider_recovery_timeout_seconds: int = 30
    webhook_attempts: int = Field(default=5, ge=1, le=5)
    webhook_interval_seconds: int = Field(default=15, ge=1)
    webhook_preparation_timeout_seconds: int = Field(default=30, ge=1)
    require_webhook_signature: bool = True
    log_dir: str = "logs"
    processing_timeout_seconds: int = Field(default=600, gt=0)
    provider_operation_timeout_seconds: int = Field(default=300, gt=0)
    recovery_batch_size: int = Field(default=100, gt=0)
    recovery_interval_seconds: int = Field(default=15, gt=0)
    recovery_health_max_age_seconds: int = Field(default=90, gt=0)
    recovery_metrics_port: int = Field(default=8001, ge=1, le=65535)
    worker_metrics_port: int = Field(default=8002, ge=1, le=65535)

    @model_validator(mode="after")
    def validate_payment_security(self) -> "Settings":
        """Fail closed outside explicitly selected offline/demo environments."""
        protected = self.app_env == "production" or self.generation_provider == "fal"
        if protected and not self.require_webhook_signature:
            raise ValueError("Payment signatures are required in production/Fal mode")
        if protected and (
            len(self.auth_secret.strip()) < 32
            or self.auth_secret.lower() in {"change-me", "secret", "password"}
        ):
            raise ValueError("Set a non-default AUTH_SECRET of at least 32 characters")
        return self

    @model_validator(mode="after")
    def validate_recovery_timeouts(self) -> "Settings":
        """Do not expire a processing claim before the provider/ARQ deadline."""
        if (
            self.processing_timeout_seconds
            <= self.provider_operation_timeout_seconds + 30
        ):
            raise ValueError("Processing timeout must exceed provider timeout + 30s")
        if self.recovery_health_max_age_seconds <= self.recovery_interval_seconds:
            raise ValueError("Heartbeat max age must exceed recovery interval")
        return self


settings = Settings()
