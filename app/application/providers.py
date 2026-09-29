from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod

import httpx

from app.domain.entities import (
    LostGenerationClaim,
    PermanentProviderError,
    RetryableProviderError,
    SubmissionUncertainError,
)
from app.infrastructure.config import settings
from app.infrastructure.metrics import (
    PROVIDER_ERRORS,
    PROVIDER_FAILOVER,
    PROVIDER_REQUESTS,
)

log = logging.getLogger("content-service.providers")


class GenerationProvider(ABC):
    name: str

    @abstractmethod
    async def generate(
        self,
        kind: str,
        prompt: str,
        image_url: str | None,
        provider_job_id: str | None = None,
        *,
        on_submitted=None,
        provider_context=None,
    ) -> dict: ...


class MockProvider(GenerationProvider):
    name = "mock"

    async def generate(
        self,
        kind,
        prompt,
        image_url,
        provider_job_id=None,
        *,
        on_submitted=None,
        provider_context=None,
    ):
        PROVIDER_REQUESTS.labels(self.name, kind).inc()
        await asyncio.sleep(0.01)
        return {
            "url": "/static/mock-result.svg",
            "provider": self.name,
            "mock": True,
            "kind": kind,
        }


class FalAIProvider(GenerationProvider):
    """Persist submit identity before polling; resumption never issues a POST."""

    name = "fal"

    def __init__(
        self,
        client_factory=httpx.AsyncClient,
        sleep=asyncio.sleep,
        *,
        name: str = "fal",
        models: dict[str, str] | None = None,
        api_key: str | None = None,
    ):
        self.client_factory = client_factory
        self.sleep = sleep
        self.name = name
        self.models = models or {}
        self.api_key = api_key

    def model_for(self, kind: str, image_url: str | None) -> str:
        """Route all four modes; legacy image/video settings retain their meaning."""
        mode = f"{'image' if image_url else 'text'}-to-{kind}"
        defaults = {
            "text-to-image": settings.fal_model_image,
            "image-to-image": settings.fal_model_image_to_image,
            "text-to-video": settings.fal_model_text_to_video,
            "image-to-video": settings.fal_model_video,
        }
        if mode not in defaults:
            raise PermanentProviderError("Unsupported generation mode")
        return self.models.get(mode, defaults[mode])

    @staticmethod
    def checked_url(value: str) -> str:
        """Only send provider credentials to the provider's HTTPS queue origin."""
        url = httpx.URL(value)
        if url.scheme != "https" or url.host != "queue.fal.run":
            raise PermanentProviderError("Invalid provider queue URL")
        return value

    @staticmethod
    def check_response(response: httpx.Response, *, submitting: bool = False) -> dict:
        """Separate final rejection from retryable polling and uncertain submit."""
        if response.status_code >= 500:
            if submitting:
                raise SubmissionUncertainError("Fal submit outcome unknown")
            raise RetryableProviderError("Fal polling temporary error")
        if response.status_code in (408, 425, 429):
            if submitting and response.status_code == 408:
                raise SubmissionUncertainError("Fal submit outcome unknown")
            raise RetryableProviderError("Fal temporary error")
        if not response.is_success:
            raise PermanentProviderError(f"Fal HTTP {response.status_code}")
        try:
            body = response.json()
            if not isinstance(body, dict):
                raise ValueError("Expected object")
        except ValueError as exc:
            if submitting:
                raise SubmissionUncertainError("Invalid submit response") from exc
            raise RetryableProviderError("Invalid polling response") from exc
        return body

    async def generate(
        self,
        kind,
        prompt,
        image_url,
        provider_job_id=None,
        *,
        on_submitted=None,
        provider_context=None,
    ):
        """Submit once, checkpoint, then poll and fetch the actual result."""
        PROVIDER_REQUESTS.labels(self.name, kind).inc()
        api_key = self.api_key or settings.fal_api_key
        if not api_key:
            raise PermanentProviderError("FAL_API_KEY is not configured")
        model = self.model_for(kind, image_url)
        base = f"https://queue.fal.run/{model}"
        context = dict(provider_context or {})
        job_id = provider_job_id
        headers = {"Authorization": f"Key {api_key}"}
        try:
            async with asyncio.timeout(settings.provider_operation_timeout_seconds):
                async with self.client_factory(timeout=20) as client:
                    if not job_id:
                        payload = {"prompt": prompt}
                        if image_url:
                            payload["image_url"] = image_url
                        response = await client.post(
                            base, json=payload, headers=headers
                        )
                        try:
                            submitted = self.check_response(response, submitting=True)
                            job_id = submitted.get("request_id")
                            if not job_id:
                                raise SubmissionUncertainError(
                                    "Fal submit has no job id"
                                )
                        except (ValueError, TypeError) as exc:
                            raise SubmissionUncertainError(
                                "Invalid submit response"
                            ) from exc
                        context = {
                            "request_id": job_id,
                            "provider": self.name,
                            "status_url": submitted.get("status_url")
                            or f"{base}/requests/{job_id}/status",
                            "response_url": submitted.get("response_url")
                            or f"{base}/requests/{job_id}/response",
                        }
                        if on_submitted:
                            await on_submitted(context)
                    status_url = self.checked_url(
                        context.get("status_url") or f"{base}/requests/{job_id}/status"
                    )
                    while True:
                        response = await client.get(status_url, headers=headers)
                        body = self.check_response(response)
                        state = body.get("status", "").lower()
                        if body.get("error") or state in ("failed", "cancelled"):
                            raise PermanentProviderError("Fal generation failed")
                        if state in ("completed", "succeeded"):
                            response_url = self.checked_url(
                                context.get("response_url")
                                or body.get("response_url")
                                or f"{base}/requests/{job_id}/response"
                            )
                            response = await client.get(response_url, headers=headers)
                            result = self.check_response(response)
                            images = result.get("images") or []
                            media = images[0] if images else result.get("video") or {}
                            result_url = media.get("url")
                            if not result_url:
                                raise PermanentProviderError("Fal result has no URL")
                            return {
                                "url": result_url,
                                "provider": self.name,
                                "provider_job_id": job_id,
                            }
                        await self.sleep(2)
        except (httpx.HTTPError, TimeoutError) as exc:
            if not job_id:
                raise SubmissionUncertainError("Fal submit outcome unknown") from exc
            raise RetryableProviderError("Fal polling interrupted") from exc


class CircuitBreaker:
    def __init__(self, threshold: int, recovery_timeout: int):
        self.threshold, self.recovery_timeout = threshold, recovery_timeout
        self.failures, self.opened_at, self.state = 0, 0.0, "closed"
        self.lock = asyncio.Lock()
        self.probe_in_flight = False

    async def allow(self):
        async with self.lock:
            if self.state == "half-open":
                return False
            if (
                self.state == "open"
                and time.monotonic() - self.opened_at >= self.recovery_timeout
            ):
                if self.probe_in_flight:
                    return False
                self.state, self.probe_in_flight = "half-open", True
            return self.state != "open"

    async def success(self):
        async with self.lock:
            self.failures, self.state, self.probe_in_flight = 0, "closed", False

    async def failure(self):
        async with self.lock:
            self.failures += 1
            self.probe_in_flight = False
            if self.failures >= self.threshold:
                self.state, self.opened_at = "open", time.monotonic()


class ProviderManager(GenerationProvider):
    name = "manager"

    def __init__(self, primary, fallback=None, breaker=None):
        if primary.name != "mock" and isinstance(fallback, MockProvider):
            raise ValueError("MockProvider cannot be a fallback for a real provider")
        self.primary, self.fallback = primary, fallback
        self.breaker = breaker or CircuitBreaker(
            settings.provider_failure_threshold,
            settings.provider_recovery_timeout_seconds,
        )

    async def generate(
        self,
        kind,
        prompt,
        image_url,
        provider_job_id=None,
        *,
        on_submitted=None,
        provider_context=None,
    ):
        """Never fail over a submitted request to a second provider."""
        submitted = bool(provider_job_id)

        async def checkpoint(context):
            nonlocal submitted
            submitted = True
            if on_submitted:
                await on_submitted(context)

        async def invoke(provider):
            if provider_job_id or on_submitted or provider_context:
                return await provider.generate(
                    kind,
                    prompt,
                    image_url,
                    provider_job_id,
                    on_submitted=checkpoint,
                    provider_context=provider_context,
                )
            return await provider.generate(kind, prompt, image_url)

        if provider_context and provider_context.get("provider") != self.primary.name:
            if self.fallback and provider_context.get("provider") == self.fallback.name:
                # A resumed fallback job must never submit again or switch back.
                try:
                    return await invoke(self.fallback)
                except Exception:
                    PROVIDER_ERRORS.labels(self.fallback.name, kind).inc()
                    raise
            raise PermanentProviderError("Saved provider does not match configuration")
        last_error = RetryableProviderError("Provider circuit is open; retry later")
        if submitted or await self.breaker.allow():
            try:
                result = await invoke(self.primary)
                await self.breaker.success()
                return result
            except asyncio.CancelledError:
                # Cancellation must release a half-open probe too.
                await asyncio.shield(self.breaker.failure())
                raise
            except (SubmissionUncertainError, LostGenerationClaim):
                await self.breaker.failure()
                PROVIDER_ERRORS.labels(self.primary.name, kind).inc()
                raise
            except Exception as exc:
                await self.breaker.failure()
                PROVIDER_ERRORS.labels(self.primary.name, kind).inc()
                if submitted or isinstance(exc, PermanentProviderError):
                    raise
                last_error = exc
        if self.fallback is None:
            raise last_error
        PROVIDER_FAILOVER.labels(self.primary.name, self.fallback.name).inc()
        try:
            return await invoke(self.fallback)
        except Exception as exc:
            PROVIDER_ERRORS.labels(self.fallback.name, kind).inc()
            log.warning(
                "Fallback provider failed provider=%s error_type=%s",
                self.fallback.name,
                type(exc).__name__,
            )
            raise


def build_provider():
    if settings.generation_provider == "fal":
        return ProviderManager(
            FalAIProvider(),
            FalAIProvider(
                name="fal-fallback",
                models=settings.fal_fallback_models,
                api_key=settings.fal_fallback_api_key,
            ),
        )
    return ProviderManager(MockProvider())
