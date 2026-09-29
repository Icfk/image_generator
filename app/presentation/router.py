import asyncio
import hashlib
import hmac
import json

from arq import create_pool
from arq.connections import RedisSettings
from fastapi import APIRouter, Depends, Header, HTTPException

from app.infrastructure.config import settings
from app.infrastructure.container import generation_service, user_service
from app.presentation.dependencies import current_user, rate_limit
from app.presentation.schemas import GenerateRequest, TopUpRequest, VideoGenerateRequest

router = APIRouter()


@router.post("/auth")
async def create_user():
    return await user_service.create()


@router.get("/balance", dependencies=[Depends(rate_limit)])
async def balance(user=Depends(current_user)):
    return {"external_user_id": user.external_id, "balance": user.balance}


@router.post("/webhooks/top-up")
async def top_up(
    data: TopUpRequest, x_webhook_signature: str | None = Header(default=None)
):
    if settings.require_webhook_signature:
        expected = hmac.new(
            settings.auth_secret.encode(),
            json.dumps(data.model_dump(), sort_keys=True).encode(),
            hashlib.sha256,
        ).hexdigest()
        if not x_webhook_signature or not hmac.compare_digest(
            expected, x_webhook_signature
        ):
            raise HTTPException(401, "Invalid webhook signature")
    try:
        return await user_service.top_up(
            data.external_user_id,
            data.amount,
            data.idempotency_key or data.transaction_id,
        )
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


async def create(kind, data, user, idempotency_key: str | None = None):
    """Publish only the winning creation; replays never contact Redis."""
    if (
        idempotency_key is not None
        and data.idempotency_key is not None
        and idempotency_key != data.idempotency_key
    ):
        raise HTTPException(400, "Header and body idempotency keys must match")
    key = idempotency_key if idempotency_key is not None else data.idempotency_key
    try:
        creation = await generation_service.create_request(
            user.id, kind, data.prompt, data.image_url, key, data.webhook_url
        )
    except PermissionError as exc:
        raise HTTPException(402, str(exc)) from exc
    result = creation.response()
    if not creation.created:
        return result
    pool = None
    try:
        async with asyncio.timeout(settings.enqueue_timeout_seconds):
            pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
            for attempt in range(3):
                try:
                    await pool.enqueue_job(
                        "run_generation",
                        result["id"],
                        data.webhook_url,
                        _job_id=f"generation:{result['id']}",
                    )
                    break
                except Exception:
                    if attempt == 2:
                        raise
            await generation_service.mark_publication(result["id"], "published")
    except Exception as exc:
        await generation_service.mark_publication(
            result["id"], "unknown", type(exc).__name__
        )
        raise HTTPException(
            503, "Worker queue state is unknown; recovery will retry publication"
        ) from exc
    finally:
        if pool is not None:
            try:
                async with asyncio.timeout(0.25):
                    await pool.aclose()
            except TimeoutError:
                pass
    return result


@router.post("/generations/images", status_code=202)
async def image(
    data: GenerateRequest,
    _: None = Depends(rate_limit),
    user=Depends(current_user),
    idempotency_key: str | None = Header(default=None, min_length=1, max_length=200),
):
    return await create("image", data, user, idempotency_key)


@router.post("/generations/videos", status_code=202)
async def video(
    data: VideoGenerateRequest,
    _: None = Depends(rate_limit),
    user=Depends(current_user),
    idempotency_key: str | None = Header(default=None, min_length=1, max_length=200),
):
    return await create("video", data, user, idempotency_key)


@router.get("/generations/{task_id}", dependencies=[Depends(rate_limit)])
async def get_generation(task_id: str, user=Depends(current_user)):
    task = await generation_service.get_for_user(task_id, user.id)
    if not task:
        raise HTTPException(404, "Generation not found")
    return {
        "id": task.id,
        "kind": task.kind,
        "status": task.status.value,
        "result": task.result,
        "error": task.error,
    }
