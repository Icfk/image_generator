import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.infrastructure.config import settings
from app.infrastructure.database import engine, redis
from app.infrastructure.logging_setup import configure_logging
from app.infrastructure.metrics import API_REQUESTS
from app.presentation.deadline import ApiDeadlineMiddleware
from app.presentation.router import router

configure_logging("api")


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await redis.aclose()
    await engine.dispose()


app = FastAPI(title="Content Generation Service", version="0.1.0", lifespan=lifespan)
app.include_router(router)
app.add_middleware(ApiDeadlineMiddleware)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", include_in_schema=False)
async def frontend():
    return FileResponse("app/static/index.html")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/client-config", include_in_schema=False)
async def client_config() -> dict:
    """Expose only non-secret UI capabilities, never provider keys or secrets."""
    return {
        "provider": settings.generation_provider,
        "demo_payments": settings.app_env in ("development", "test")
        and not settings.require_webhook_signature,
        "poll_interval_seconds": 12,
    }


@app.get("/metrics")
async def metrics():
    return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)


@app.middleware("http")
async def request_logging(request: Request, call_next):
    """Record bounded metric labels, including failures and unknown routes."""
    request_id = str(uuid.uuid4())
    started = time.perf_counter()
    status = 500
    method = (
        request.method
        if request.method
        in {
            "GET",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "HEAD",
            "OPTIONS",
            "TRACE",
            "CONNECT",
        }
        else "OTHER"
    )
    logger = logging.getLogger("content-service")
    try:
        response = await call_next(request)
        status = response.status_code
        response.headers["X-Request-ID"] = request_id
        return response
    except Exception:
        logger.exception("Unhandled API exception request_id=%s", request_id)
        raise
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", "__unmatched__")
        API_REQUESTS.labels(method, path, status).inc()
        logger.info(
            "request_id=%s method=%s route=%s status=%s duration=%.3fs",
            request_id,
            method,
            path,
            status,
            time.perf_counter() - started,
        )
