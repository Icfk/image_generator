from fastapi import Depends, Header, HTTPException

from app.infrastructure.container import auth_service, rate_limit_service


async def current_user(x_api_key: str | None = Header(default=None)):
    if not x_api_key:
        raise HTTPException(401, "X-API-Key is required")
    user = await auth_service.authenticate(x_api_key)
    if not user:
        raise HTTPException(401, "Invalid API key")
    return user


async def rate_limit(user=Depends(current_user)) -> None:
    """Share one per-user budget across authenticated business API routes."""
    try:
        await rate_limit_service.check(user.id)
    except PermissionError as exc:
        raise HTTPException(
            429,
            str(exc),
            headers={"Retry-After": "60"},
        ) from exc
