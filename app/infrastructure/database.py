from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.infrastructure.application_uow import ApplicationUnitOfWorkFactory
from app.infrastructure.config import settings
from app.infrastructure.rate_limit import RateLimitRepository
from app.infrastructure.repositories import UnitOfWork

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    **(
        {"pool_timeout": 2, "connect_args": {"timeout": 2, "command_timeout": 2}}
        if settings.database_url.startswith("postgresql")
        else {}
    ),
)
Session = async_sessionmaker(engine, expire_on_commit=False)
redis = Redis.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_connect_timeout=1,
    socket_timeout=1,
)
rate_limit_repository = RateLimitRepository(redis)


class UnitOfWorkFactory:
    """Create application transaction boundaries."""

    def __init__(self, session_factory: Callable = Session) -> None:
        self.session_factory = session_factory

    def for_application(self) -> ApplicationUnitOfWorkFactory:
        """Keep the SQLAlchemy session and models behind repository ports."""
        return ApplicationUnitOfWorkFactory(self)

    @asynccontextmanager
    async def create(self) -> AsyncIterator[UnitOfWork]:
        async with self.session_factory() as session:
            async with UnitOfWork(session) as unit_of_work:
                yield unit_of_work
