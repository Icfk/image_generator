"""Composition root: instantiate concrete persistence outside application services."""

from app.application.services import (
    AuthService,
    GenerationService,
    RateLimitService,
    UserService,
)
from app.infrastructure.database import UnitOfWorkFactory, rate_limit_repository

uow_factory = UnitOfWorkFactory()
generation_service = GenerationService(uow_factory)
user_service = UserService(uow_factory)
auth_service = AuthService(uow_factory)
rate_limit_service = RateLimitService(rate_limit_repository)
