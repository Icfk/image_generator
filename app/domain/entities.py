from enum import StrEnum


class TaskStatus(StrEnum):
    created = "created"
    queued = "queued"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class RetryableProviderError(RuntimeError):
    pass


class PermanentProviderError(RuntimeError):
    pass


class SubmissionUncertainError(RuntimeError):
    """A submit may have succeeded; never automatically submit a replacement."""


class LostGenerationClaim(RuntimeError):
    """A newer worker/recovery transition owns the generation."""
