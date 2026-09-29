from app.domain.entities import TaskStatus


def test_generation_statuses_are_explicit():
    assert [s.value for s in TaskStatus] == [
        "created",
        "queued",
        "processing",
        "completed",
        "failed",
    ]
