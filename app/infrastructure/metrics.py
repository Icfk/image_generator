from prometheus_client import Counter, Gauge, Histogram

API_REQUESTS = Counter(
    "api_requests_total", "API requests", ["method", "path", "status"]
)
GENERATIONS = Counter("generations_total", "Generation tasks", ["kind", "status"])
GENERATIONS_SUCCESS = Counter(
    "generations_success_total", "Successful generations", ["kind"]
)
GENERATIONS_FAILED = Counter("generations_failed_total", "Failed generations", ["kind"])
GENERATION_DURATION = Histogram(
    "generation_duration_seconds", "Generation duration", ["kind"]
)
GENERATION_COST = Counter("generation_cost_total", "Generation cost", ["kind"])
PROVIDER_REQUESTS = Counter(
    "provider_requests_total", "Provider requests", ["provider", "kind"]
)
PROVIDER_ERRORS = Counter(
    "provider_errors_total", "Provider errors", ["provider", "kind"]
)
PROVIDER_FAILOVER = Counter(
    "provider_failover_total", "Provider failovers", ["from_provider", "to_provider"]
)
WEBHOOK_DELIVERY_ATTEMPTS = Counter(
    "webhook_delivery_attempts_total", "Webhook delivery attempts", ["status"]
)
RECOVERY_ACTIONS = Counter(
    "recovery_actions_total", "Committed recovery actions", ["action", "outcome"]
)
RECOVERY_CYCLES = Counter("recovery_cycles_total", "Recovery cycles", ["outcome"])
RECOVERY_HEARTBEAT = Gauge(
    "recovery_last_success_timestamp_seconds", "Last successful recovery sweep"
)
