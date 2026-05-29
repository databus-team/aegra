"""Prometheus metrics for broker operations.

These metrics track consumer connections and disconnections for SSE streaming,
helping monitor the health and efficiency of the streaming infrastructure.
"""

from prometheus_client import Counter, Gauge

CONSUMER_CONNECTED_TOTAL = Counter(
    "aegra_broker_consumer_connected_total",
    "Total number of consumer connections to SSE brokers",
    ["broker_type"],
)

CONSUMER_DISCONNECTED_TOTAL = Counter(
    "aegra_broker_consumer_disconnected_total",
    "Total number of consumer disconnections from SSE brokers",
    ["broker_type"],
)

ACTIVE_CONSUMERS = Gauge(
    "aegra_broker_active_consumers",
    "Current number of active consumers across all brokers",
    ["broker_type"],
)

BROKER_CREATED_TOTAL = Counter(
    "aegra_broker_created_total",
    "Total number of brokers created",
    ["broker_type"],
)

BROKER_CLEANED_UP_TOTAL = Counter(
    "aegra_broker_cleaned_up_total",
    "Total number of brokers cleaned up",
    ["broker_type"],
)

REPLAY_BUFFER_SKIPPED_TOTAL = Counter(
    "aegra_broker_replay_buffer_skipped_total",
    "Total number of events skipped from replay buffer (no active consumers)",
    ["broker_type"],
)


def record_consumer_connected(broker_type: str) -> None:
    """Record a consumer connection event."""
    CONSUMER_CONNECTED_TOTAL.labels(broker_type=broker_type).inc()


def record_consumer_disconnected(broker_type: str) -> None:
    """Record a consumer disconnection event."""
    CONSUMER_DISCONNECTED_TOTAL.labels(broker_type=broker_type).inc()


def set_active_consumers(broker_type: str, count: int) -> None:
    """Set the current active consumer gauge."""
    ACTIVE_CONSUMERS.labels(broker_type=broker_type).set(count)


def record_broker_created(broker_type: str) -> None:
    """Record a new broker creation."""
    BROKER_CREATED_TOTAL.labels(broker_type=broker_type).inc()


def record_broker_cleaned_up(broker_type: str) -> None:
    """Record a broker cleanup."""
    BROKER_CLEANED_UP_TOTAL.labels(broker_type=broker_type).inc()


def record_replay_buffer_skipped(broker_type: str) -> None:
    """Record an event that was skipped from replay buffer."""
    REPLAY_BUFFER_SKIPPED_TOTAL.labels(broker_type=broker_type).inc()
