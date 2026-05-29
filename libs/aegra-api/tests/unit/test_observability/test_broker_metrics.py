"""Unit tests for broker metrics module"""

from aegra_api.observability.broker_metrics import (
    ACTIVE_CONSUMERS,
    BROKER_CLEANED_UP_TOTAL,
    BROKER_CREATED_TOTAL,
    CONSUMER_CONNECTED_TOTAL,
    CONSUMER_DISCONNECTED_TOTAL,
    REPLAY_BUFFER_SKIPPED_TOTAL,
    record_broker_cleaned_up,
    record_broker_created,
    record_consumer_connected,
    record_consumer_disconnected,
    record_replay_buffer_skipped,
    set_active_consumers,
)


class TestBrokerMetrics:
    """Test broker metrics"""

    def test_record_consumer_connected_increments_counter(self):
        """Test that consumer connection increments the counter"""
        initial = CONSUMER_CONNECTED_TOTAL.labels(broker_type="test")._value.get()
        record_consumer_connected("test")
        new = CONSUMER_CONNECTED_TOTAL.labels(broker_type="test")._value.get()
        assert new == initial + 1

    def test_record_consumer_disconnected_increments_counter(self):
        """Test that consumer disconnection increments the counter"""
        initial = CONSUMER_DISCONNECTED_TOTAL.labels(broker_type="test")._value.get()
        record_consumer_disconnected("test")
        new = CONSUMER_DISCONNECTED_TOTAL.labels(broker_type="test")._value.get()
        assert new == initial + 1

    def test_set_active_consumers_sets_gauge(self):
        """Test that setting active consumers updates the gauge"""
        set_active_consumers("test", 5)
        value = ACTIVE_CONSUMERS.labels(broker_type="test")._value.get()
        assert value == 5

    def test_record_broker_created_increments_counter(self):
        """Test that broker creation increments the counter"""
        initial = BROKER_CREATED_TOTAL.labels(broker_type="test")._value.get()
        record_broker_created("test")
        new = BROKER_CREATED_TOTAL.labels(broker_type="test")._value.get()
        assert new == initial + 1

    def test_record_broker_cleaned_up_increments_counter(self):
        """Test that broker cleanup increments the counter"""
        initial = BROKER_CLEANED_UP_TOTAL.labels(broker_type="test")._value.get()
        record_broker_cleaned_up("test")
        new = BROKER_CLEANED_UP_TOTAL.labels(broker_type="test")._value.get()
        assert new == initial + 1

    def test_record_replay_buffer_skipped_increments_counter(self):
        """Test that replay buffer skip increments the counter"""
        initial = REPLAY_BUFFER_SKIPPED_TOTAL.labels(broker_type="test")._value.get()
        record_replay_buffer_skipped("test")
        new = REPLAY_BUFFER_SKIPPED_TOTAL.labels(broker_type="test")._value.get()
        assert new == initial + 1

    def test_metrics_have_correct_labels(self):
        """Test that all metrics have the expected broker_type label"""
        labels = {"broker_type": "redis"}
        record_consumer_connected(**labels)
        record_consumer_disconnected(**labels)
        set_active_consumers(**labels, count=1)
        record_broker_created(**labels)
        record_broker_cleaned_up(**labels)
        record_replay_buffer_skipped(**labels)
