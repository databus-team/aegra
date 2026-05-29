"""Unit tests for consumer tracking configuration in RedisRunBroker"""

from unittest.mock import patch

from aegra_api.services.redis_broker import RedisRunBroker


def _make_redis_broker(run_id: str = "run-123") -> RedisRunBroker:
    """Helper to create a RedisRunBroker with default keys."""
    return RedisRunBroker(
        run_id,
        f"aegra:run:{run_id}",
        f"aegra:run:cache:{run_id}",
        f"aegra:run:counter:{run_id}",
    )


class TestRedisRunBrokerConsumerTrackingConfig:
    """Test consumer tracking with configuration option in RedisRunBroker"""

    def test_tracking_disabled_has_active_consumers_returns_true(self):
        """Test that when tracking is disabled, has_active_consumers always returns True"""
        with patch("aegra_api.services.redis_broker.settings") as mock_settings:
            mock_settings.redis.ENABLE_CONSUMER_TRACKING = False

            broker = _make_redis_broker()

            assert broker._tracking_enabled is False
            assert broker.has_active_consumers() is True

    def test_tracking_disabled_on_consumer_connected_no_effect(self):
        """Test that when tracking is disabled, on_consumer_connected has no effect"""
        with patch("aegra_api.services.redis_broker.settings") as mock_settings:
            mock_settings.redis.ENABLE_CONSUMER_TRACKING = False

            broker = _make_redis_broker()

            broker.on_consumer_connected()

            assert broker._active_consumers == 0

    def test_tracking_disabled_on_consumer_disconnected_no_effect(self):
        """Test that when tracking is disabled, on_consumer_disconnected has no effect"""
        with patch("aegra_api.services.redis_broker.settings") as mock_settings:
            mock_settings.redis.ENABLE_CONSUMER_TRACKING = False

            broker = _make_redis_broker()

            broker.on_consumer_disconnected()

            assert broker._active_consumers == 0

    def test_tracking_enabled_behaves_normally(self):
        """Test that when tracking is enabled, consumer tracking works normally"""
        with patch("aegra_api.services.redis_broker.settings") as mock_settings:
            mock_settings.redis.ENABLE_CONSUMER_TRACKING = True

            broker = _make_redis_broker()

            assert broker._tracking_enabled is True
            assert broker.has_active_consumers() is False

            broker.on_consumer_connected()
            assert broker.has_active_consumers() is True

            broker.on_consumer_disconnected()
            assert broker.has_active_consumers() is False
