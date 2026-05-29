"""Unit tests for consumer tracking configuration option"""

from unittest.mock import patch

import pytest

from aegra_api.services.broker import RunBroker


class TestRunBrokerConsumerTrackingConfig:
    """Test consumer tracking with configuration option"""

    @pytest.mark.asyncio
    async def test_tracking_disabled_has_active_consumers_returns_true(self):
        """Test that when tracking is disabled, has_active_consumers always returns True"""
        with patch("aegra_api.services.broker.settings") as mock_settings:
            mock_settings.redis.ENABLE_CONSUMER_TRACKING = False

            broker = RunBroker("run-123")

            assert broker._tracking_enabled is False
            assert broker.has_active_consumers() is True

    @pytest.mark.asyncio
    async def test_tracking_disabled_on_consumer_connected_no_effect(self):
        """Test that when tracking is disabled, on_consumer_connected has no effect"""
        with patch("aegra_api.services.broker.settings") as mock_settings:
            mock_settings.redis.ENABLE_CONSUMER_TRACKING = False

            broker = RunBroker("run-123")

            broker.on_consumer_connected()

            assert broker._active_consumers == 0

    @pytest.mark.asyncio
    async def test_tracking_disabled_on_consumer_disconnected_no_effect(self):
        """Test that when tracking is disabled, on_consumer_disconnected has no effect"""
        with patch("aegra_api.services.broker.settings") as mock_settings:
            mock_settings.redis.ENABLE_CONSUMER_TRACKING = False

            broker = RunBroker("run-123")

            broker.on_consumer_disconnected()

            assert broker._active_consumers == 0

    @pytest.mark.asyncio
    async def test_tracking_enabled_behaves_normally(self):
        """Test that when tracking is enabled, consumer tracking works normally"""
        with patch("aegra_api.services.broker.settings") as mock_settings:
            mock_settings.redis.ENABLE_CONSUMER_TRACKING = True

            broker = RunBroker("run-123")

            assert broker._tracking_enabled is True
            assert broker.has_active_consumers() is False

            broker.on_consumer_connected()
            assert broker.has_active_consumers() is True

            broker.on_consumer_disconnected()
            assert broker.has_active_consumers() is False
