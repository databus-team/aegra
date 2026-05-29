"""Unit tests for consumer tracking in RedisRunBroker"""

import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aegra_api.services.redis_broker import RedisBrokerManager, RedisRunBroker


def _make_redis_broker(run_id: str = "run-123") -> RedisRunBroker:
    """Helper to create a RedisRunBroker with default keys."""
    return RedisRunBroker(
        run_id,
        f"aegra:run:{run_id}",
        f"aegra:run:cache:{run_id}",
        f"aegra:run:counter:{run_id}",
    )


class TestRedisRunBrokerConsumerTracking:
    """Test consumer tracking in RedisRunBroker"""

    def test_initial_no_active_consumers(self):
        """Test that broker starts with no active consumers"""
        broker = _make_redis_broker()

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()

    def test_on_consumer_connected_increments_counter(self):
        """Test that on_consumer_connected increments the counter"""
        broker = _make_redis_broker()

        broker.on_consumer_connected()

        assert broker._active_consumers == 1
        assert broker.has_active_consumers()

    def test_multiple_consumers_increment_counter(self):
        """Test that multiple consumers increment the counter correctly"""
        broker = _make_redis_broker()

        broker.on_consumer_connected()
        broker.on_consumer_connected()
        broker.on_consumer_connected()

        assert broker._active_consumers == 3
        assert broker.has_active_consumers()

    def test_on_consumer_disconnected_decrements_counter(self):
        """Test that on_consumer_disconnected decrements the counter"""
        broker = _make_redis_broker()
        broker.on_consumer_connected()
        broker.on_consumer_connected()

        broker.on_consumer_disconnected()

        assert broker._active_consumers == 1
        assert broker.has_active_consumers()

    def test_on_consumer_disconnected_prevents_negative_count(self):
        """Test that on_consumer_disconnected prevents negative count"""
        broker = _make_redis_broker()

        broker.on_consumer_disconnected()

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()

    def test_all_consumers_disconnected_resets_counter(self):
        """Test that all consumers disconnecting resets counter"""
        broker = _make_redis_broker()
        broker.on_consumer_connected()
        broker.on_consumer_connected()

        broker.on_consumer_disconnected()
        broker.on_consumer_disconnected()

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()

    @pytest.mark.asyncio
    async def test_aiter_increments_and_decrements_consumer_count(self):
        """Test that aiter() properly tracks consumer connection/disconnection"""
        broker = _make_redis_broker("run-aiter-test")

        messages = [
            {"type": "message", "data": '{"event_id": "evt-1", "payload": ["values", {"msg": "hi"}]}'},
            {"type": "message", "data": '{"event_id": "evt-end", "payload": ["end", {"status": "success"}]}'},
        ]
        call_count = 0
        allow_end = asyncio.Event()

        async def mock_get_message(ignore_subscribe_messages: bool = True, timeout: float = 0.5):
            nonlocal call_count
            if call_count == 0:
                call_count += 1
                return messages[0]
            elif call_count == 1:
                await allow_end.wait()
                call_count += 1
                return messages[1]
            return None

        mock_pubsub = AsyncMock()
        mock_pubsub.get_message = mock_get_message
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.aclose = AsyncMock()

        mock_client = MagicMock()
        mock_client.pubsub.return_value = mock_pubsub
        mock_client.lrange = AsyncMock(return_value=[])

        with patch("aegra_api.services.redis_broker.redis_manager") as mock_rm:
            mock_rm.get_client.return_value = mock_client

            async def consume():
                events = []
                async for event_id, payload in broker.aiter():
                    events.append((event_id, payload))
                return events

            # Start consuming
            task = asyncio.create_task(consume())
            await asyncio.sleep(0.05)  # Give aiter time to start

            assert broker._active_consumers == 1

            # Now allow the end message
            allow_end.set()

            # Let the consumer finish
            events = await task

            assert broker._active_consumers == 0
            assert len(events) == 2

    @pytest.mark.asyncio
    async def test_aiter_disconnect_on_exception(self):
        """Test that aiter() properly decrements counter on exception"""
        broker = _make_redis_broker("run-exception-test")

        mock_pubsub = AsyncMock()
        allow_exception = asyncio.Event()

        async def mock_get_message(ignore_subscribe_messages: bool = True, timeout: float = 0.5):
            await allow_exception.wait()
            raise RuntimeError("Test exception")

        mock_pubsub.get_message = mock_get_message
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.aclose = AsyncMock()

        mock_client = MagicMock()
        mock_client.pubsub.return_value = mock_pubsub
        mock_client.lrange = AsyncMock(return_value=[])

        with patch("aegra_api.services.redis_broker.redis_manager") as mock_rm:
            mock_rm.get_client.return_value = mock_client

            async def faulty_consumer():
                async for _event_id, _payload in broker.aiter():
                    pass

            task = asyncio.create_task(faulty_consumer())
            await asyncio.sleep(0.05)

            assert broker._active_consumers == 1

            # Now trigger the exception
            allow_exception.set()

            with suppress(asyncio.CancelledError, RuntimeError):
                await task

            assert broker._active_consumers == 0


class TestRedisBrokerManagerConsumerTracking:
    """Test consumer tracking through RedisBrokerManager"""

    def test_get_or_create_broker_initializes_consumer_count(self):
        """Test that get_or_create_broker initializes consumer tracking"""
        manager = RedisBrokerManager()

        broker = manager.get_or_create_broker("run-123")

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()

    def test_multiple_consumers_across_brokers(self):
        """Test that consumer tracking works independently per broker"""
        manager = RedisBrokerManager()

        broker1 = manager.get_or_create_broker("run-1")
        broker2 = manager.get_or_create_broker("run-2")

        broker1.on_consumer_connected()
        broker1.on_consumer_connected()
        broker2.on_consumer_connected()

        assert broker1._active_consumers == 2
        assert broker2._active_consumers == 1
        assert broker1.has_active_consumers()
        assert broker2.has_active_consumers()
