"""Unit tests for consumer tracking functionality in brokers"""

import asyncio
from contextlib import suppress

import pytest

from aegra_api.services.broker import BrokerManager, RunBroker


class TestRunBrokerConsumerTracking:
    """Test consumer tracking in RunBroker"""

    @pytest.mark.asyncio
    async def test_initial_no_active_consumers(self):
        """Test that broker starts with no active consumers"""
        broker = RunBroker("run-123")

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()

    @pytest.mark.asyncio
    async def test_on_consumer_connected_increments_counter(self):
        """Test that on_consumer_connected increments the counter"""
        broker = RunBroker("run-123")

        broker.on_consumer_connected()

        assert broker._active_consumers == 1
        assert broker.has_active_consumers()

    @pytest.mark.asyncio
    async def test_multiple_consumers_increment_counter(self):
        """Test that multiple consumers increment the counter correctly"""
        broker = RunBroker("run-123")

        broker.on_consumer_connected()
        broker.on_consumer_connected()
        broker.on_consumer_connected()

        assert broker._active_consumers == 3
        assert broker.has_active_consumers()

    @pytest.mark.asyncio
    async def test_on_consumer_disconnected_decrements_counter(self):
        """Test that on_consumer_disconnected decrements the counter"""
        broker = RunBroker("run-123")
        broker.on_consumer_connected()
        broker.on_consumer_connected()

        broker.on_consumer_disconnected()

        assert broker._active_consumers == 1
        assert broker.has_active_consumers()

    @pytest.mark.asyncio
    async def test_on_consumer_disconnected_prevents_negative_count(self):
        """Test that on_consumer_disconnected prevents negative count"""
        broker = RunBroker("run-123")

        broker.on_consumer_disconnected()

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()

    @pytest.mark.asyncio
    async def test_all_consumers_disconnected_resets_counter(self):
        """Test that all consumers disconnecting resets counter"""
        broker = RunBroker("run-123")
        broker.on_consumer_connected()
        broker.on_consumer_connected()

        broker.on_consumer_disconnected()
        broker.on_consumer_disconnected()

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()

    @pytest.mark.asyncio
    async def test_aiter_increments_and_decrements_consumer_count(self):
        """Test that aiter() properly tracks consumer connection/disconnection"""
        broker = RunBroker("run-123")

        async def consume():
            events = []
            async for event_id, payload in broker.aiter():
                events.append((event_id, payload))
            return events

        # Start consuming first
        task = asyncio.create_task(consume())
        await asyncio.sleep(0.05)  # Give aiter time to start

        # Now put events (after consumer has started)
        assert broker._active_consumers == 1
        assert broker.has_active_consumers()

        await broker.put("evt-1", {"data": "test"})
        await broker.put("evt-end", ("end", {}))

        # Let the consumer finish
        events = await task

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()
        assert len(events) == 2

    @pytest.mark.asyncio
    async def test_aiter_disconnect_on_exception(self):
        """Test that aiter() properly decrements counter on exception"""
        broker = RunBroker("run-123")

        async def faulty_consumer():
            async for _event_id, _payload in broker.aiter():
                raise RuntimeError("Test exception")

        task = asyncio.create_task(faulty_consumer())
        await asyncio.sleep(0.05)

        assert broker._active_consumers == 1

        task.cancel()
        with suppress(asyncio.CancelledError, RuntimeError):
            await task

        assert broker._active_consumers == 0


class TestBrokerManagerConsumerTracking:
    """Test consumer tracking through BrokerManager"""

    @pytest.mark.asyncio
    async def test_get_or_create_broker_initializes_consumer_count(self):
        """Test that get_or_create_broker initializes consumer tracking"""
        manager = BrokerManager()

        broker = manager.get_or_create_broker("run-123")

        assert broker._active_consumers == 0
        assert not broker.has_active_consumers()

    @pytest.mark.asyncio
    async def test_multiple_consumers_across_brokers(self):
        """Test that consumer tracking works independently per broker"""
        manager = BrokerManager()

        broker1 = manager.get_or_create_broker("run-1")
        broker2 = manager.get_or_create_broker("run-2")

        broker1.on_consumer_connected()
        broker1.on_consumer_connected()
        broker2.on_consumer_connected()

        assert broker1._active_consumers == 2
        assert broker2._active_consumers == 1
        assert broker1.has_active_consumers()
        assert broker2.has_active_consumers()
