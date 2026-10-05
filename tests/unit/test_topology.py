from __future__ import annotations

from app.config import Settings
from app.core import constants
from app.messaging.topology import build_topology, retry_routing_key


def test_settings_expose_two_delay_tiers_for_three_attempts() -> None:
    assert Settings(processing_max_attempts=3, processing_retry_base_seconds=5).processing_retry_delays == (
        5,
        15,
    )


def test_work_queue_dead_letters_to_the_dlx() -> None:
    topology = build_topology((5, 15))
    arguments = topology.payments_queue.arguments or {}
    assert topology.payments_queue.name == constants.PAYMENTS_QUEUE
    assert arguments["x-dead-letter-exchange"] == constants.DEAD_LETTER_EXCHANGE
    assert arguments["x-dead-letter-routing-key"] == constants.PAYMENT_CREATED_ROUTING_KEY


def test_retry_queues_expire_back_into_the_work_queue() -> None:
    topology = build_topology((5, 15))
    assert topology.retry_tier_count == 2
    assert [queue.name for queue in topology.retry_queues] == [
        "payments.retry.1",
        "payments.retry.2",
    ]
    ttl_values = [(queue.arguments or {})["x-message-ttl"] for queue in topology.retry_queues]
    assert ttl_values == [5000, 15000]
    for queue in topology.retry_queues:
        arguments = queue.arguments or {}
        assert arguments["x-dead-letter-exchange"] == constants.PAYMENTS_EXCHANGE
        assert arguments["x-dead-letter-routing-key"] == constants.PAYMENT_CREATED_ROUTING_KEY


def test_retry_routing_keys_are_stable() -> None:
    assert retry_routing_key(1) == "payment.created.retry.1"
    assert retry_routing_key(2) == "payment.created.retry.2"


def test_topology_is_not_declared_twice() -> None:
    topology = build_topology((5,))
    assert topology.payments_queue.declare is False
    assert topology.dead_letter_queue.declare is False
    assert all(queue.declare is False for queue in topology.retry_queues)