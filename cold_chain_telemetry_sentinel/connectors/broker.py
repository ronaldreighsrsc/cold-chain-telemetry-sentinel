"""
Event Streaming Broker Connector.
Provides a unified abstraction over Apache Kafka / Redpanda streaming topics
with a high-performance, thread-safe In-Memory broker for local development and CI.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class IEventBroker(ABC):
    """Abstract contract for event streaming brokers."""

    @abstractmethod
    def publish(self, topic: str, message: Dict[str, Any]) -> bool:
        """Publishes a structured payload to the target topic."""
        pass

    @abstractmethod
    def subscribe(self, topic: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Registers a consumer callback for a topic."""
        pass

    @abstractmethod
    def start(self) -> None:
        """Starts background consumption loops."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stops background workers cleanly."""
        pass


class InMemoryEventBroker(IEventBroker):
    """
    High-throughput, zero-dependency in-memory event broker.
    Guarantees FIFO ordering per topic partition, mimicking Kafka topic semantics.
    """

    def __init__(self):
        self._topics: Dict[str, Queue[Dict[str, Any]]] = defaultdict(Queue)
        self._subscribers: Dict[str, List[Callable[[Dict[str, Any]], None]]] = defaultdict(list)
        self._running = False
        self._worker_thread: Optional[Thread] = None
        self._stop_event = Event()

    def publish(self, topic: str, message: Dict[str, Any]) -> bool:
        """Enqueues an event onto the topic queue."""
        self._topics[topic].put(message)
        # Immediate dispatch if consumers are registered and worker is active
        if not self._running:
            # Synchronous direct dispatch for deterministic testing
            self._dispatch_now(topic, message)
        return True

    def subscribe(self, topic: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        self._subscribers[topic].append(handler)

    def _dispatch_now(self, topic: str, message: Dict[str, Any]) -> None:
        for handler in self._subscribers.get(topic, []):
            try:
                handler(message)
            except Exception as e:
                logger.error(f"Error executing consumer for topic '{topic}': {e}", exc_info=True)

    def _consumer_loop(self) -> None:
        while not self._stop_event.is_set():
            dispatched_any = False
            for topic, q in list(self._topics.items()):
                try:
                    msg = q.get_nowait()
                    self._dispatch_now(topic, msg)
                    q.task_done()
                    dispatched_any = True
                except Empty:
                    continue
            if not dispatched_any:
                self._stop_event.wait(timeout=0.01)

    def start(self) -> None:
        if not self._running:
            self._running = True
            self._stop_event.clear()
            self._worker_thread = Thread(target=self._consumer_loop, daemon=True)
            self._worker_thread.start()

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=1.0)
