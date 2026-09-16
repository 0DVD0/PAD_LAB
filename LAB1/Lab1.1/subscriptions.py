"""Registru thread-safe al abonamentelor."""

import threading
from typing import Any


class SubscriptionRegistry:
    def __init__(self) -> None:
        self._topics: dict[str, dict[str, dict[str, Any]]] = {}
        self._lock = threading.RLock()

    def subscribe(
        self,
        subscriber_id: str,
        topic: str,
        host: str,
        port: int,
        accepted_types: list[str] | None = None,
    ) -> None:
        with self._lock:
            self._topics.setdefault(topic, {})[subscriber_id] = {
                "subscriber_id": subscriber_id,
                "host": host,
                "port": port,
                "accepted_types": accepted_types or [],
            }

    def unsubscribe(self, subscriber_id: str, topic: str) -> bool:
        with self._lock:
            subscribers = self._topics.get(topic)
            if not subscribers or subscriber_id not in subscribers:
                return False
            del subscribers[subscriber_id]
            if not subscribers:
                del self._topics[topic]
            return True

    def get_subscribers(self, topic: str, message_type: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(subscriber)
                for subscriber in self._topics.get(topic, {}).values()
                if not subscriber["accepted_types"]
                or message_type in subscriber["accepted_types"]
            ]

    def snapshot(self) -> dict[str, list[dict[str, Any]]]:
        with self._lock:
            return {
                topic: [dict(item) for item in subscribers.values()]
                for topic, subscribers in self._topics.items()
            }
