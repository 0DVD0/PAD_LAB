import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class SubscriberEndpoint:
    subscriber_id: str
    host: str
    port: int
    accepted_types: tuple[str, ...]


class SubscriptionRegistry:
    def __init__(self) -> None:
        self._topics: dict[str, dict[str, SubscriberEndpoint]] = {}
        self._lock = threading.RLock()

    def subscribe(self, endpoint: SubscriberEndpoint, topic: str) -> None:
        with self._lock:
            self._topics.setdefault(topic, {})[endpoint.subscriber_id] = endpoint

    def unsubscribe(self, subscriber_id: str, topic: str) -> bool:
        with self._lock:
            entries = self._topics.get(topic)
            if not entries or subscriber_id not in entries:
                return False
            del entries[subscriber_id]
            if not entries:
                del self._topics[topic]
            return True

    def subscribers_for(self, topic: str, message_type: str) -> list[SubscriberEndpoint]:
        with self._lock:
            return [
                endpoint for endpoint in self._topics.get(topic, {}).values()
                if not endpoint.accepted_types or message_type in endpoint.accepted_types
            ]

    def snapshot(self) -> dict[str, list[SubscriberEndpoint]]:
        with self._lock:
            return {topic: list(entries.values()) for topic, entries in self._topics.items()}
