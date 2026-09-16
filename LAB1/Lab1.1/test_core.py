import socket
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from protocols import receive_message, send_message
from storage import MessageStorage
from subscriptions import SubscriptionRegistry
from validation import validate_published_message, validate_request


def sample_message(message_id: str = "msg-1") -> dict:
    return {
        "id": message_id,
        "topic": "news",
        "type": "news",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": {"text": "Salut"},
    }


class ProtocolTests(unittest.TestCase):
    def test_round_trip(self) -> None:
        first, second = socket.socketpair()
        try:
            send_message(first, {"text": "Salut, Moldova!"})
            self.assertEqual(receive_message(second), {"text": "Salut, Moldova!"})
        finally:
            first.close()
            second.close()


class ValidationTests(unittest.TestCase):
    def test_valid_publish(self) -> None:
        request = {"action": "publish", "message": sample_message()}
        self.assertEqual(validate_request(request), "publish")

    def test_invalid_topic(self) -> None:
        message = sample_message()
        message["topic"] = "topic invalid"
        with self.assertRaises(ValueError):
            validate_published_message(message)

    def test_dynamic_message_type(self) -> None:
        message = sample_message()
        message["type"] = "email.sent"
        validate_published_message(message)


class RegistryTests(unittest.TestCase):
    def test_topic_and_type_filtering(self) -> None:
        registry = SubscriptionRegistry()
        registry.subscribe("alice", "news", "127.0.0.1", 6101, ["news"])
        registry.subscribe("bob", "news", "127.0.0.1", 6102, ["notification"])
        subscribers = registry.get_subscribers("news", "news")
        self.assertEqual([item["subscriber_id"] for item in subscribers], ["alice"])


class StorageTests(unittest.TestCase):
    def test_persistent_message_and_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = MessageStorage(Path(directory) / "test.db")
            storage.initialize()
            subscriber = {
                "subscriber_id": "alice", "host": "127.0.0.1", "port": 6101,
            }
            storage.save_message_and_deliveries(sample_message(), [subscriber])
            delivery_ids = storage.due_delivery_ids()
            self.assertEqual(len(delivery_ids), 1)
            task = storage.claim_delivery(delivery_ids[0])
            self.assertEqual(task["message"]["payload"]["text"], "Salut")
            storage.mark_delivered(delivery_ids[0])
            self.assertEqual(storage.stats()["messages"], {"completed": 1})


if __name__ == "__main__":
    unittest.main()
