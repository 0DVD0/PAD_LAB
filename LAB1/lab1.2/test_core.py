import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from google.protobuf.struct_pb2 import Struct

import message_broker_pb2 as pb
from storage import MessageStorage
from subscriptions import SubscriberEndpoint, SubscriptionRegistry
from validation import validate_message


def message(identifier: str = "grpc-test") -> pb.Message:
    payload = Struct()
    payload.update({"text": "Salut gRPC"})
    return pb.Message(id=identifier, topic="news", type="custom.type", created_at=datetime.now(timezone.utc).isoformat(), payload=payload)


class CoreTests(unittest.TestCase):
    def test_dynamic_type_is_valid(self):
        validate_message(message())

    def test_registry_filters_type(self):
        registry = SubscriptionRegistry()
        registry.subscribe(SubscriberEndpoint("alice", "127.0.0.1", 6101, ("custom.type",)), "news")
        self.assertEqual(len(registry.subscribers_for("news", "custom.type")), 1)
        self.assertEqual(len(registry.subscribers_for("news", "other.type")), 0)

    def test_protobuf_is_persisted_without_alteration(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = MessageStorage(Path(directory) / "test.db")
            storage.initialize()
            original = message()
            endpoint = SubscriberEndpoint("alice", "127.0.0.1", 6101, ())
            storage.save_message(original, [endpoint])
            delivery_id = storage.due_ids()[0]
            task = storage.claim(delivery_id)
            self.assertEqual(task["message"].SerializeToString(), original.SerializeToString())
            storage.delivered(delivery_id)
            self.assertEqual(storage.stats()[0], {"completed": 1})


if __name__ == "__main__":
    unittest.main()
