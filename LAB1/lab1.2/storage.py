"""SQLite pastreaza mesajele protobuf in forma binara serializata."""

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import message_broker_pb2 as pb

from config import MAX_DELIVERY_ATTEMPTS, RETRY_BASE_DELAY
from subscriptions import SubscriberEndpoint


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MessageStorage:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def initialize(self) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY, topic TEXT NOT NULL, type TEXT NOT NULL,
                    protobuf BLOB NOT NULL, created_at TEXT NOT NULL, status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS subscriptions (
                    subscriber_id TEXT NOT NULL, topic TEXT NOT NULL, host TEXT NOT NULL,
                    port INTEGER NOT NULL, accepted_types TEXT NOT NULL,
                    created_at TEXT NOT NULL, PRIMARY KEY (subscriber_id, topic)
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT NOT NULL,
                    subscriber_id TEXT NOT NULL, host TEXT NOT NULL, port INTEGER NOT NULL,
                    status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT NOT NULL, last_error TEXT, updated_at TEXT NOT NULL,
                    UNIQUE(message_id, subscriber_id),
                    FOREIGN KEY(message_id) REFERENCES messages(id)
                );
                """
            )
            connection.execute("UPDATE deliveries SET status='retry' WHERE status='delivering'")

    def save_subscription(self, endpoint: SubscriberEndpoint, topic: str) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute(
                """INSERT INTO subscriptions
                   (subscriber_id, topic, host, port, accepted_types, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(subscriber_id, topic) DO UPDATE SET
                   host=excluded.host, port=excluded.port,
                   accepted_types=excluded.accepted_types""",
                (endpoint.subscriber_id, topic, endpoint.host, endpoint.port,
                 json.dumps(endpoint.accepted_types), utc_now()),
            )
            connection.execute(
                """UPDATE deliveries SET host=?, port=?, updated_at=?
                   WHERE subscriber_id=? AND status IN ('pending','retry')
                   AND message_id IN (SELECT id FROM messages WHERE topic=?)""",
                (endpoint.host, endpoint.port, utc_now(), endpoint.subscriber_id, topic),
            )

    def remove_subscription(self, subscriber_id: str, topic: str) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute("DELETE FROM subscriptions WHERE subscriber_id=? AND topic=?", (subscriber_id, topic))

    def load_subscriptions(self) -> list[tuple[str, SubscriberEndpoint]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM subscriptions").fetchall()
        return [(row["topic"], SubscriberEndpoint(row["subscriber_id"], row["host"], row["port"], tuple(json.loads(row["accepted_types"])))) for row in rows]

    def save_message(self, message: pb.Message, subscribers: list[SubscriberEndpoint]) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute(
                "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)",
                (message.id, message.topic, message.type, message.SerializeToString(),
                 message.created_at, "pending" if subscribers else "no_subscribers"),
            )
            now = utc_now()
            for endpoint in subscribers:
                connection.execute(
                    """INSERT INTO deliveries
                       (message_id, subscriber_id, host, port, status, attempts,
                        next_attempt_at, updated_at)
                       VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)""",
                    (message.id, endpoint.subscriber_id, endpoint.host, endpoint.port, now, now),
                )

    def due_ids(self) -> list[int]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id FROM deliveries WHERE status IN ('pending','retry') AND next_attempt_at<=? ORDER BY id",
                (utc_now(),),
            ).fetchall()
        return [row["id"] for row in rows]

    def claim(self, delivery_id: int) -> dict | None:
        with self._write_lock, self._connect() as connection:
            cursor = connection.execute(
                "UPDATE deliveries SET status='delivering', updated_at=? WHERE id=? AND status IN ('pending','retry')",
                (utc_now(), delivery_id),
            )
            if cursor.rowcount != 1:
                return None
            row = connection.execute(
                "SELECT d.*, m.protobuf FROM deliveries d JOIN messages m ON m.id=d.message_id WHERE d.id=?",
                (delivery_id,),
            ).fetchone()
        message = pb.Message()
        message.ParseFromString(row["protobuf"])
        return {"id": row["id"], "message_id": row["message_id"],
                "subscriber_id": row["subscriber_id"], "host": row["host"],
                "port": row["port"], "attempts": row["attempts"], "message": message}

    def delivered(self, delivery_id: int) -> None:
        with self._write_lock, self._connect() as connection:
            message_id = connection.execute("SELECT message_id FROM deliveries WHERE id=?", (delivery_id,)).fetchone()[0]
            connection.execute("UPDATE deliveries SET status='delivered', last_error=NULL, updated_at=? WHERE id=?", (utc_now(), delivery_id))
            self._refresh(connection, message_id)

    def failed_attempt(self, delivery_id: int, attempts: int, error: str) -> str:
        attempts += 1
        status = "failed" if attempts >= MAX_DELIVERY_ATTEMPTS else "retry"
        next_at = (datetime.now(timezone.utc) + timedelta(seconds=RETRY_BASE_DELAY ** attempts)).isoformat()
        with self._write_lock, self._connect() as connection:
            message_id = connection.execute("SELECT message_id FROM deliveries WHERE id=?", (delivery_id,)).fetchone()[0]
            connection.execute(
                "UPDATE deliveries SET status=?, attempts=?, next_attempt_at=?, last_error=?, updated_at=? WHERE id=?",
                (status, attempts, next_at, error[:500], utc_now(), delivery_id),
            )
            self._refresh(connection, message_id)
        return status

    @staticmethod
    def _refresh(connection: sqlite3.Connection, message_id: str) -> None:
        statuses = {row[0] for row in connection.execute("SELECT status FROM deliveries WHERE message_id=?", (message_id,)).fetchall()}
        if statuses == {"delivered"}:
            status = "completed"
        elif statuses and statuses <= {"delivered", "failed"}:
            status = "failed" if "failed" in statuses else "completed"
        else:
            status = "pending"
        connection.execute("UPDATE messages SET status=? WHERE id=?", (status, message_id))

    def stats(self) -> tuple[dict[str, int], dict[str, int], int]:
        with self._connect() as connection:
            messages = dict(connection.execute("SELECT status, COUNT(*) FROM messages GROUP BY status").fetchall())
            deliveries = dict(connection.execute("SELECT status, COUNT(*) FROM deliveries GROUP BY status").fetchall())
            subscriptions = connection.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0]
        return messages, deliveries, subscriptions
