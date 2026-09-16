"""Persistenta SQLite pentru mesaje, abonamente si livrari."""

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import MAX_DELIVERY_ATTEMPTS, RETRY_BASE_DELAY


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MessageStorage:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS subscriptions (
                    subscriber_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    accepted_types TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (subscriber_id, topic)
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT NOT NULL,
                    subscriber_id TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT NOT NULL,
                    last_error TEXT,
                    updated_at TEXT NOT NULL,
                    UNIQUE (message_id, subscriber_id),
                    FOREIGN KEY (message_id) REFERENCES messages(id)
                );
                """
            )
            connection.execute(
                "UPDATE deliveries SET status='retry' WHERE status='delivering'"
            )

    def save_subscription(
        self,
        subscriber_id: str,
        topic: str,
        host: str,
        port: int,
        accepted_types: list[str],
    ) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO subscriptions
                    (subscriber_id, topic, host, port, accepted_types, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(subscriber_id, topic) DO UPDATE SET
                    host=excluded.host,
                    port=excluded.port,
                    accepted_types=excluded.accepted_types
                """,
                (subscriber_id, topic, host, port, json.dumps(accepted_types), utc_now()),
            )
            connection.execute(
                """
                UPDATE deliveries
                SET host=?, port=?, updated_at=?
                WHERE subscriber_id=?
                  AND status IN ('pending', 'retry')
                  AND message_id IN (SELECT id FROM messages WHERE topic=?)
                """,
                (host, port, utc_now(), subscriber_id, topic),
            )

    def remove_subscription(self, subscriber_id: str, topic: str) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute(
                "DELETE FROM subscriptions WHERE subscriber_id=? AND topic=?",
                (subscriber_id, topic),
            )

    def load_subscriptions(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM subscriptions").fetchall()
        return [
            {
                "subscriber_id": row["subscriber_id"],
                "topic": row["topic"],
                "host": row["host"],
                "port": row["port"],
                "accepted_types": json.loads(row["accepted_types"]),
            }
            for row in rows
        ]

    def save_message_and_deliveries(
        self, message: dict[str, Any], subscribers: list[dict[str, Any]]
    ) -> None:
        status = "pending" if subscribers else "no_subscribers"
        with self._write_lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO messages (id, topic, type, payload, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    message["id"], message["topic"], message["type"],
                    json.dumps(message["payload"], ensure_ascii=False),
                    message["created_at"], status,
                ),
            )
            now = utc_now()
            for subscriber in subscribers:
                connection.execute(
                    """
                    INSERT INTO deliveries
                        (message_id, subscriber_id, host, port, status,
                         attempts, next_attempt_at, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)
                    """,
                    (
                        message["id"], subscriber["subscriber_id"],
                        subscriber["host"], subscriber["port"], now, now,
                    ),
                )

    def due_delivery_ids(self) -> list[int]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id FROM deliveries
                WHERE status IN ('pending', 'retry') AND next_attempt_at <= ?
                ORDER BY id
                """,
                (utc_now(),),
            ).fetchall()
        return [row["id"] for row in rows]

    def claim_delivery(self, delivery_id: int) -> dict[str, Any] | None:
        with self._write_lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE deliveries SET status='delivering', updated_at=?
                WHERE id=? AND status IN ('pending', 'retry')
                """,
                (utc_now(), delivery_id),
            )
            if cursor.rowcount != 1:
                return None
            row = connection.execute(
                """
                SELECT d.*, m.topic, m.type, m.payload, m.created_at
                FROM deliveries d JOIN messages m ON m.id=d.message_id
                WHERE d.id=?
                """,
                (delivery_id,),
            ).fetchone()
        return {
            "delivery_id": row["id"],
            "message_id": row["message_id"],
            "subscriber_id": row["subscriber_id"],
            "host": row["host"],
            "port": row["port"],
            "attempts": row["attempts"],
            "message": {
                "id": row["message_id"], "topic": row["topic"],
                "type": row["type"], "payload": json.loads(row["payload"]),
                "created_at": row["created_at"],
            },
        }

    def mark_delivered(self, delivery_id: int) -> None:
        with self._write_lock, self._connect() as connection:
            row = connection.execute(
                "SELECT message_id FROM deliveries WHERE id=?", (delivery_id,)
            ).fetchone()
            connection.execute(
                "UPDATE deliveries SET status='delivered', updated_at=?, last_error=NULL WHERE id=?",
                (utc_now(), delivery_id),
            )
            self._refresh_message_status(connection, row["message_id"])

    def mark_failed_attempt(self, delivery_id: int, attempts: int, error: str) -> str:
        new_attempts = attempts + 1
        status = "failed" if new_attempts >= MAX_DELIVERY_ATTEMPTS else "retry"
        delay = RETRY_BASE_DELAY ** new_attempts
        next_attempt = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
        with self._write_lock, self._connect() as connection:
            row = connection.execute(
                "SELECT message_id FROM deliveries WHERE id=?", (delivery_id,)
            ).fetchone()
            connection.execute(
                """
                UPDATE deliveries
                SET status=?, attempts=?, next_attempt_at=?, last_error=?, updated_at=?
                WHERE id=?
                """,
                (status, new_attempts, next_attempt, error[:500], utc_now(), delivery_id),
            )
            self._refresh_message_status(connection, row["message_id"])
        return status

    @staticmethod
    def _refresh_message_status(
        connection: sqlite3.Connection, message_id: str
    ) -> None:
        rows = connection.execute(
            "SELECT status FROM deliveries WHERE message_id=?", (message_id,)
        ).fetchall()
        statuses = {row["status"] for row in rows}
        if statuses == {"delivered"}:
            message_status = "completed"
        elif statuses and statuses <= {"delivered", "failed"}:
            message_status = "failed" if "failed" in statuses else "completed"
        else:
            message_status = "pending"
        connection.execute(
            "UPDATE messages SET status=? WHERE id=?", (message_status, message_id)
        )

    def stats(self) -> dict[str, Any]:
        with self._connect() as connection:
            messages = dict(connection.execute(
                "SELECT status, COUNT(*) FROM messages GROUP BY status"
            ).fetchall())
            deliveries = dict(connection.execute(
                "SELECT status, COUNT(*) FROM deliveries GROUP BY status"
            ).fetchall())
            subscriptions = connection.execute(
                "SELECT COUNT(*) FROM subscriptions"
            ).fetchone()[0]
        return {
            "messages": messages,
            "deliveries": deliveries,
            "subscriptions": subscriptions,
        }
