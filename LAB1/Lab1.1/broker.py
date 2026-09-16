"""Broker TCP/JSON cu Publisher/Subscriber, SQLite, ACK si retry."""

import json
import logging
import queue
import socket
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from config import (
    BROKER_CLIENT_WORKERS,
    BROKER_HOST,
    BROKER_PORT,
    DATABASE_PATH,
    DELIVERY_WORKERS,
    RETRY_SCAN_INTERVAL,
)
from console_ui import event
from protocols import (
    create_client_connection,
    create_server_socket,
    receive_message,
    send_message,
)
from storage import MessageStorage
from subscriptions import SubscriptionRegistry
from validation import validate_request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BROKER] %(levelname)s [%(threadName)s] %(message)s",
)

storage = MessageStorage(DATABASE_PATH)
registry = SubscriptionRegistry()
delivery_queue: queue.Queue[int] = queue.Queue()
stop_event = threading.Event()


def success_response(**data: Any) -> dict[str, Any]:
    return {"status": "success", **data}


def error_response(code: str, details: str) -> dict[str, Any]:
    return {"status": "error", "code": code, "details": details}


def handle_subscribe(request: dict[str, Any]) -> dict[str, Any]:
    subscriber_id = request["subscriber_id"]
    topic = request["topic"]
    host = request["host"]
    port = request["port"]
    accepted_types = request.get("accepted_types", [])
    storage.save_subscription(subscriber_id, topic, host, port, accepted_types)
    registry.subscribe(subscriber_id, topic, host, port, accepted_types)
    event("BROKER", "SUBSCRIBE", f"{subscriber_id} -> {topic} ({host}:{port})", "green")
    return success_response(
        details=f"Abonare realizata la topicul '{topic}'",
        subscriber_id=subscriber_id,
        topic=topic,
    )


def handle_unsubscribe(request: dict[str, Any]) -> dict[str, Any]:
    subscriber_id = request["subscriber_id"]
    topic = request["topic"]
    removed = registry.unsubscribe(subscriber_id, topic)
    storage.remove_subscription(subscriber_id, topic)
    event("BROKER", "UNSUBSCRIBE", f"{subscriber_id} -> {topic}", "yellow")
    return success_response(removed=removed, subscriber_id=subscriber_id, topic=topic)


def handle_publish(request: dict[str, Any]) -> dict[str, Any]:
    message = request["message"]
    subscribers = registry.get_subscribers(message["topic"], message["type"])
    storage.save_message_and_deliveries(message, subscribers)
    for delivery_id in storage.due_delivery_ids():
        delivery_queue.put(delivery_id)
    event(
        "BROKER",
        "PUBLISH",
        f"{message['id']} topic={message['topic']} abonati={len(subscribers)}",
        "magenta",
    )
    return {
        "status": "accepted",
        "message_id": message["id"],
        "subscriber_count": len(subscribers),
        "details": "Mesaj salvat persistent pentru livrare"
        if subscribers
        else "Mesaj salvat; topicul nu are abonati compatibili",
    }


def route_request(request: dict[str, Any]) -> dict[str, Any]:
    action = validate_request(request)
    if action == "subscribe":
        return handle_subscribe(request)
    if action == "unsubscribe":
        return handle_unsubscribe(request)
    if action == "publish":
        return handle_publish(request)
    if action == "list_topics":
        return success_response(topics=registry.snapshot())
    if action == "stats":
        return success_response(stats=storage.stats())
    return success_response(details="pong")


def safely_send(connection: socket.socket, response: dict[str, Any]) -> None:
    try:
        send_message(connection, response)
    except OSError as error:
        logging.warning("Nu s-a putut trimite raspunsul: %s", error)


def handle_client(connection: socket.socket, address: tuple[str, int]) -> None:
    try:
        request = receive_message(connection)
        response = route_request(request)
        send_message(connection, response)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        logging.warning("Cerere invalida de la %s:%s: %s", *address, error)
        safely_send(connection, error_response("INVALID_REQUEST", str(error)))
    except sqlite3.IntegrityError as error:
        logging.warning("Conflict de date: %s", error)
        safely_send(connection, error_response("DUPLICATE_MESSAGE", str(error)))
    except (ConnectionError, socket.timeout) as error:
        logging.warning("Comunicare esuata cu %s:%s: %s", *address, error)
    except Exception:
        logging.exception("Eroare interna la procesarea clientului")
        safely_send(connection, error_response("INTERNAL_ERROR", "Eroare interna in Broker"))
    finally:
        connection.close()


def deliver(task: dict[str, Any]) -> None:
    with create_client_connection(task["host"], task["port"]) as connection:
        send_message(connection, {"action": "deliver", "message": task["message"]})
        response = receive_message(connection)
    if response.get("action") != "ack" or response.get("status") != "success":
        raise RuntimeError("Subscriber-ul nu a confirmat mesajul prin ACK")
    if response.get("message_id") != task["message_id"]:
        raise RuntimeError("ACK-ul contine un message_id diferit")
    if response.get("subscriber_id") != task["subscriber_id"]:
        raise RuntimeError("ACK-ul contine un subscriber_id diferit")


def delivery_worker() -> None:
    while not stop_event.is_set():
        try:
            delivery_id = delivery_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            task = storage.claim_delivery(delivery_id)
            if task is None:
                continue
            deliver(task)
            storage.mark_delivered(delivery_id)
            event(
                "BROKER", "DELIVERED",
                f"{task['message_id']} -> {task['subscriber_id']}", "green"
            )
        except Exception as error:
            if "task" in locals() and task is not None:
                status = storage.mark_failed_attempt(
                    delivery_id, task["attempts"], str(error)
                )
                event(
                    "BROKER", status.upper(),
                    f"{task['message_id']} -> {task['subscriber_id']}: {error}",
                    "red" if status == "failed" else "yellow",
                )
            else:
                logging.exception("Worker-ul nu a putut incarca livrarea")
        finally:
            task = None
            delivery_queue.task_done()


def retry_scheduler() -> None:
    while not stop_event.wait(RETRY_SCAN_INTERVAL):
        for delivery_id in storage.due_delivery_ids():
            delivery_queue.put(delivery_id)


def restore_state() -> None:
    storage.initialize()
    for item in storage.load_subscriptions():
        registry.subscribe(
            item["subscriber_id"], item["topic"], item["host"],
            item["port"], item["accepted_types"],
        )
    for delivery_id in storage.due_delivery_ids():
        delivery_queue.put(delivery_id)


def start_background_workers() -> None:
    for index in range(DELIVERY_WORKERS):
        threading.Thread(
            target=delivery_worker,
            name=f"delivery-worker-{index + 1}",
            daemon=True,
        ).start()
    threading.Thread(
        target=retry_scheduler, name="retry-scheduler", daemon=True
    ).start()


def start_broker() -> None:
    restore_state()
    start_background_workers()
    with create_server_socket(BROKER_HOST, BROKER_PORT) as server:
        event(
            "BROKER", "START",
            f"asculta pe {BROKER_HOST}:{BROKER_PORT}; DB={DATABASE_PATH}", "cyan"
        )
        with ThreadPoolExecutor(
            max_workers=BROKER_CLIENT_WORKERS,
            thread_name_prefix="client-worker",
        ) as executor:
            while True:
                connection, address = server.accept()
                executor.submit(handle_client, connection, address)


if __name__ == "__main__":
    try:
        start_broker()
    except KeyboardInterrupt:
        stop_event.set()
        event("BROKER", "STOP", "Broker oprit de utilizator", "yellow")
