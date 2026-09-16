"""Subscriber TCP care se aboneaza la topicuri si confirma livrarile."""

import argparse
import json
import socket
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from config import (
    BROKER_HOST,
    BROKER_PORT,
    DEFAULT_SUBSCRIBER_HOST,
    SUBSCRIBER_WORKERS,
)
from console_ui import event, panel, show_topics
from protocols import (
    create_client_connection,
    create_server_socket,
    receive_message,
    send_message,
)
from validation import validate_delivery_request


def broker_request(request: dict[str, Any]) -> dict[str, Any]:
    with create_client_connection(BROKER_HOST, BROKER_PORT) as connection:
        send_message(connection, request)
        return receive_message(connection)


def subscribe(
    subscriber_id: str,
    topic: str,
    host: str,
    port: int,
    accepted_types: list[str],
) -> dict[str, Any]:
    return broker_request({
        "action": "subscribe",
        "subscriber_id": subscriber_id,
        "topic": topic,
        "host": host,
        "port": port,
        "accepted_types": accepted_types,
    })


def unsubscribe(subscriber_id: str, topic: str) -> None:
    try:
        response = broker_request({
            "action": "unsubscribe",
            "subscriber_id": subscriber_id,
            "topic": topic,
        })
        if response.get("status") == "success":
            event(
                "SUBSCRIBER",
                "UNSUBSCRIBE",
                f"Dezabonat de la topicul '{topic}'",
                "yellow",
            )
    except OSError as error:
        event(
            "SUBSCRIBER",
            "WARNING",
            f"Broker-ul nu a putut fi notificat la oprire: {error}",
            "yellow",
        )


def choose_subscriber_id(value: str | None) -> str:
    if value:
        return value
    generated = f"subscriber-{uuid.uuid4().hex[:6]}"
    entered = input(f"Numele Subscriber-ului [{generated}]: ").strip()
    return entered or generated


def choose_topic(value: str | None) -> str:
    if value:
        return value

    try:
        response = broker_request({"action": "list_topics"})
        topics = response.get("topics", {})
    except OSError:
        topics = {}

    if topics:
        show_topics(topics)
        topic_names = sorted(topics)
        print("\nAlege numarul unui topic sau scrie un topic nou:")
        for index, topic_name in enumerate(topic_names, start=1):
            print(f"{index}. {topic_name}")
        choice = input("Topic: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(topic_names):
            return topic_names[int(choice) - 1]
        if choice:
            return choice
    else:
        panel(
            "Topicuri",
            ["Nu exista topicuri cu abonati.", "Scrie un nume pentru a crea primul topic."],
            "yellow",
        )

    while True:
        topic = input("Topic: ").strip()
        if topic:
            return topic
        event("SUBSCRIBER", "ERROR", "Topic-ul nu poate fi gol", "red")


def handle_delivery(
    connection: socket.socket,
    address: tuple[str, int],
    subscriber_id: str,
) -> None:
    try:
        request = receive_message(connection)
        message = validate_delivery_request(request)
        panel(
            "Mesaj primit",
            [
                f"Subscriber: {subscriber_id}",
                f"ID: {message['id']}",
                f"Topic: {message['topic']}",
                f"Tip: {message['type']}",
                f"Payload: {json.dumps(message['payload'], ensure_ascii=False)}",
            ],
            "green",
        )
        send_message(connection, {
            "action": "ack",
            "status": "success",
            "message_id": message["id"],
            "subscriber_id": subscriber_id,
        })
    except Exception as error:
        event("SUBSCRIBER", "ERROR", f"{address}: {error}", "red")
        try:
            send_message(connection, {
                "action": "ack", "status": "error", "details": str(error),
                "subscriber_id": subscriber_id,
            })
        except OSError:
            pass
    finally:
        connection.close()


def run_subscriber(
    subscriber_id: str,
    topic: str,
    host: str,
    port: int,
    accepted_types: list[str],
) -> None:
    with create_server_socket(host, port) as server:
        # Portul 0 cere sistemului de operare sa aloce un port TCP liber.
        assigned_port = server.getsockname()[1]
        response = subscribe(
            subscriber_id, topic, host, assigned_port, accepted_types
        )
        if response.get("status") != "success":
            raise RuntimeError(response.get("details", "Abonarea a esuat"))
        panel(
            "Subscriber activ",
            [
                f"ID: {subscriber_id}", f"Topic: {topic}",
                f"Adresa: {host}:{assigned_port} (port alocat automat)"
                if port == 0
                else f"Adresa: {host}:{assigned_port}",
                f"Tipuri: {', '.join(accepted_types) if accepted_types else 'toate'}",
                "Asteapta mesaje (Ctrl+C pentru oprire)",
            ],
        )
        try:
            with ThreadPoolExecutor(
                max_workers=SUBSCRIBER_WORKERS,
                thread_name_prefix="subscriber-worker",
            ) as executor:
                while True:
                    connection, address = server.accept()
                    executor.submit(handle_delivery, connection, address, subscriber_id)
        finally:
            unsubscribe(subscriber_id, topic)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Subscriber pentru brokerul de mesaje")
    parser.add_argument(
        "--id", dest="subscriber_id",
        help="Optional; daca lipseste este cerut interactiv",
    )
    parser.add_argument(
        "--topic",
        help="Optional; daca lipseste apare selectorul interactiv",
    )
    parser.add_argument(
        "--port", type=int, default=0,
        help="Implicit 0: sistemul alege automat un port liber",
    )
    parser.add_argument("--host", default=DEFAULT_SUBSCRIBER_HOST)
    parser.add_argument(
        "--types", nargs="*", default=[],
        help="Tipuri acceptate; gol inseamna toate tipurile",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    try:
        subscriber_id = choose_subscriber_id(args.subscriber_id)
        topic = choose_topic(args.topic)
        run_subscriber(
            subscriber_id, topic, args.host, args.port, args.types
        )
    except KeyboardInterrupt:
        event("SUBSCRIBER", "STOP", "Subscriber oprit", "yellow")
    except (ConnectionRefusedError, socket.timeout, OSError, RuntimeError) as error:
        event("SUBSCRIBER", "ERROR", str(error), "red")
