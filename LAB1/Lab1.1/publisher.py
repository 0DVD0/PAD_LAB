"""Publisher interactiv pentru mesaje rutate dupa topic."""

import json
import socket
import uuid
from datetime import datetime, timezone
from typing import Any

from config import BROKER_HOST, BROKER_PORT
from console_ui import event, panel, show_topics
from protocols import create_client_connection, receive_message, send_message


def broker_request(request: dict[str, Any]) -> dict[str, Any]:
    with create_client_connection(BROKER_HOST, BROKER_PORT) as connection:
        send_message(connection, request)
        return receive_message(connection)


def create_message(topic: str, message_type: str, text: str) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "topic": topic,
        "type": message_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": {"text": text},
    }


def publish_interactively() -> None:
    topic = input("Topic: ").strip()
    message_type = input("Tip [notification]: ").strip() or "notification"
    text = input("Text: ").strip()
    if not topic or not text:
        event("PUBLISHER", "ERROR", "Topic-ul si textul sunt obligatorii", "red")
        return
    message = create_message(topic, message_type, text)
    response = broker_request({"action": "publish", "message": message})
    panel(
        "Rezultat publicare",
        [
            f"ID: {message['id']}",
            f"Status: {response.get('status')}",
            f"Abonati: {response.get('subscriber_count', 0)}",
            f"Detalii: {response.get('details', '-')}",
        ],
        "green" if response.get("status") == "accepted" else "red",
    )


def list_topics() -> None:
    response = broker_request({"action": "list_topics"})
    show_topics(response.get("topics", {}))


def show_stats() -> None:
    response = broker_request({"action": "stats"})
    stats = response.get("stats", {})
    panel("Statistici Broker", [json.dumps(stats, ensure_ascii=False, indent=2)])


def main() -> None:
    panel(
        "Publisher",
        [f"Broker: {BROKER_HOST}:{BROKER_PORT}", "Publicare TCP/JSON pe topicuri"],
    )
    while True:
        print("\n1. Publica mesaj")
        print("2. Listeaza topicurile")
        print("3. Statistici Broker")
        print("0. Iesire")
        option = input("Alege: ").strip()
        try:
            if option == "1":
                publish_interactively()
            elif option == "2":
                list_topics()
            elif option == "3":
                show_stats()
            elif option == "0":
                return
            else:
                event("PUBLISHER", "ERROR", "Optiune invalida", "red")
        except ConnectionRefusedError:
            event("PUBLISHER", "ERROR", "Broker-ul nu este disponibil", "red")
        except (socket.timeout, ConnectionError, ValueError) as error:
            event("PUBLISHER", "ERROR", str(error), "red")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        event("PUBLISHER", "STOP", "Publisher oprit", "yellow")
