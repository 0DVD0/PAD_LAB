"""Validarea semantica a cererilor protocolului."""

from datetime import datetime
from typing import Any

def required_string(data: dict[str, Any], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Campul '{field}' trebuie sa fie un string ne-gol")
    return value.strip()


def validate_topic(topic: str) -> None:
    if len(topic) > 100:
        raise ValueError("Topic-ul poate avea maximum 100 de caractere")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    if any(character not in allowed for character in topic):
        raise ValueError("Topic-ul contine caractere nepermise")


def validate_message_type(message_type: str) -> None:
    if len(message_type) > 100:
        raise ValueError("Tipul poate avea maximum 100 de caractere")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    if any(character not in allowed for character in message_type):
        raise ValueError("Tipul mesajului contine caractere nepermise")


def validate_published_message(message: Any) -> None:
    if not isinstance(message, dict):
        raise ValueError("Campul 'message' trebuie sa fie un obiect")
    required_string(message, "id")
    topic = required_string(message, "topic")
    validate_topic(topic)
    message_type = required_string(message, "type")
    validate_message_type(message_type)
    created_at = required_string(message, "created_at")
    try:
        datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("Campul 'created_at' nu este o data ISO valida") from error
    payload = message.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("Campul 'payload' trebuie sa fie un obiect")
    if "text" in payload and (
        not isinstance(payload["text"], str) or not payload["text"].strip()
    ):
        raise ValueError("Campul optional 'payload.text' trebuie sa fie ne-gol")
    if message_type in {"notification", "news"}:
        required_string(payload, "text")


def validate_request(request: dict[str, Any]) -> str:
    action = required_string(request, "action")
    if action == "publish":
        validate_published_message(request.get("message"))
    elif action == "subscribe":
        required_string(request, "subscriber_id")
        topic = required_string(request, "topic")
        validate_topic(topic)
        required_string(request, "host")
        port = request.get("port")
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise ValueError("Campul 'port' trebuie sa fie intre 1 si 65535")
        accepted_types = request.get("accepted_types", [])
        if not isinstance(accepted_types, list):
            raise ValueError("Campul 'accepted_types' trebuie sa fie o lista")
        for item in accepted_types:
            if not isinstance(item, str) or not item:
                raise ValueError("Campul 'accepted_types' contine tipuri invalide")
            validate_message_type(item)
    elif action == "unsubscribe":
        required_string(request, "subscriber_id")
        topic = required_string(request, "topic")
        validate_topic(topic)
    elif action not in {"list_topics", "stats", "ping"}:
        raise ValueError(f"Actiune necunoscuta: {action}")
    return action


def validate_delivery_request(request: dict[str, Any]) -> dict[str, Any]:
    if request.get("action") != "deliver":
        raise ValueError("Subscriber-ul accepta doar actiunea 'deliver'")
    message = request.get("message")
    validate_published_message(message)
    return message
