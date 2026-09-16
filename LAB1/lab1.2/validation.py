from datetime import datetime

import message_broker_pb2 as pb


def _validate_name(value: str, label: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{label} trebuie sa fie ne-gol")
    if len(value) > 100:
        raise ValueError(f"{label} poate avea maximum 100 de caractere")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    if any(character not in allowed for character in value):
        raise ValueError(f"{label} contine caractere nepermise")


def validate_message(message: pb.Message) -> None:
    if not message.id.strip():
        raise ValueError("ID-ul mesajului este obligatoriu")
    _validate_name(message.topic, "Topic-ul")
    _validate_name(message.type, "Tipul")
    if not message.created_at:
        raise ValueError("created_at este obligatoriu")
    try:
        datetime.fromisoformat(message.created_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("created_at nu este o data ISO valida") from error


def validate_subscription(request: pb.SubscribeRequest) -> None:
    _validate_name(request.subscriber_id, "Subscriber ID")
    _validate_name(request.topic, "Topic-ul")
    if not request.host.strip():
        raise ValueError("Host-ul este obligatoriu")
    if not 1 <= request.port <= 65535:
        raise ValueError("Portul trebuie sa fie intre 1 si 65535")
    for message_type in request.accepted_types:
        _validate_name(message_type, "Tipul acceptat")
