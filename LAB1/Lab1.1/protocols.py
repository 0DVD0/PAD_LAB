"""Nivelul abstract de retea: framing, JSON si conexiuni TCP."""

import json
import socket
import struct
from typing import Any

from config import HEADER_SIZE, MAX_MESSAGE_SIZE, SOCKET_TIMEOUT


def receive_exactly(connection: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if chunk == b"":
            raise ConnectionError(
                "Conexiunea s-a inchis inainte de primirea mesajului complet"
            )
        data.extend(chunk)
    return bytes(data)


def send_message(connection: socket.socket, message: dict[str, Any]) -> None:
    message_bytes = json.dumps(
        message, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    if not message_bytes:
        raise ValueError("Mesajul nu poate fi gol")
    if len(message_bytes) > MAX_MESSAGE_SIZE:
        raise ValueError("Mesajul depaseste dimensiunea maxima permisa")
    connection.sendall(struct.pack("!I", len(message_bytes)) + message_bytes)


def receive_message(connection: socket.socket) -> dict[str, Any]:
    header = receive_exactly(connection, HEADER_SIZE)
    message_size = struct.unpack("!I", header)[0]
    if message_size == 0:
        raise ValueError("Mesajul nu poate fi gol")
    if message_size > MAX_MESSAGE_SIZE:
        raise ValueError("Mesajul depaseste dimensiunea maxima permisa")
    message = json.loads(
        receive_exactly(connection, message_size).decode("utf-8")
    )
    if not isinstance(message, dict):
        raise ValueError("Mesajul trebuie sa fie un obiect JSON")
    return message


def create_server_socket(host: str, port: int) -> socket.socket:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen()
    return server


def create_client_connection(host: str, port: int) -> socket.socket:
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.settimeout(SOCKET_TIMEOUT)
    try:
        connection.connect((host, port))
    except Exception:
        connection.close()
        raise
    return connection
