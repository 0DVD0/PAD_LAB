"""Publisher gRPC interactiv."""

import uuid
from datetime import datetime, timezone

import grpc
from google.protobuf.struct_pb2 import Struct

import message_broker_pb2 as pb
import message_broker_pb2_grpc as rpc
from config import BROKER_HOST, BROKER_PORT, RPC_TIMEOUT
from console_ui import event, panel, topics_table


def connect():
    channel = grpc.insecure_channel(f"{BROKER_HOST}:{BROKER_PORT}")
    return channel, rpc.BrokerServiceStub(channel)


def create_message(topic: str, message_type: str, text: str) -> pb.Message:
    payload = Struct()
    payload.update({"text": text})
    return pb.Message(
        id=str(uuid.uuid4()), topic=topic, type=message_type,
        created_at=datetime.now(timezone.utc).isoformat(), payload=payload,
    )


def publish(stub) -> None:
    topic = input("Topic: ").strip()
    message_type = input("Tip [notification]: ").strip() or "notification"
    text = input("Text: ").strip()
    if not topic or not text:
        event("PUBLISHER-gRPC", "ERROR", "Topic-ul si textul sunt obligatorii", "red")
        return
    message = create_message(topic, message_type, text)
    response = stub.Publish(pb.PublishRequest(message=message), timeout=RPC_TIMEOUT)
    panel(
        "Rezultat publicare gRPC",
        [f"ID: {response.message_id}", f"Acceptat: {response.accepted}",
         f"Abonati: {response.subscriber_count}", f"Detalii: {response.details}"],
        "green" if response.accepted else "red",
    )


def stats(stub) -> None:
    response = stub.Stats(pb.Empty(), timeout=RPC_TIMEOUT)
    lines = [f"Abonamente: {response.subscriptions}"]
    lines += [f"Mesaje {item.status}: {item.count}" for item in response.messages]
    lines += [f"Livrari {item.status}: {item.count}" for item in response.deliveries]
    panel("Statistici gRPC", lines)


def main() -> None:
    channel, stub = connect()
    try:
        panel("Publisher gRPC", [f"Broker: {BROKER_HOST}:{BROKER_PORT}", "Protocol Buffers peste HTTP/2"])
        while True:
            print("\n1. Publica mesaj\n2. Listeaza topicuri\n3. Statistici\n0. Iesire")
            option = input("Alege: ").strip()
            try:
                if option == "1":
                    publish(stub)
                elif option == "2":
                    topics_table(stub.ListTopics(pb.Empty(), timeout=RPC_TIMEOUT))
                elif option == "3":
                    stats(stub)
                elif option == "0":
                    return
                else:
                    event("PUBLISHER-gRPC", "ERROR", "Optiune invalida", "red")
            except grpc.RpcError as error:
                event("PUBLISHER-gRPC", error.code().name, error.details(), "red")
    finally:
        channel.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        event("PUBLISHER-gRPC", "STOP", "Publisher oprit", "yellow")
