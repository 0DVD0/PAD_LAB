"""Subscriber gRPC interactiv, cu port liber alocat automat."""

import argparse
import json
import uuid
from concurrent import futures

import grpc
from google.protobuf.json_format import MessageToDict

import message_broker_pb2 as pb
import message_broker_pb2_grpc as rpc
from config import (
    BROKER_HOST, BROKER_PORT, DEFAULT_SUBSCRIBER_HOST,
    RPC_TIMEOUT, SUBSCRIBER_RPC_WORKERS,
)
from console_ui import event, panel, topics_table
from validation import validate_message


def broker_stub():
    channel = grpc.insecure_channel(f"{BROKER_HOST}:{BROKER_PORT}")
    return channel, rpc.BrokerServiceStub(channel)


def choose_id(value: str | None) -> str:
    if value:
        return value
    default = f"subscriber-{uuid.uuid4().hex[:6]}"
    return input(f"Numele Subscriber-ului [{default}]: ").strip() or default


def choose_topic(value: str | None) -> str:
    if value:
        return value
    channel, stub = broker_stub()
    try:
        response = stub.ListTopics(pb.Empty(), timeout=RPC_TIMEOUT)
    finally:
        channel.close()
    names = sorted(item.topic for item in response.topics)
    if names:
        topics_table(response)
        for index, name in enumerate(names, 1):
            print(f"{index}. {name}")
        choice = input("Alege numarul sau scrie un topic nou: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(names):
            return names[int(choice) - 1]
        if choice:
            return choice
    while True:
        topic = input("Topic nou: ").strip()
        if topic:
            return topic


class SubscriberService(rpc.SubscriberServiceServicer):
    def __init__(self, subscriber_id: str) -> None:
        self.subscriber_id = subscriber_id

    def Deliver(self, request: pb.DeliveryRequest, context) -> pb.DeliveryAck:
        if request.subscriber_id != self.subscriber_id:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "Livrare pentru alt Subscriber")
        try:
            validate_message(request.message)
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        payload = MessageToDict(request.message.payload)
        panel(
            "Mesaj protobuf primit",
            [
                f"Subscriber: {self.subscriber_id}",
                f"ID: {request.message.id}",
                f"Topic: {request.message.topic}",
                f"Tip: {request.message.type}",
                f"Payload: {json.dumps(payload, ensure_ascii=False)}",
            ],
            "green",
        )
        return pb.DeliveryAck(
            success=True,
            message_id=request.message.id,
            subscriber_id=self.subscriber_id,
            details="Mesaj procesat",
        )


def run(subscriber_id: str, topic: str, host: str, port: int, accepted_types: list[str]) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=SUBSCRIBER_RPC_WORKERS))
    rpc.add_SubscriberServiceServicer_to_server(SubscriberService(subscriber_id), server)
    assigned_port = server.add_insecure_port(f"{host}:{port}")
    if assigned_port == 0:
        raise RuntimeError("Nu s-a putut aloca portul Subscriber-ului")
    server.start()

    channel, stub = broker_stub()
    subscribed = False
    try:
        response = stub.Subscribe(
            pb.SubscribeRequest(
                subscriber_id=subscriber_id, topic=topic, host=host,
                port=assigned_port, accepted_types=accepted_types,
            ),
            timeout=RPC_TIMEOUT,
        )
        if not response.success:
            raise RuntimeError(response.details)
        subscribed = True
        panel(
            "Subscriber gRPC activ",
            [
                f"ID: {subscriber_id}", f"Topic: {topic}",
                f"Endpoint: {host}:{assigned_port}" + (" (automat)" if port == 0 else ""),
                f"Tipuri: {', '.join(accepted_types) or 'toate'}",
                "Asteapta apeluri RPC (Ctrl+C pentru oprire)",
            ],
        )
        server.wait_for_termination()
    except KeyboardInterrupt:
        event("SUBSCRIBER-gRPC", "STOP", "Oprire solicitata", "yellow")
    finally:
        if subscribed:
            try:
                stub.Unsubscribe(pb.UnsubscribeRequest(subscriber_id=subscriber_id, topic=topic), timeout=RPC_TIMEOUT)
            except grpc.RpcError:
                pass
        channel.close()
        server.stop(grace=1)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Subscriber gRPC")
    parser.add_argument("--id", dest="subscriber_id")
    parser.add_argument("--topic")
    parser.add_argument("--host", default=DEFAULT_SUBSCRIBER_HOST)
    parser.add_argument("--port", type=int, default=0, help="0 = port liber automat")
    parser.add_argument("--types", nargs="*", default=[])
    return parser.parse_args()


if __name__ == "__main__":
    args = arguments()
    try:
        run(choose_id(args.subscriber_id), choose_topic(args.topic), args.host, args.port, args.types)
    except grpc.RpcError as error:
        event("SUBSCRIBER-gRPC", "RPC ERROR", error.details(), "red")
    except RuntimeError as error:
        event("SUBSCRIBER-gRPC", "ERROR", str(error), "red")
