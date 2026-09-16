"""Broker gRPC cu topicuri, protobuf, SQLite, workeri si retry."""

import queue
import sqlite3
import threading
from concurrent import futures

import grpc

import message_broker_pb2 as pb
import message_broker_pb2_grpc as rpc
from config import (
    BROKER_HOST, BROKER_PORT, BROKER_RPC_WORKERS, DATABASE_PATH,
    DELIVERY_WORKERS, RETRY_SCAN_INTERVAL, RPC_TIMEOUT,
)
from console_ui import event
from storage import MessageStorage
from subscriptions import SubscriberEndpoint, SubscriptionRegistry
from validation import validate_message, validate_subscription

storage = MessageStorage(DATABASE_PATH)
registry = SubscriptionRegistry()
delivery_queue: queue.Queue[int] = queue.Queue()
stop_event = threading.Event()


def invalid_argument(context: grpc.ServicerContext, error: Exception) -> None:
    context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))


class BrokerService(rpc.BrokerServiceServicer):
    def Subscribe(self, request: pb.SubscribeRequest, context) -> pb.OperationResponse:
        try:
            validate_subscription(request)
        except ValueError as error:
            invalid_argument(context, error)
        endpoint = SubscriberEndpoint(
            request.subscriber_id, request.host, request.port,
            tuple(request.accepted_types),
        )
        storage.save_subscription(endpoint, request.topic)
        registry.subscribe(endpoint, request.topic)
        event("BROKER-gRPC", "SUBSCRIBE", f"{request.subscriber_id} -> {request.topic}", "green")
        return pb.OperationResponse(success=True, details="Abonare realizata")

    def Unsubscribe(self, request: pb.UnsubscribeRequest, context) -> pb.OperationResponse:
        if not request.subscriber_id or not request.topic:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "ID-ul si topicul sunt obligatorii")
        removed = registry.unsubscribe(request.subscriber_id, request.topic)
        storage.remove_subscription(request.subscriber_id, request.topic)
        return pb.OperationResponse(success=removed, details="Dezabonat" if removed else "Abonament inexistent")

    def Publish(self, request: pb.PublishRequest, context) -> pb.PublishResponse:
        try:
            validate_message(request.message)
            subscribers = registry.subscribers_for(request.message.topic, request.message.type)
            storage.save_message(request.message, subscribers)
        except ValueError as error:
            invalid_argument(context, error)
        except sqlite3.IntegrityError:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "ID-ul mesajului exista deja")
        for delivery_id in storage.due_ids():
            delivery_queue.put(delivery_id)
        event("BROKER-gRPC", "PUBLISH", f"{request.message.id} topic={request.message.topic} abonati={len(subscribers)}", "magenta")
        return pb.PublishResponse(
            accepted=True, message_id=request.message.id,
            subscriber_count=len(subscribers),
            details="Mesaj protobuf salvat pentru livrare" if subscribers else "Mesaj salvat fara abonati compatibili",
        )

    def ListTopics(self, request: pb.Empty, context) -> pb.TopicsResponse:
        response = pb.TopicsResponse()
        for topic_name, endpoints in sorted(registry.snapshot().items()):
            topic = response.topics.add(topic=topic_name)
            for endpoint in endpoints:
                item = topic.subscribers.add(
                    subscriber_id=endpoint.subscriber_id,
                    host=endpoint.host,
                    port=endpoint.port,
                )
                item.accepted_types.extend(endpoint.accepted_types)
        return response

    def Stats(self, request: pb.Empty, context) -> pb.StatsResponse:
        messages, deliveries, subscriptions = storage.stats()
        response = pb.StatsResponse(subscriptions=subscriptions)
        response.messages.extend(pb.CountByStatus(status=k, count=v) for k, v in messages.items())
        response.deliveries.extend(pb.CountByStatus(status=k, count=v) for k, v in deliveries.items())
        return response


def deliver(task: dict) -> None:
    channel = grpc.insecure_channel(f"{task['host']}:{task['port']}")
    try:
        stub = rpc.SubscriberServiceStub(channel)
        response = stub.Deliver(
            pb.DeliveryRequest(message=task["message"], subscriber_id=task["subscriber_id"]),
            timeout=RPC_TIMEOUT,
        )
    finally:
        channel.close()
    if not response.success or response.message_id != task["message_id"] or response.subscriber_id != task["subscriber_id"]:
        raise RuntimeError(response.details or "ACK gRPC invalid")


def delivery_worker() -> None:
    while not stop_event.is_set():
        try:
            delivery_id = delivery_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        task = None
        try:
            task = storage.claim(delivery_id)
            if task is None:
                continue
            deliver(task)
            storage.delivered(delivery_id)
            event("BROKER-gRPC", "DELIVERED", f"{task['message_id']} -> {task['subscriber_id']}", "green")
        except Exception as error:
            if task is not None:
                status = storage.failed_attempt(delivery_id, task["attempts"], str(error))
                event("BROKER-gRPC", status.upper(), f"{task['message_id']} -> {task['subscriber_id']}: {error}", "red" if status == "failed" else "yellow")
        finally:
            delivery_queue.task_done()


def retry_scheduler() -> None:
    while not stop_event.wait(RETRY_SCAN_INTERVAL):
        for delivery_id in storage.due_ids():
            delivery_queue.put(delivery_id)


def restore_and_start_workers() -> None:
    storage.initialize()
    for topic, endpoint in storage.load_subscriptions():
        registry.subscribe(endpoint, topic)
    for delivery_id in storage.due_ids():
        delivery_queue.put(delivery_id)
    for index in range(DELIVERY_WORKERS):
        threading.Thread(target=delivery_worker, name=f"delivery-worker-{index + 1}", daemon=True).start()
    threading.Thread(target=retry_scheduler, name="retry-scheduler", daemon=True).start()


def serve() -> None:
    restore_and_start_workers()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=BROKER_RPC_WORKERS))
    rpc.add_BrokerServiceServicer_to_server(BrokerService(), server)
    address = f"{BROKER_HOST}:{BROKER_PORT}"
    server.add_insecure_port(address)
    server.start()
    event("BROKER-gRPC", "START", f"asculta pe {address}; DB={DATABASE_PATH}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        stop_event.set()
        server.stop(grace=2)
        event("BROKER-gRPC", "STOP", "Broker oprit", "yellow")


if __name__ == "__main__":
    serve()
