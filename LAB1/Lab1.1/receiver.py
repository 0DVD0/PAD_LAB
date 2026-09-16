"""Alias pastrat pentru compatibilitate: porneste Subscriber-ul."""

from subscriber import (
    choose_subscriber_id,
    choose_topic,
    parse_arguments,
    run_subscriber,
)


if __name__ == "__main__":
    args = parse_arguments()
    subscriber_id = choose_subscriber_id(args.subscriber_id)
    topic = choose_topic(args.topic)
    run_subscriber(subscriber_id, topic, args.host, args.port, args.types)
