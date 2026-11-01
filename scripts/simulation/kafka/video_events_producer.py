

"""Video Events Producer — Phase 3: Kafka Event Emission (Transport)

Produces InteractionEntity events to Kafka topic `video_interactions`.
Optional dirty-injection hook for anomaly testing.
See README.md#phase-markers-used-in-code-comments
"""

import json

from kafka import KafkaProducer

from config import (
    KAFKA_SERVICE_URL,
    KAFKA_SERVICE_CERT,
    KAFKA_SERVICE_KEY,
    KAFKA_CA_CERT,
)

from ..clock import BusinessClock
from .interaction_factory import InteractionFactory
from ..random_provider import RandomProvider


TOPIC_NAME = "video_interactions"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVICE_URL,
        security_protocol="SSL",
        ssl_cafile=KAFKA_CA_CERT,
        ssl_certfile=KAFKA_SERVICE_CERT,
        ssl_keyfile=KAFKA_SERVICE_KEY,
        value_serializer=lambda value: json.dumps(
            value,
            default=str,
        ).encode("utf-8"),
    )


def produce_events(
    *,
    producer: KafkaProducer,
    event_generator,
):
    count = 0

    for event in event_generator:

        producer.send(
            TOPIC_NAME,
            value=event.to_dict(),
        )

        count += 1

    producer.flush()

    return count


# def run_producer(
#     *,
#     clock: BusinessClock,
#     random_provider: RandomProvider,
#     user_ids,
#     video_ids,
#     event_count: int,
# ):
#     interaction_factory = InteractionFactory(
#         clock=clock,
#         random_provider=random_provider,
#     )

#     event_generator = interaction_factory.generate(
#         user_ids=user_ids,
#         video_ids=video_ids,
#         count=event_count,
#     )

#     producer = create_producer()

#     try:
#         count = produce_events(
#             producer=producer,
#             event_generator=event_generator,
#         )

#         print(
#             f"Kafka producer sent {count} events"
#         )

#     finally:
#         producer.close()
#         print("Kafka producer connection closed")
def run_producer(
    *,
    clock: BusinessClock,
    random_provider: RandomProvider,
    user_ids,
    video_ids,
    event_count: int,
    dirty_injector=None,
    anomaly_rate: float = 0.0,
    events=None,  # NEW: optional pre-generated events
):
    if events is not None:
        event_generator = iter(events)
    else:
        interaction_factory = InteractionFactory(
            clock=clock,
            random_provider=random_provider,
        )

        event_generator = interaction_factory.generate(
            user_ids=user_ids,
            video_ids=video_ids,
            count=event_count,
        )

    if dirty_injector is not None and anomaly_rate > 0:
        event_generator = dirty_injector.inject(
            events=event_generator,
            anomaly_rate=anomaly_rate,
        )

    producer = create_producer()

    try:
        count = produce_events(
            producer=producer,
            event_generator=event_generator,
        )

        print(
            f"Kafka producer sent {count} events"
        )

    finally:
        producer.close()
        print("Kafka producer connection closed")