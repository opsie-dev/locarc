from typing import Callable

from httpx import Client

from locarc.events import EventProtocol
from locarc.models import Service
from locarc.models import Topic
from locarc.providers import get_event_provider

EventCallback = Callable[[EventProtocol], None]


def ServiceCallback(service: Service) -> EventCallback:
    client = Client(base_url=str(service.url))

    def _callback(event: EventProtocol) -> None:
        response = client.request(
            service.method,
            service.path,
            json=event.json(),
        )
        response.raise_for_status()
        event.ack()

    return _callback


def TopicCallback(topic: Topic) -> EventCallback:
    provider = get_event_provider(topic.provider)

    def _callback(event: EventProtocol) -> None:
        provider.publish_event(topic, event)

    return _callback
