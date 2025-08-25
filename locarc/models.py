from enum import StrEnum

from pydantic import BaseModel
from pydantic import HttpUrl


class EventProvider(StrEnum):
    PUBSUB = "pubsub"


class Topic(BaseModel):
    id: str
    provider: EventProvider


class ServiceMethod(StrEnum):
    POST = "POST"


class Service(BaseModel):
    id: str
    url: HttpUrl

    method: ServiceMethod = ServiceMethod.POST
    path: str = "/"


class SubscriptionDestinations(BaseModel):
    services: list[str] | None = None
    topics: list[str] | None = None


class Subscription(BaseModel):
    id: str
    destinations: SubscriptionDestinations
    provider: EventProvider
    timeout: float | None = None
    topic: str


class Arc(BaseModel):
    services: list[Service] | None = None
    subscriptions: list[Subscription] | None = None
    topics: list[Topic]

    def get_service_by_id(self, service_id: str) -> Service:
        if self.services is None:
            raise KeyError(f"No service are declared in arc file.")
        for service in self.services:
            if service.id == service_id:
                return service
        raise KeyError(f"Service `{service_id} is not declared in arc file.")

    def get_topic_by_id(self, topic_id: str) -> Topic:
        if self.topics is None:
            raise KeyError(f"No topic are declared in arc file.")
        for topic in self.topics:
            if topic.id == topic_id:
                return topic
        raise KeyError(f"Topic `{topic_id} is not declared in arc file.")
