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

    def get_service_by_id(self, service_id: str) -> Service | None:
        if self.services is None:
            return None
        for service in self.services:
            if service.id == service_id:
                return service
        return None

    def get_topic_by_id(self, topic_id: str) -> Topic | None:
        if self.topics is None:
            return None
        for topic in self.topics:
            if topic.id == topic_id:
                return topic
        return None
