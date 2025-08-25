from functools import lru_cache

from locarc.events import EventProviderProtocol
from locarc.models import EventProvider


@lru_cache
def get_event_provider(
    event_provider_name: EventProvider,
) -> EventProviderProtocol:
    if event_provider_name == EventProvider.PUBSUB:
        from locarc.providers.pubsub import PubsubEventProvider
        # TODO: parse settings for configuration.
        return PubsubEventProvider.create(
            ...,
        )
    raise ValueError(f"Unsupported provider `{event_provider_name}`")
