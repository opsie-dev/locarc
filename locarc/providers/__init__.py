from functools import lru_cache

from locarc.events import EventProviderProtocol
from locarc.models import EventProvider


def create_pubsub_provider() -> EventProviderProtocol:
    from locarc.providers.pubsub import PubsubEventProvider
    from locarc.providers.pubsub import PubsubSettings

    settings = PubsubSettings()
    return PubsubEventProvider.create(
        settings.project_id,
    )


@lru_cache
def get_event_provider(
    event_provider_name: EventProvider,
) -> EventProviderProtocol:
    if event_provider_name == EventProvider.PUBSUB:
        return create_pubsub_provider()
    # NOTE: add additional event provider here.
    raise ValueError(f"Unsupported provider `{event_provider_name}`")
