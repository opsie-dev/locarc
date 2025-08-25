from concurrent.futures import Future
from pathlib import Path
from typing_extensions import Annotated

from pydantic import ValidationError
from typer import Option
from typer import run
from yaml import YAMLError
from yaml import load as load_yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from locarc.callbacks import EventCallback
from locarc.callbacks import ServiceCallback
from locarc.callbacks import TopicCallback
from locarc.providers import get_event_provider
from locarc.models import Arc
from locarc.models import Subscription


def safe_load_arc_file(
    arcfile: Path,
) -> Arc:
    with arcfile.open() as stream:
        try:
            return Arc(**load_yaml(stream, Loader=Loader))
        except ValidationError as e:
            pass
        except YAMLError as e:
            pass
        except Exception as e:
            pass


def parse_event_subscription_callback(
    subscription: Subscription,
) -> list[EventCallback]:
    callbacks: list[EventCallback] = []
    if subscription.destinations.services is not None:
        for service in subscription.destinations.services:
            callbacks.append(ServiceCallback(service))
    if subscription.destinations.topics is not None:
        for topic in subscription.destinations.topics:
            callbacks.append(TopicCallback(topic))
    return callbacks


def entrypoint(
    arcfile: Annotated[Path, Option(
        help="TBD",
    )],
) -> None:
    arc = safe_load_arc_file(arcfile)
    if arc.topics is not None:
        for topic in arc.topics:
            provider = get_event_provider(topic.provider)
            provider.create_topic(topic.id)
    if arc.subscriptions is None:
        return
    futures: list[Future] = []
    for subscription in arc.subscriptions:
        topic = arc.get_topic_by_id(subscription.topic)
        if topic is None:
            # TODO: manage error at CLI level.
            raise KeyError(f"Topic `{subscription.topic} is not declared in arc file.") 
        callbacks = parse_event_subscription_callback(subscription)
        provider = get_event_provider(subscription.provider)
        provider.create_subscription(subscription.id, subscription.topic)
        futures.append(
            provider.listen_subscription(
                subscription,
                callbacks,
            ),
        )
    try:
        for future in futures:
            future.exception
    except Exception as e:
        # TODO: manage exception.
        pass


if __name__ == "__main__":
    run(entrypoint)
