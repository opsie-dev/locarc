from concurrent.futures import CancelledError
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
from locarc.errors import ARC_ERROR
from locarc.errors import ARC_INVALID_YAML_ERROR
from locarc.errors import ARC_VALIDATION_ERROR
from locarc.logger import LOGGER
from locarc.models import Arc
from locarc.models import Subscription
from locarc.providers import get_event_provider


def safe_load_arc_file(
    arcfile: Path,
) -> Arc:
    with arcfile.open() as stream:
        try:
            return Arc(**load_yaml(stream, Loader=Loader))
        except ValidationError as e:
            LOGGER.error("Invalid YAML file:")
            for error in e.errors():
                message = error.get("msg")
                LOGGER.error(f"- ValidationError: {message}")
            raise ARC_VALIDATION_ERROR
        except YAMLError as e:
            LOGGER.error(f"Invalid YAML file, {e}, abort")
            raise ARC_INVALID_YAML_ERROR
        except Exception as e:
            LOGGER.error(f"Unexpected error occurs: {e}, abort")
            raise ARC_ERROR


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
        default="locarc.yaml",
        dir_okay=False,
        envvar="LOCARC_FILE",
        exists=True,
        file_okay=True,
        help="Path to locarc spec file to run event stack from",
        resolve_path=True,
    )],
    default_timeout: float | None = Option(
        default=None,
        envvar="LOCARC_DEFAULT_SUBSCRIPTION_TIMEOUT",
        help="The default timeout to use for waiting subscription to end",
    ),
) -> None:
    arc = safe_load_arc_file(arcfile)
    if arc.topics is not None:
        for topic in arc.topics:
            provider = get_event_provider(topic.provider)
            provider.create_topic(topic.id)
    if arc.subscriptions is None:
        LOGGER.warning("No subscription defined in the arcfile, abort.")
        raise ARC_VALIDATION_ERROR
    futures: dict[Subscription, Future] = {}
    for subscription in arc.subscriptions:
        topic = arc.get_topic_by_id(subscription.topic)
        if topic is None:
            LOGGER.error(f"Topic `{subscription.topic} is not declared in arc file.")
            raise ARC_VALIDATION_ERROR
        callbacks = parse_event_subscription_callback(subscription)
        provider = get_event_provider(subscription.provider)
        provider.create_subscription(subscription.id, subscription.topic)
        futures[subscription] = (
            provider.listen_subscription(
                subscription,
                callbacks,
            ),
        )
    try:
        for subscription, future in futures.items():
            timeout = subscription.timeout
            if timeout is None:
                timeout = default_timeout
            future.result(timeout=timeout)
    # TODO: manage exception.
    except CancelledError as e:
        pass
    except TimeoutError as e:
        pass
    except Exception as e:
        pass


if __name__ == "__main__":
    run(entrypoint)
