from signal import signal
from signal import SIGINT
from signal import SIGTERM
from types import FrameType

from locarc.types import AnyCallable
from locarc.types import SignalHandler


def callable_to_signal_handler(
    fn: AnyCallable,
    *args,
    **kwargs,
) -> SignalHandler:

    def _handler(sig: int, frame: FrameType | None) -> None:
        fn(*args, **kwargs)
    
    return _handler


def on_exit_signal(
    fn: AnyCallable,
    *args,
    **kwargs,
) -> None:
    handler = callable_to_signal_handler(fn, *args, **kwargs)
    signal(SIGINT, handler)
    signal(SIGTERM, handler)
