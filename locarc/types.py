from concurrent.futures import Future
from types import FrameType
from typing import Any
from typing import Callable

AnyCallable = Callable[..., Any]
AnyFuture = Future[Any]
SignalHandler = Callable[[int, FrameType | None], None]
