from types import FrameType
from typing import Any
from typing import Callable

AnyCallable = Callable[..., Any]
SignalHandler = Callable[[int, FrameType | None], None]