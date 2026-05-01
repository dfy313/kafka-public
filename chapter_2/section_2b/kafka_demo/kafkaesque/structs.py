from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True, slots=True)
class RecordMetadata:
    topic: str
    partition: int
    offset: int
    key: str
    value: Any

class SendFuture:
    """Compatibility shim so app code can call .get(timeout=...)"""
    def __init__(self, md: RecordMetadata):
        self._md = md
    def get(self, timeout=None) -> RecordMetadata:
        return self._md # Already synchronous; timeout is ignored

class Color:
    WHITE   = "\033[0m"
    CYAN    = "\033[96m"
    YELLOW  = "\033[93m"
    MAGENTA = "\033[95m"
    PINK    = "\033[38;5;212m"
    BLUE    = "\033[94m"
    GREEN   = "\033[92m"
    GOLD    = "\033[38;5;178m"
