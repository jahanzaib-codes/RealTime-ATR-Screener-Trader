import asyncio
from dataclasses import dataclass
from typing import Any, Dict, AsyncGenerator, Set


@dataclass
class Event:
    type: str
    payload: Dict[str, Any]


class EventBus:
    def __init__(self) -> None:
        # Each subscriber gets its own queue to ensure broadcast delivery
        self._subscribers: Set[asyncio.Queue[Event]] = set()
        self._lock = asyncio.Lock()

    async def publish(self, event_type: str, payload: Dict[str, Any]) -> None:
        event = Event(event_type, payload)
        async with self._lock:
            # Fan-out to all subscriber queues
            for q in list(self._subscribers):
                # Use put_nowait to avoid deadlocks if a consumer is slow; drop only if queue is full
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    # Best-effort delivery; slow consumers may miss events
                    pass

    async def subscribe(self) -> AsyncGenerator[Event, None]:
        # Individual queue per subscriber for broadcast behavior
        q: asyncio.Queue[Event] = asyncio.Queue(maxsize=1000)
        async with self._lock:
            self._subscribers.add(q)
        try:
            while True:
                event = await q.get()
                yield event
        finally:
            # Unsubscribe when the generator is closed/cancelled
            async with self._lock:
                self._subscribers.discard(q)
