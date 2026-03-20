from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
import os
from typing import Any

try:
    import redis.asyncio as redis_async  # type: ignore
except Exception:  # noqa: BLE001
    redis_async = None


class CAEventBus:
    """CA realtime bus with Redis pub/sub and in-memory fallback."""

    def __init__(self, queue_size: int = 256) -> None:
        self._queue_size = queue_size
        self._subscribers: dict[str, tuple[int, asyncio.Queue[dict[str, Any]], asyncio.Task[Any] | None]] = {}
        self._lock = asyncio.Lock()
        self._redis_url = os.getenv("ACCORD_REDIS_URL", "").strip()
        self._redis_client: Any | None = None
        self._logger = logging.getLogger("accord.realtime")

    def _channel(self, ca_id: int) -> str:
        return f"accord_events:ca:{ca_id}"

    async def _get_redis(self) -> Any | None:
        if not self._redis_url or redis_async is None:
            return None
        if self._redis_client is None:
            try:
                self._redis_client = redis_async.from_url(self._redis_url, decode_responses=True)
                await self._redis_client.ping()
            except Exception as exc:  # noqa: BLE001
                self._logger.warning("Redis unavailable for realtime bus: %s", exc)
                self._redis_client = None
                return None
        return self._redis_client

    async def subscribe(self, ca_id: int) -> tuple[str, asyncio.Queue[dict[str, Any]]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._queue_size)
        token = f"sub_{ca_id}_{datetime.utcnow().timestamp()}_{id(queue)}"

        task: asyncio.Task[Any] | None = None
        redis_client = await self._get_redis()
        if redis_client is not None:
            channel = self._channel(ca_id)
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)

            async def _pump() -> None:
                try:
                    while True:
                        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                        if message and message.get("type") == "message":
                            raw = message.get("data")
                            try:
                                payload = json.loads(str(raw))
                            except Exception:  # noqa: BLE001
                                payload = {
                                    "ca_id": ca_id,
                                    "type": "RAW",
                                    "summary": str(raw),
                                    "occurred_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                                }
                            if queue.full():
                                try:
                                    queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    pass
                            queue.put_nowait(payload)
                        await asyncio.sleep(0.05)
                except asyncio.CancelledError:
                    pass
                finally:
                    try:
                        await pubsub.unsubscribe(channel)
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        await pubsub.close()
                    except Exception:  # noqa: BLE001
                        pass

            task = asyncio.create_task(_pump())

        async with self._lock:
            self._subscribers[token] = (ca_id, queue, task)
        return token, queue

    async def unsubscribe(self, token: str) -> None:
        task: asyncio.Task[Any] | None = None
        async with self._lock:
            item = self._subscribers.pop(token, None)
            if item is not None:
                _, _, task = item
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def publish(self, event: dict[str, Any]) -> None:
        ca_id = int(event.get("ca_id") or 0)
        if ca_id <= 0:
            return

        redis_client = await self._get_redis()
        if redis_client is not None:
            try:
                await redis_client.publish(self._channel(ca_id), json.dumps(event, default=str, ensure_ascii=True))
            except Exception as exc:  # noqa: BLE001
                self._logger.warning("Redis publish failed, falling back to memory bus: %s", exc)

        async with self._lock:
            targets = [queue for target_ca_id, queue, _ in self._subscribers.values() if target_ca_id == ca_id]

        for queue in targets:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                continue


ca_event_bus = CAEventBus()


async def publish_ca_event(event: dict[str, Any]) -> None:
    await ca_event_bus.publish(event)
