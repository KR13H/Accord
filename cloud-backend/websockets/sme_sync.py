from __future__ import annotations

import asyncio
import json
import threading
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect


class SmeSyncConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, business_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections[business_id].add(websocket)

    async def disconnect(self, business_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            sockets = self._connections.get(business_id)
            if sockets is None:
                return
            sockets.discard(websocket)
            if not sockets:
                self._connections.pop(business_id, None)

    async def broadcast(self, business_id: str, payload: dict[str, Any]) -> None:
        async with self._lock:
            targets = list(self._connections.get(business_id, set()))

        if not targets:
            return

        dead: list[WebSocket] = []
        message = json.dumps(payload, ensure_ascii=True, default=str)
        for socket in targets:
            try:
                await socket.send_text(message)
            except Exception:  # noqa: BLE001
                dead.append(socket)

        if dead:
            async with self._lock:
                bucket = self._connections.get(business_id)
                if bucket is None:
                    return
                for socket in dead:
                    bucket.discard(socket)
                if not bucket:
                    self._connections.pop(business_id, None)


sme_sync_manager = SmeSyncConnectionManager()


def fire_and_forget_business_event(business_id: str, payload: dict[str, Any]) -> None:
    async def _broadcast() -> None:
        await sme_sync_manager.broadcast(business_id, payload)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_broadcast())
    except RuntimeError:
        threading.Thread(target=lambda: asyncio.run(_broadcast()), daemon=True).start()


def create_sme_sync_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1/sme/ws", tags=["sme", "websocket"])

    @router.websocket("/sync")
    async def sme_sync_websocket(
        websocket: WebSocket,
        business_id: str = Query(default="SME-001"),
    ) -> None:
        clean_business_id = (business_id or "SME-001").strip() or "SME-001"
        await sme_sync_manager.connect(clean_business_id, websocket)
        await sme_sync_manager.broadcast(
            clean_business_id,
            {
                "event": "SYNC_CONNECTED",
                "business_id": clean_business_id,
            },
        )

        try:
            while True:
                _ = await websocket.receive_text()
        except WebSocketDisconnect:
            await sme_sync_manager.disconnect(clean_business_id, websocket)
        except Exception:  # noqa: BLE001
            await sme_sync_manager.disconnect(clean_business_id, websocket)

    return router
