"""Persistent transport from the AstrBot plugin to the Hermes platform adapter."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import json
import logging
import mimetypes
from pathlib import Path
import tempfile
from typing import Any, Awaitable, Callable, Optional
import uuid

import aiohttp

from .bridge_protocol import (
    CHUNK_SIZE,
    PROTOCOL_VERSION,
    build_websocket_url,
    resolve_delivery_temp_dir,
    safe_filename,
    sha256_path,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AttachmentSource:
    path: str
    filename: str
    mime_type: str
    kind: str


@dataclass(frozen=True)
class DeliveryRoute:
    platform_id: str
    message_type: Any
    session_id: str


@dataclass
class _IncomingDelivery:
    payload: dict[str, Any]
    path: Path
    handle: Any
    digest: Any
    size: int = 0


DeliveryHandler = Callable[[dict[str, Any], Optional[str], DeliveryRoute], Awaitable[str | None]]


class AstrBotConnector:
    def __init__(
        self,
        *,
        gateway_url: str,
        profile: str,
        token: str,
        idle_timeout: int,
        delivery_handler: DeliveryHandler,
    ) -> None:
        self.ws_url = build_websocket_url(gateway_url, profile)
        self.profile = profile
        self.token = token
        self.idle_timeout = max(0, int(idle_timeout))
        self.delivery_handler = delivery_handler
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._supervisor_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._accepted: dict[str, asyncio.Future] = {}
        self._routes: dict[str, DeliveryRoute] = {}
        self._active_turns: dict[str, set[str]] = {}
        self._incoming: Optional[_IncomingDelivery] = None
        self._idle_tasks: dict[str, asyncio.Task] = {}
        self._closed = False

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("connector is closed")
        if self._supervisor_task is not None and not self._supervisor_task.done():
            return
        self._supervisor_task = asyncio.create_task(self._supervise_connection())

    async def ensure_connected(self) -> None:
        if self.connected:
            return
        async with self._connect_lock:
            if self.connected:
                return
            await self._close_transport()
            headers = {"Authorization": f"Bearer {self.token}"}
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
            try:
                self._ws = await self._session.ws_connect(
                    self.ws_url,
                    headers=headers,
                    heartbeat=30,
                    max_msg_size=CHUNK_SIZE * 2,
                )
            except Exception:
                await self._close_transport()
                raise
            self._ready.clear()
            self._receive_task = asyncio.create_task(self._receive_loop())
            await self._ws.send_json(
                {"type": "hello", "protocol": PROTOCOL_VERSION, "profile": self.profile}
            )
            try:
                await asyncio.wait_for(self._ready.wait(), timeout=10)
            except Exception:
                await self._close_transport()
                raise
            logger.info("[gateway_universal] 已连接 Hermes AstrBot 平台适配器: %s", self.ws_url)

    async def send_turn(
        self,
        payload: dict[str, Any],
        attachments: list[AttachmentSource],
        route: DeliveryRoute,
    ) -> str:
        if self._closed:
            raise RuntimeError("connector is closed")
        await self.ensure_connected()
        assert self._ws is not None
        turn_id = str(payload.get("turn_id") or uuid.uuid4().hex)
        chat_id = str(payload["chat_id"])
        self._routes[chat_id] = route

        attachment_meta = []
        sources: list[tuple[str, AttachmentSource, int, str]] = []
        for source in attachments:
            path = Path(source.path)
            if not path.is_file():
                continue
            blob_id = uuid.uuid4().hex
            size = path.stat().st_size
            checksum = sha256_path(path)
            meta = {
                "blob_id": blob_id,
                "filename": safe_filename(source.filename or path.name),
                "mime_type": source.mime_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                "kind": source.kind,
                "size": size,
                "sha256": checksum,
            }
            attachment_meta.append(meta)
            sources.append((blob_id, source, size, checksum))

        frame = dict(payload)
        frame.update({"type": "turn.start", "turn_id": turn_id, "attachments": attachment_meta})
        future = asyncio.get_running_loop().create_future()
        self._accepted[turn_id] = future
        try:
            async with self._send_lock:
                await self._ws.send_json(frame)
                for blob_id, source, size, checksum in sources:
                    path = Path(source.path)
                    await self._ws.send_json(
                        {
                            "type": "blob.start",
                            "blob_id": blob_id,
                            "filename": safe_filename(source.filename or path.name),
                            "mime_type": source.mime_type or "application/octet-stream",
                            "kind": source.kind,
                            "size": size,
                            "sha256": checksum,
                        }
                    )
                    with path.open("rb") as handle:
                        while chunk := handle.read(CHUNK_SIZE):
                            await self._ws.send_bytes(chunk)
                    await self._ws.send_json({"type": "blob.end", "blob_id": blob_id})
                await self._ws.send_json({"type": "turn.commit", "turn_id": turn_id})
        await asyncio.wait_for(future, timeout=15)
        self._active_turns.setdefault(chat_id, set()).add(turn_id)
        if self.idle_timeout > 0:
            self._refresh_idle(chat_id)
        return turn_id
        finally:
            self._accepted.pop(turn_id, None)

    async def close(self) -> None:
        self._closed = True
        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            await asyncio.gather(self._supervisor_task, return_exceptions=True)
            self._supervisor_task = None
        for task in self._idle_tasks.values():
            task.cancel()
        self._idle_tasks.clear()
        await self._close_transport()

    async def _supervise_connection(self) -> None:
        retry_delay = 2
        while not self._closed:
            try:
                await self.ensure_connected()
                retry_delay = 2
                receive_task = self._receive_task
                if receive_task is None:
                    await asyncio.sleep(retry_delay)
                    continue
                await receive_task
                if not self._closed:
                    logger.warning(
                        "[gateway_universal] Hermes 适配器连接已断开，将自动重连"
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[gateway_universal] Hermes 适配器重连失败，%s 秒后重试: %s",
                    retry_delay,
                    exc,
                )
            finally:
                if not self._closed:
                    await self._close_transport()
            if not self._closed:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    async def _close_transport(self) -> None:
        current = asyncio.current_task()
        if self._receive_task is not None and self._receive_task is not current:
            self._receive_task.cancel()
            await asyncio.gather(self._receive_task, return_exceptions=True)
        self._receive_task = None
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._ready.clear()
        self._cleanup_incoming()

    async def _receive_loop(self) -> None:
        assert self._ws is not None
        ws = self._ws
        try:
            async for message in ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    try:
                        payload = json.loads(message.data)
                    except json.JSONDecodeError:
                        continue
                    await self._handle_control(payload)
                elif message.type == aiohttp.WSMsgType.BINARY:
                    incoming = self._incoming
                    if incoming is None:
                        continue
                    incoming.handle.write(message.data)
                    incoming.digest.update(message.data)
                    incoming.size += len(message.data)
                elif message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                }:
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[gateway_universal] Hermes 适配器连接异常")
        finally:
            if self._ws is ws:
                self._ws = None
                self._ready.clear()
            for future in self._accepted.values():
                if not future.done():
                    future.set_exception(ConnectionError("Hermes adapter disconnected"))
            self._cleanup_incoming()

    async def _handle_control(self, payload: dict[str, Any]) -> None:
        frame_type = payload.get("type")
        if frame_type == "hello.ready":
            if int(payload.get("protocol") or 0) == PROTOCOL_VERSION:
                self._ready.set()
            return
        if frame_type == "turn.accepted":
            future = self._accepted.get(str(payload.get("turn_id") or ""))
            if future is not None and not future.done():
                future.set_result(payload)
            return
        if frame_type == "activity":
            chat_id = str(payload.get("chat_id") or "")
            if self._active_turns.get(chat_id):
                self._refresh_idle(chat_id)
            return
        if frame_type in {"turn.finish", "turn.completed", "turn.done"}:
            chat_id = str(payload.get("chat_id") or "")
            self._cancel_idle(chat_id)
            self._active_turns.pop(chat_id, None)
            status = str(payload.get("status") or "")
            error = payload.get("error")
            if status in {"error", "failure"} and error:
                logger.warning("[gateway_universal] Hermes turn 异常结束: %s", error)
            return
        if frame_type in {"delivery.text", "delivery.remote"}:
            await self._deliver(payload, None)
            return
        if frame_type == "delivery.start":
            self._cleanup_incoming()
            temp_dir = resolve_delivery_temp_dir()
            temp = tempfile.NamedTemporaryFile(
                dir=str(temp_dir),
                prefix="hermes_out_",
                delete=False,
            )
            self._incoming = _IncomingDelivery(
                payload=payload,
                path=Path(temp.name),
                handle=temp,
                digest=hashlib.sha256(),
            )
            return
        if frame_type == "delivery.end":
            incoming = self._incoming
            self._incoming = None
            if incoming is None:
                return
            incoming.handle.close()
            expected_size = int(incoming.payload.get("size") or 0)
            expected_sha = str(incoming.payload.get("sha256") or "")
            if incoming.size != expected_size or (
                expected_sha and incoming.digest.hexdigest() != expected_sha
            ):
                incoming.path.unlink(missing_ok=True)
                await self._ack(incoming.payload, False, error="attachment verification failed")
                return
            try:
                await self._deliver(incoming.payload, str(incoming.path))
            finally:
                incoming.path.unlink(missing_ok=True)

    async def _deliver(self, payload: dict[str, Any], path: Optional[str]) -> None:
        chat_id = str(payload.get("chat_id") or "")
        route = self._routes.get(chat_id)
        if route is None:
            await self._ack(payload, False, error="unknown AstrBot chat route")
            return
        self._cancel_idle(chat_id)
        self._active_turns.pop(chat_id, None)
        try:
            message_id = await self.delivery_handler(payload, path, route)
        except Exception as exc:
            logger.exception("[gateway_universal] 向 AstrBot 投递响应失败")
            await self._ack(payload, False, error=str(exc))
            return
        await self._ack(payload, True, message_id=message_id)

    async def _ack(
        self,
        payload: dict[str, Any],
        success: bool,
        *,
        message_id: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            return
        await ws.send_json(
            {
                "type": "delivery.ack",
                "delivery_id": payload.get("delivery_id"),
                "success": success,
                "message_id": message_id,
                "error": error,
            }
        )

    def _refresh_idle(self, chat_id: str) -> None:
        if not chat_id or self.idle_timeout <= 0:
            return
        self._cancel_idle(chat_id)
        self._idle_tasks[chat_id] = asyncio.create_task(self._idle_watch(chat_id))

    def _cancel_idle(self, chat_id: str) -> None:
        task = self._idle_tasks.pop(chat_id, None)
        if task is not None:
            task.cancel()

    async def _idle_watch(self, chat_id: str) -> None:
        try:
            await asyncio.sleep(self.idle_timeout)
            if not self._active_turns.get(chat_id):
                return
            route = self._routes.get(chat_id)
            if route is None:
                return
            await self.delivery_handler(
                {
                    "type": "delivery.text",
                    "chat_id": chat_id,
                    "text": f"Hermes 已连续 {self.idle_timeout} 秒没有返回活动，任务可能仍在后台运行。",
                },
                None,
                route,
            )
        except asyncio.CancelledError:
            return
        finally:
            self._idle_tasks.pop(chat_id, None)

    def _cleanup_incoming(self) -> None:
        incoming = self._incoming
        self._incoming = None
        if incoming is None:
            return
        try:
            if not incoming.handle.closed:
                incoming.handle.close()
        except Exception:
            pass
        try:
            incoming.path.unlink(missing_ok=True)
        except OSError:
            pass


