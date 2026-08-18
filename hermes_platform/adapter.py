"""Profile-local Hermes platform adapter for AstrBot."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import hmac
import json
import logging
import mimetypes
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Optional
import uuid

from aiohttp import WSMsgType, web

from agent.secret_scope import UnscopedSecretError
from agent.secret_scope import get_secret as get_scoped_secret

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_media_bytes,
)


logger = logging.getLogger(__name__)
PROTOCOL_VERSION = 1
CHUNK_SIZE = 256 * 1024


def _secret(name: str) -> str:
    try:
        value = get_scoped_secret(name, "")
    except UnscopedSecretError:
        value = os.getenv(name, "")
    return str(value or "")


@dataclass
class _IncomingBlob:
    blob_id: str
    filename: str
    mime_type: str
    kind: str
    expected_size: int
    expected_sha256: str
    path: Path
    handle: Any
    digest: Any = field(default_factory=hashlib.sha256)
    size: int = 0


@dataclass
class _IncomingTurn:
    turn_id: str
    payload: dict[str, Any]
    blobs: dict[str, _IncomingBlob] = field(default_factory=dict)
    current_blob: Optional[_IncomingBlob] = None


class AstrBotAdapter(BasePlatformAdapter):
    """Expose AstrBot as a first-class Hermes messaging platform."""

    def __init__(self, config, **_kwargs):
        super().__init__(config=config, platform=Platform("astrbot"))
        extra = getattr(config, "extra", {}) or {}
        self.host = str(os.getenv("ASTRBOT_BRIDGE_HOST") or extra.get("host") or "0.0.0.0")
        self.port = int(os.getenv("ASTRBOT_BRIDGE_PORT") or extra.get("port") or 8643)
        self.token = str(
            _secret("ASTRBOT_BRIDGE_TOKEN")
            or extra.get("token")
            or _secret("API_SERVER_KEY")
            or ""
        )
        self.max_file_bytes = int(
            os.getenv("ASTRBOT_BRIDGE_MAX_FILE_BYTES")
            or extra.get("max_file_bytes")
            or 100 * 1024 * 1024
        )
        self.delivery_timeout = float(extra.get("delivery_timeout") or 60)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._ws: Optional[web.WebSocketResponse] = None
        self._send_lock = asyncio.Lock()
        self._pending_acks: dict[str, asyncio.Future] = {}
        self._turn_tasks: set[asyncio.Task] = set()

    @property
    def name(self) -> str:
        return "AstrBot"

    @property
    def authorization_is_upstream(self) -> bool:
        """Inbound messages are authenticated via token and authorized by AstrBot."""
        return True

    @property
    def enforces_own_access_policy(self) -> bool:
        """AstrBot plugin restricts commands to AstrBot administrators."""
        return True

    async def on_processing_complete(self, event: MessageEvent, outcome: Any) -> None:
        """Signal turn completion to the AstrBot connector to cancel idle watchers."""
        await super().on_processing_complete(event, outcome)
        chat_id = getattr(event.source, "chat_id", None)
        turn_id = getattr(event, "message_id", None)
        if chat_id:
            outcome_val = outcome.value if hasattr(outcome, "value") else str(outcome)
            await self._send_turn_finished(turn_id or "", chat_id, status=outcome_val)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.token:
            self._set_fatal_error(
                "config_missing",
                "ASTRBOT_BRIDGE_TOKEN or API_SERVER_KEY is required",
                retryable=False,
            )
            return False
        app = web.Application(client_max_size=self.max_file_bytes)
        app.router.add_get("/astrbot/ws", self._handle_ws)
        app.router.add_get("/p/{profile}/astrbot/ws", self._handle_ws)
        app.router.add_get("/health", self._health)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        try:
            await self._site.start()
        except OSError as exc:
            logger.error("AstrBot adapter failed to bind %s:%s: %s", self.host, self.port, exc)
            self._set_fatal_error("bind_failed", str(exc), retryable=True)
            return False
        self._mark_connected()
        logger.info("AstrBot adapter listening on %s:%s", self.host, self.port)
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close(code=1001, message=b"gateway shutdown")
        for task in list(self._turn_tasks):
            task.cancel()
        if self._turn_tasks:
            await asyncio.gather(*self._turn_tasks, return_exceptions=True)
        if self._runner is not None:
            await self._runner.cleanup()
        self._ws = None
        self._runner = None
        self._site = None

    async def _health(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "platform": "astrbot", "connected": self._ws is not None})

    def _authorized(self, request: web.Request) -> bool:
        supplied = request.headers.get("Authorization", "")
        expected = f"Bearer {self.token}"
        return hmac.compare_digest(supplied, expected)

    async def _handle_ws(self, request: web.Request) -> web.StreamResponse:
        if not self._authorized(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        ws = web.WebSocketResponse(heartbeat=30, max_msg_size=CHUNK_SIZE * 2)
        await ws.prepare(request)
        if self._ws is not None and not self._ws.closed:
            await self._ws.close(code=4000, message=b"replaced by a new connector")
        self._ws = ws
        turn: Optional[_IncomingTurn] = None
        await ws.send_json({"type": "hello.ready", "protocol": PROTOCOL_VERSION})
        logger.info("AstrBot connector connected")
        try:
            async for message in ws:
                if message.type == WSMsgType.TEXT:
                    try:
                        payload = json.loads(message.data)
                    except json.JSONDecodeError:
                        await ws.send_json({"type": "error", "error": "invalid_json"})
                        continue
                    turn = await self._handle_control(ws, turn, payload)
                elif message.type == WSMsgType.BINARY:
                    if turn is None or turn.current_blob is None:
                        await ws.send_json({"type": "error", "error": "unexpected_binary"})
                        continue
                    blob = turn.current_blob
                    blob.handle.write(message.data)
                    blob.digest.update(message.data)
                    blob.size += len(message.data)
                    if blob.size > self.max_file_bytes:
                        raise ValueError("attachment_too_large")
                elif message.type in {WSMsgType.ERROR, WSMsgType.CLOSE, WSMsgType.CLOSED}:
                    break
        except Exception as exc:
            logger.warning("AstrBot connector session failed: %s", exc)
        finally:
            self._cleanup_turn(turn)
            if self._ws is ws:
                self._ws = None
            logger.info("AstrBot connector disconnected")
        return ws

    async def _handle_control(
        self,
        ws: web.WebSocketResponse,
        turn: Optional[_IncomingTurn],
        payload: dict[str, Any],
    ) -> Optional[_IncomingTurn]:
        frame_type = payload.get("type")
        if frame_type == "hello":
            if int(payload.get("protocol") or 0) != PROTOCOL_VERSION:
                await ws.close(code=4002, message=b"unsupported protocol")
            return turn
        if frame_type == "ping":
            await ws.send_json({"type": "pong"})
            return turn
        if frame_type == "delivery.ack":
            delivery_id = str(payload.get("delivery_id") or "")
            future = self._pending_acks.pop(delivery_id, None)
            if future is not None and not future.done():
                future.set_result(payload)
            return turn
        if frame_type == "turn.start":
            self._cleanup_turn(turn)
            return _IncomingTurn(str(payload.get("turn_id") or uuid.uuid4().hex), payload)
        if frame_type == "blob.start":
            if turn is None:
                raise ValueError("blob_without_turn")
            if turn.current_blob is not None:
                raise ValueError("blob_already_open")
            expected_size = int(payload.get("size") or 0)
            if expected_size < 0 or expected_size > self.max_file_bytes:
                raise ValueError("attachment_too_large")
            temp = tempfile.NamedTemporaryFile(prefix="astrbot_in_", delete=False)
            blob = _IncomingBlob(
                blob_id=str(payload.get("blob_id") or uuid.uuid4().hex),
                filename=_safe_filename(payload.get("filename")),
                mime_type=str(payload.get("mime_type") or "application/octet-stream"),
                kind=str(payload.get("kind") or "document"),
                expected_size=expected_size,
                expected_sha256=str(payload.get("sha256") or ""),
                path=Path(temp.name),
                handle=temp,
            )
            turn.current_blob = blob
            turn.blobs[blob.blob_id] = blob
            return turn
        if frame_type == "blob.end":
            if turn is None or turn.current_blob is None:
                raise ValueError("blob_not_open")
            blob = turn.current_blob
            blob.handle.close()
            if blob.size != blob.expected_size:
                raise ValueError("attachment_size_mismatch")
            if blob.expected_sha256 and blob.digest.hexdigest() != blob.expected_sha256:
                raise ValueError("attachment_checksum_mismatch")
            turn.current_blob = None
            return turn
        if frame_type == "turn.commit":
            if turn is None or turn.current_blob is not None:
                raise ValueError("turn_not_ready")
            task = asyncio.create_task(self._dispatch_turn(turn))
            self._turn_tasks.add(task)
            task.add_done_callback(self._turn_tasks.discard)
            await ws.send_json({"type": "turn.accepted", "turn_id": turn.turn_id})
            return None
        return turn

    async def _dispatch_turn(self, turn: _IncomingTurn) -> None:
        chat_id = str(turn.payload.get("chat_id") or "")
        try:
            media_urls: list[str] = []
            media_types: list[str] = []
            for meta in turn.payload.get("attachments") or []:
                blob = turn.blobs.get(str(meta.get("blob_id") or ""))
                if blob is None:
                    continue
                data = blob.path.read_bytes()
                cached = cache_media_bytes(
                    data,
                    filename=blob.filename,
                    mime_type=blob.mime_type,
                    default_kind=blob.kind if blob.kind in {"image", "video", "audio", "document"} else None,
                )
                if cached is not None:
                    media_urls.append(cached.path)
                    media_types.append(cached.media_type)

            chat_type = str(turn.payload.get("chat_type") or "dm")
            user_id = str(turn.payload.get("user_id") or "unknown")
            user_name = str(turn.payload.get("user_name") or user_id)
            source = self.build_source(
                chat_id=chat_id,
                chat_name=str(turn.payload.get("chat_name") or chat_id),
                chat_type=chat_type,
                user_id=user_id,
                user_name=user_name,
            )
            reply = turn.payload.get("reply") or {}
            event = MessageEvent(
                text=str(turn.payload.get("text") or ""),
                message_type=MessageType.TEXT,
                user_id=user_id,
                user_name=user_name,
                source=source,
                message_id=str(turn.payload.get("message_id") or turn.turn_id),
                timestamp=datetime.now(),
                media_urls=media_urls,
                media_types=media_types,
                reply_to_message_id=str(reply.get("message_id") or "") or None,
                reply_to_text=str(reply.get("text") or "") or None,
                reply_to_author_id=str(reply.get("author_id") or "") or None,
                reply_to_author_name=str(reply.get("author_name") or "") or None,
            )
            await self.handle_message(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("AstrBot inbound turn failed: %s", turn.turn_id)
            if chat_id:
                await self._send_turn_finished(turn.turn_id, chat_id, status="error", error=str(exc))
        finally:
            self._cleanup_turn(turn)

    async def _send_turn_finished(
        self,
        turn_id: str,
        chat_id: str,
        *,
        status: str = "completed",
        error: Optional[str] = None,
    ) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            return
        try:
            payload = {
                "type": "turn.finish",
                "turn_id": turn_id,
                "chat_id": chat_id,
                "status": status,
            }
            if error:
                payload["error"] = error
            await ws.send_json(payload)
        except Exception:
            logger.debug("AstrBot turn.finish delivery failed", exc_info=True)

    def _cleanup_turn(self, turn: Optional[_IncomingTurn]) -> None:
        if turn is None:
            return
        for blob in turn.blobs.values():
            try:
                if not blob.handle.closed:
                    blob.handle.close()
            except Exception:
                pass
            try:
                blob.path.unlink(missing_ok=True)
            except OSError:
                pass

    async def send(self, chat_id: str, content: str, reply_to=None, metadata=None) -> SendResult:
        return await self._send_delivery("text", chat_id, content=content)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        ws = self._ws
        if ws is not None and not ws.closed:
            try:
                await ws.send_json({"type": "activity", "chat_id": chat_id, "activity": "working"})
            except Exception:
                logger.debug("AstrBot activity delivery failed", exc_info=True)

    async def send_image(self, chat_id: str, image_url: str, caption=None, reply_to=None, metadata=None, **kwargs) -> SendResult:
        return await self._send_delivery("image", chat_id, content=caption or "", source_url=image_url)

    async def send_image_file(self, chat_id: str, image_path: str, caption=None, reply_to=None, metadata=None, **kwargs) -> SendResult:
        return await self._send_delivery("image", chat_id, content=caption or "", file_path=image_path)

    async def send_document(self, chat_id: str, file_path: str, caption=None, file_name=None, reply_to=None, metadata=None, **kwargs) -> SendResult:
        return await self._send_delivery("document", chat_id, content=caption or "", file_path=file_path, file_name=file_name)

    async def send_voice(self, chat_id: str, audio_path: str, caption=None, reply_to=None, metadata=None, **kwargs) -> SendResult:
        return await self._send_delivery("audio", chat_id, content=caption or "", file_path=audio_path)

    async def send_video(self, chat_id: str, video_path: str, caption=None, reply_to=None, metadata=None, **kwargs) -> SendResult:
        return await self._send_delivery("video", chat_id, content=caption or "", file_path=video_path)

    async def _send_delivery(
        self,
        kind: str,
        chat_id: str,
        *,
        content: str = "",
        file_path: Optional[str] = None,
        file_name: Optional[str] = None,
        source_url: Optional[str] = None,
    ) -> SendResult:
        ws = self._ws
        if ws is None or ws.closed:
            return SendResult(success=False, error="AstrBot connector is not connected", retryable=True)
        delivery_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self._pending_acks[delivery_id] = future
        try:
            async with self._send_lock:
                if file_path:
                    path = Path(file_path)
                    if not path.is_file():
                        return SendResult(success=False, error="Attachment is not readable")
                    size = path.stat().st_size
                    if size > self.max_file_bytes:
                        return SendResult(success=False, error="Attachment exceeds configured size limit")
                    sha256 = _sha256(path)
                    await ws.send_json({
                        "type": "delivery.start",
                        "delivery_id": delivery_id,
                        "chat_id": chat_id,
                        "kind": kind,
                        "caption": content,
                        "filename": _safe_filename(file_name or path.name),
                        "mime_type": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                        "size": size,
                        "sha256": sha256,
                    })
                    with path.open("rb") as handle:
                        while chunk := handle.read(CHUNK_SIZE):
                            await ws.send_bytes(chunk)
                    await ws.send_json({"type": "delivery.end", "delivery_id": delivery_id})
                elif source_url:
                    await ws.send_json({
                        "type": "delivery.remote",
                        "delivery_id": delivery_id,
                        "chat_id": chat_id,
                        "kind": kind,
                        "caption": content,
                        "source_url": source_url,
                    })
                else:
                    await ws.send_json({
                        "type": "delivery.text",
                        "delivery_id": delivery_id,
                        "chat_id": chat_id,
                        "text": content,
                    })
            ack = await asyncio.wait_for(future, timeout=self.delivery_timeout)
            if ack.get("success"):
                return SendResult(success=True, message_id=str(ack.get("message_id") or delivery_id))
            return SendResult(success=False, error=str(ack.get("error") or "AstrBot delivery failed"))
        except asyncio.TimeoutError:
            return SendResult(success=False, error="AstrBot delivery acknowledgement timed out", retryable=True)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)
        finally:
            self._pending_acks.pop(delivery_id, None)

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        is_group = ":group:" in chat_id
        return {"name": chat_id, "type": "group" if is_group else "dm", "chat_id": chat_id}


def _safe_filename(value: Any) -> str:
    name = Path(str(value or "file").replace("\\", "/")).name.replace("\x00", "").strip()
    return name if name not in {"", ".", ".."} else "file"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def check_requirements() -> bool:
    return bool(_secret("ASTRBOT_BRIDGE_TOKEN") or _secret("API_SERVER_KEY"))


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(_secret("ASTRBOT_BRIDGE_TOKEN") or extra.get("token") or _secret("API_SERVER_KEY"))


def _env_enablement() -> Optional[dict[str, Any]]:
    if not check_requirements():
        return None
    return {
        "host": os.getenv("ASTRBOT_BRIDGE_HOST", "0.0.0.0"),
        "port": int(os.getenv("ASTRBOT_BRIDGE_PORT", "8643")),
    }


def register(ctx):
    ctx.register_platform(
        name="astrbot",
        label="AstrBot",
        adapter_factory=lambda cfg: AstrBotAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=[],
        install_hint="Set ASTRBOT_BRIDGE_TOKEN (or API_SERVER_KEY) in the selected profile",
        env_enablement_fn=_env_enablement,
        max_message_length=0,
        emoji="AB",
        pii_safe=True,
        allow_update_command=False,
        platform_hint=(
            "You are chatting through AstrBot on QQ. Files and media can be delivered "
            "natively. To return a local file, use Hermes' normal MEDIA path convention."
        ),
    )
