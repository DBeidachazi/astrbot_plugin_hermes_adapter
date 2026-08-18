"""AstrBot connector for the profile-local Hermes platform adapter."""

from __future__ import annotations

import mimetypes
from pathlib import Path
import sys
from typing import Any, Optional
import uuid

from astrbot.api import logger
from astrbot.api.all import EventMessageType
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import File, Image, Plain, Record, Reply, Video
from astrbot.api.star import Context, Star, register
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType

from .astrbot_connector import AttachmentSource, AstrBotConnector, DeliveryRoute
from .bridge_protocol import build_chat_id, safe_filename


PLUGIN_ID = "gateway_universal"


@register(
    PLUGIN_ID,
    "a4869",
    "通过 /h 将 QQ 会话接入 Hermes 原生平台适配器",
    "2.0.0",
)
class GatewayUniversalBridge(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context, config)
        self.config = config or {}
        profile = str(self._cfg("hermes_profile", "default") or "default").strip()
        gateway_url = str(
            self._cfg("hermes_gateway_url", "http://host.docker.internal:8643")
            or "http://host.docker.internal:8643"
        ).strip()
        token = str(self._cfg("hermes_gateway_auth_token", "") or "").strip()
        self._admin_ids = self._load_admin_ids()
        self.connector = AstrBotConnector(
            gateway_url=gateway_url,
            profile=profile,
            token=token,
            delivery_handler=self._deliver_from_hermes,
        )
        logger.info(
            "[gateway_universal] 初始化完成 - profile=%s, adapter=%s",
            profile,
            self.connector.ws_url,
        )

    async def initialize(self) -> None:
        await self.connector.start()

    def _cfg(self, key: str, default: Any) -> Any:
        if isinstance(self.config, dict):
            value = self.config.get(key, default)
        else:
            value = getattr(self.config, key, default)
        if isinstance(value, dict) and "value" in value:
            return value["value"]
        return value

    def _load_admin_ids(self) -> set[str]:
        values = self._cfg("admin_qq_ids", [])
        if not isinstance(values, list):
            values = []
        result = {str(value).strip() for value in values if str(value).strip()}
        legacy = str(self._cfg("admin_qq_id", "") or "").strip()
        if legacy:
            result.add(legacy)
        return result

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        sender_id = str(event.get_sender_id())
        if self._admin_ids:
            return sender_id in self._admin_ids
        global_admins = self.context.get_config().get("admins_id", [])
        return sender_id in {str(value) for value in global_admins} or "astrbot" in global_admins

    @staticmethod
    def _command_text(event: AstrMessageEvent) -> Optional[str]:
        raw = str(getattr(event, "message_str", "") or "").strip()
        command = "/h" if raw.startswith("/h") else "h" if raw.startswith("h") else ""
        if not command:
            return None
        if len(raw) > len(command) and not raw[len(command)].isspace():
            return None
        return raw[len(command) :].strip()

    @staticmethod
    def _stop_event(event: AstrMessageEvent) -> None:
        event.stop_event()
        event.should_call_llm(True)
        event.set_extra("skip_llm_hooks", True)
        event._has_send_oper = True

    @filter.event_message_type(EventMessageType.ALL, priority=sys.maxsize)
    async def handle_message(self, event: AstrMessageEvent, *args, **kwargs):
        text = self._command_text(event)
        if text is None or not self._is_admin(event):
            return
        self._stop_event(event)

        attachments, reply = await self._collect_attachments(event)
        if not text and not attachments:
            yield event.plain_result("用法：/h <消息>，也可以附带或引用图片、文件、语音、视频。")
            return

        sender_id = str(event.get_sender_id())
        group_id = str(event.get_group_id() or "")
        platform_name = str(event.get_platform_name())
        chat_id = build_chat_id(platform_name, sender_id, group_id)
        route = DeliveryRoute(
            platform_id=str(event.get_platform_id()),
            message_type=event.get_message_type(),
            session_id=group_id or sender_id,
        )
        payload = {
            "turn_id": uuid.uuid4().hex,
            "chat_id": chat_id,
            "chat_type": "group" if group_id else "dm",
            "chat_name": group_id or sender_id,
            "user_id": sender_id,
            "user_name": str(event.get_sender_name() or sender_id),
            "message_id": str(getattr(event.message_obj, "message_id", "") or ""),
            "text": text,
            "reply": reply,
        }
        try:
            await self.connector.send_turn(payload, attachments, route)
        except Exception as exc:
            logger.error("[gateway_universal] 无法提交消息到 Hermes: %s", exc)
            yield event.plain_result(f"无法连接 Hermes 平台适配器：{exc}")

    async def _collect_attachments(
        self, event: AstrMessageEvent
    ) -> tuple[list[AttachmentSource], dict[str, str]]:
        attachments: list[AttachmentSource] = []
        reply: dict[str, str] = {}
        components = getattr(getattr(event, "message_obj", None), "message", None) or []
        for component in components:
            if isinstance(component, Reply):
                reply = {
                    "message_id": str(getattr(component, "id", "") or ""),
                    "text": str(getattr(component, "message_str", "") or ""),
                    "author_id": str(getattr(component, "sender_id", "") or ""),
                    "author_name": str(getattr(component, "sender_nickname", "") or ""),
                }
                for nested in getattr(component, "chain", None) or []:
                    source = await self._component_attachment(nested)
                    if source is not None:
                        attachments.append(source)
                continue
            source = await self._component_attachment(component)
            if source is not None:
                attachments.append(source)
        return attachments, reply

    async def _component_attachment(self, component: Any) -> Optional[AttachmentSource]:
        kind = ""
        path = ""
        if isinstance(component, Image):
            kind = "image"
            path = await component.convert_to_file_path()
        elif isinstance(component, File):
            kind = "document"
            path = await component.get_file()
        elif isinstance(component, Video):
            kind = "video"
            path = await component.convert_to_file_path()
        elif isinstance(component, Record):
            kind = "audio"
            resolver = getattr(component, "convert_to_file_path", None)
            if resolver is not None:
                path = await resolver()
        if not kind or not path or not Path(path).is_file():
            return None
        name = safe_filename(getattr(component, "name", "") or Path(path).name)
        return AttachmentSource(
            path=str(path),
            filename=name,
            mime_type=mimetypes.guess_type(name)[0] or "application/octet-stream",
            kind=kind,
        )

    async def _deliver_from_hermes(
        self,
        payload: dict[str, Any],
        path: Optional[str],
        route: DeliveryRoute,
    ) -> str | None:
        session = MessageSession(
            platform_name=route.platform_id,
            message_type=route.message_type,
            session_id=route.session_id,
        )
        frame_type = payload.get("type")
        if frame_type == "delivery.text":
            text = str(payload.get("text") or "")
            if not text:
                return None
            await self.context.send_message(session=session, message_chain=MessageChain([Plain(text)]))
            return str(uuid.uuid4())

        caption = str(payload.get("caption") or "").strip()
        if caption:
            await self.context.send_message(session=session, message_chain=MessageChain([Plain(caption)]))

        kind = str(payload.get("kind") or "document")
        if frame_type == "delivery.remote":
            source_url = str(payload.get("source_url") or "")
            if kind != "image" or not source_url:
                raise ValueError("unsupported remote attachment")
            component = Image.fromURL(source_url)
        elif path:
            filename = safe_filename(payload.get("filename") or Path(path).name)
            if kind == "image":
                component = Image.fromFileSystem(path)
            elif kind == "audio":
                component = Record(file=path)
            elif kind == "video":
                component = Video(file=path)
            else:
                component = File(name=filename, file=path)
        else:
            raise ValueError("attachment payload is missing data")

        await self.context.send_message(session=session, message_chain=MessageChain([component]))
        return str(uuid.uuid4())

    async def terminate(self) -> None:
        await self.connector.close()
