#!/usr/bin/env python3
"""
Hermes 网关桥接插件；
内置 ``_bridge_runtime``（会话工具）与 ``_gateway_lib``（L1 合并、``/v1/responses`` 客户端），
不依赖其他桥接插件目录。
L1 统一配置见 ``data/config/gateway_bridges.json``，``active_profile_by_plugin`` 建议使用键 ``gateway_universal``。
L1 统一配置见 ``data/config/gateway_bridges.json``。
"""

from __future__ import annotations

import importlib.util
import inspect
import json
import os
from pathlib import Path
import sys
from typing import Any

import astrbot.api.star as _astrbot_star
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, register
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType
from astrbot.core.star.star_handler import star_handlers_registry
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from data.plugins.astrbot_plugin_gateway_universal._gateway_lib import (
    ResponsesGatewayClient as _ResponsesGatewayClient,
    merge_gateway_l1_into_l2,
)

GATEWAY_UNIVERSAL_ID = "gateway_universal"


def _noop_plugin_register(*_args, **_kwargs):
    def _decorator(cls):
        return cls

    return _decorator


try:
    _bridge_dir = Path(__file__).resolve().parent / "_bridge_runtime"
    _bridge_file = _bridge_dir / "main.py"
    _pkg_name = "_astrbot_gateway_universal_bridge_runtime"
    _spec = importlib.util.spec_from_file_location(
        _pkg_name,
        _bridge_file,
        submodule_search_locations=[str(_bridge_dir)],
    )
    if _spec is None or _spec.loader is None:
        raise RuntimeError(f"Invalid spec for bridge: {_bridge_file}")
    _bridge_mod = importlib.util.module_from_spec(_spec)
    sys.modules[_pkg_name] = _bridge_mod
    _saved_register = _astrbot_star.register
    _astrbot_star.register = _noop_plugin_register
    try:
        _spec.loader.exec_module(_bridge_mod)
    finally:
        _astrbot_star.register = _saved_register
    _BaseBridge = _bridge_mod.ClawdbotBridge
    _Filter = _bridge_mod.filter
    _EventMessageType = _bridge_mod.EventMessageType
    _MAX_PRIORITY = _bridge_mod.sys.maxsize
    logger.info("[gateway_universal] 已加载内置桥接运行时: %s", _bridge_dir)
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "gateway_universal: failed to load _bridge_runtime (copy from clawdbot bundle)."
    ) from e

def _unwrap(value: Any) -> Any:
    if isinstance(value, dict) and "value" in value:
        return value["value"]
    return value


def _unified_gateway_bridges_path(cfg: dict[str, Any]) -> Path:
    custom = _unwrap(cfg.get("unified_gateway_config_path"))
    if isinstance(custom, str) and custom.strip():
        return Path(custom.strip())
    return Path(get_astrbot_data_path()) / "config" / "gateway_bridges.json"


def _set_cfg(cfg: dict[str, Any], key: str, value: Any) -> None:
    if key in cfg and isinstance(cfg[key], dict) and "value" in cfg[key]:
        cfg[key]["value"] = value
    else:
        cfg[key] = value


def _disable_conflicting_gateway_handlers(this_module: str) -> None:
    """关闭其他桥接插件及重复 runtime 的 handler，仅保留本插件。"""
    for handler in list(star_handlers_registry):
        if handler.handler_name not in {"handle_message", "on_study_group_message"}:
            continue
        if handler.handler_module_path == this_module:
            continue
        mod_path = getattr(handler, "handler_module_path", "") or ""
        qualname = getattr(handler.handler, "__qualname__", "")
        mp = mod_path.replace("\\", "/")
        if "_astrbot_plugin_clawdbot_bridge_runtime" in mod_path:
            handler.enabled = False
            continue
        if "_astrbot_gateway_universal_bridge_runtime" in mod_path:
            handler.enabled = False
            continue
        if qualname.startswith("ClawdbotBridge."):
            handler.enabled = False
            continue
        if "astrbot_plugin_hermes_bridge" in mp:
            handler.enabled = False
            continue
        if "astrbot_plugin_clawdbot_bridge" in mp:
            handler.enabled = False
            continue
        if "astrbot_plugin_gateway_universal" in mp and this_module not in mp:
            handler.enabled = False


@register(
    GATEWAY_UNIVERSAL_ID,
    "a4869",
    "Hermes 网关桥接，支持 profile 与按用户隔离会话",
    "1.0.0",
)
class GatewayUniversalBridge(_BaseBridge):
    """仅通过 /h 命令调用 Hermes，不在插件加载阶段探测网关。"""

    def __init__(self, context: Context, config: dict | None = None):
        cfg: dict[str, Any] = {str(k): _unwrap(v) for k, v in dict(config or {}).items()}
        self._gateway_backend = "hermes"

        profiles = _unwrap(cfg.get("profiles"))
        if isinstance(profiles, str):
            try:
                profiles = json.loads(profiles)
            except (TypeError, ValueError):
                profiles = None
        if isinstance(profiles, list):
            profiles = {
                str(item.get("name")): item
                for item in profiles
                if isinstance(item, dict) and str(item.get("name", "")).strip()
            }
        active_profile = str(_unwrap(cfg.get("active_profile")) or "default").strip()
        if isinstance(profiles, dict):
            selected = profiles.get(active_profile) or profiles.get("default")
            if isinstance(selected, dict):
                cfg.update({str(k): _unwrap(v) for k, v in selected.items()})
        cfg.pop("gateway_backend", None)

        if not cfg.get("_gateway_l1_merge_applied"):
            cfg = merge_gateway_l1_into_l2(
                cfg,
                unified_file=_unified_gateway_bridges_path(cfg),
                registry_plugin_id=GATEWAY_UNIVERSAL_ID,
                mapping_plugin_id="hermes_bridge",
            )
            cfg["_gateway_l1_merge_applied"] = True

        hermes_gateway_url = _unwrap(cfg.get("hermes_gateway_url")) or "http://host.docker.internal:8642"
        hermes_agent_id = _unwrap(cfg.get("hermes_agent_id")) or "default"
        hermes_gateway_auth_token = str(_unwrap(cfg.get("hermes_gateway_auth_token")) or "").strip()
        if not hermes_gateway_auth_token:
            hermes_gateway_auth_token = os.environ.get("HERMES_GATEWAY_AUTH_TOKEN", "").strip() or os.environ.get("API_SERVER_KEY", "").strip()
        _set_cfg(cfg, "clawdbot_gateway_url", str(hermes_gateway_url))
        _set_cfg(cfg, "clawdbot_agent_id", str(hermes_agent_id))
        _set_cfg(cfg, "gateway_auth_token", hermes_gateway_auth_token)
        _set_cfg(cfg, "gateway_model_template", "hermes:{agent_id}")
        _set_cfg(cfg, "gateway_send_openclaw_headers", True)

        super().__init__(context, cfg)

        if _ResponsesGatewayClient is not None:
            _tok = str(
                _unwrap(cfg.get("hermes_gateway_auth_token"))
                or _unwrap(cfg.get("gateway_auth_token"))
                or ""
            ).strip()
            if not _tok:
                _tok = (
                    os.environ.get("HERMES_GATEWAY_AUTH_TOKEN", "").strip()
                    or os.environ.get("API_SERVER_KEY", "").strip()
                )
            self.gateway_auth_token = _tok
            _mt = self._get_config("gateway_model_template", "hermes:{agent_id}")
            self.client = _ResponsesGatewayClient(
                gateway_url=self.gateway_url,
                agent_id=self.agent_id,
                auth_token=_tok,
                timeout=int(self.timeout),
                model_template=str(_mt or "hermes:{agent_id}"),
                send_gateway_headers=True,
                log_prefix="[gateway_universal]",
            )

            admin_id_cfg = _unwrap(cfg.get("admin_qq_id"))
            admin_ids_cfg = _unwrap(cfg.get("admin_qq_ids"))
            if isinstance(admin_ids_cfg, str):
                try:
                    import json as _json

                    admin_ids_cfg = _json.loads(admin_ids_cfg)
                except Exception:
                    admin_ids_cfg = []
            if not isinstance(admin_ids_cfg, list):
                admin_ids_cfg = []
            if admin_id_cfg and str(admin_id_cfg) not in [str(x) for x in admin_ids_cfg]:
                admin_ids_cfg.append(str(admin_id_cfg))
            self._forced_admin_ids = [str(x) for x in admin_ids_cfg if str(x).strip()]
            if self._forced_admin_ids:
                self.admin_qq_ids = list(self._forced_admin_ids)
                self.admin_qq_id = self._forced_admin_ids[0]

            self.command_handler = _bridge_mod.CommandHandler(
                switch_commands=["/h"],
                exit_commands=[],
            )

        _disable_conflicting_gateway_handlers(__name__)

    @property
    def _user_brand_display(self) -> str:
        custom = _unwrap(getattr(self, "config", None) or {})
        if isinstance(self.config, dict):
            custom = _unwrap(self.config.get("user_brand_display"))
        else:
            custom = _unwrap(getattr(self.config, "user_brand_display", ""))
        if isinstance(custom, str) and custom.strip():
            return custom.strip()
        return "Hermes" if self._gateway_backend == "hermes" else "OpenClaw"

    _AUTH_401_HINT = (
        "\n\n提示：网关已校验 API 密钥。请在插件配置填写 token（与网关 API_SERVER_KEY 一致），"
        "或设置环境变量 HERMES_GATEWAY_AUTH_TOKEN / API_SERVER_KEY。"
    )

    def _brand_user_facing_text(self, text: str) -> str:
        if self._gateway_backend != "hermes":
            return text
        if not text:
            return text
        out = text.replace("OpenClaw", self._user_brand_display)
        if "invalid_api_key" in out and "hermes_gateway_auth_token" not in out:
            out = out + self._AUTH_401_HINT
        return out

    def _brand_message_result(self, result: Any) -> Any:
        if self._gateway_backend != "hermes":
            return result
        if result is None:
            return result
        chain = getattr(result, "chain", None)
        if not chain:
            return result
        for comp in chain:
            raw = getattr(comp, "text", None)
            if isinstance(raw, str) and raw:
                comp.text = self._brand_user_facing_text(raw)
        return result

    async def _send_response(
        self, event: AstrMessageEvent, response_text: str, is_study_group: bool
    ):
        if self._gateway_backend != "hermes":
            result = _BaseBridge._send_response(self, event, response_text, is_study_group)
            if inspect.isasyncgen(result):
                async for r in result:
                    yield r
            return
        response_text = self._brand_user_facing_text(response_text)
        if is_study_group and self.admin_qq_id:
            logger.info(
                "[gateway_universal] 学习群响应，私信管理员 %s",
                self.admin_qq_id,
            )
            group_id = str(event.group_id) if hasattr(event, "group_id") else "未知"
            sender_id = event.get_sender_id()
            message = event.message_str.strip()
            admin_message = (
                f"[学习群 {self._user_brand_display}]\n群号: {group_id}\n发送者: {sender_id}\n"
                f"原消息: {message[:100]}\n\n{response_text}"
            )
            try:
                session = MessageSession(
                    platform_name=event.get_platform_id(),
                    message_type=MessageType.FRIEND_MESSAGE,
                    session_id=self.admin_qq_id,
                )
                await self.context.send_message(
                    session=session,
                    message_chain=MessageChain([Plain(admin_message)]),
                )
            except Exception as e:
                logger.error("[gateway_universal] 发送私信失败: %s", e)
        else:
            result = event.plain_result(response_text)
            event.set_result(result)
            yield result

    def _is_admin(self, event) -> bool:
        sender_id = str(event.get_sender_id())
        if getattr(self, "_forced_admin_ids", None):
            return sender_id in self._forced_admin_ids
        return super()._is_admin(event)

    @staticmethod
    def _extract_h_message(event) -> str | None:
        raw = str(getattr(event, "message_str", "") or "").strip()
        if not raw.startswith("/h"):
            return None
        if len(raw) > 2 and not raw[2].isspace():
            return None
        text = raw[2:].strip()
        parts = []
        components = getattr(getattr(event, "message_obj", None), "message", None) or []
        for component in components:
            for attr in ("url", "file", "path"):
                value = getattr(component, attr, None)
                if value:
                    parts.append(str(value))
                    break
        if parts:
            text = f"{text}\n" if text else ""
            text += "\n".join(f"[media] {item}" for item in parts)
        return text or None

    @_Filter.event_message_type(_EventMessageType.ALL, priority=_MAX_PRIORITY)
    async def handle_message(self, event, *args, **kwargs):
        message = self._extract_h_message(event)
        if message is None:
            return
        if not self._is_admin(event):
            return
        self._stop_event(event)
        session_key = self.session_manager.get_gateway_session_key(
            event, self.default_session
        )
        logger.info("[gateway_universal] /h -> session=%s", session_key)
        response = await self.client.send_message(message, session_key)
        async for result in self._send_response(
            event, response or "✅ Hermes 已处理，但未返回文本。", False
        ):
            yield self._brand_message_result(result)
