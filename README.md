# Hermes AstrBot 平台适配器

这个项目包含两部分：

- AstrBot 插件：接收 `/h`，读取 QQ 消息及引用消息。
- Hermes 平台插件：把 AstrBot 注册为 Hermes 原生消息平台。

文本、图片、文件、语音和视频通过适配器协议传输，不需要共享 Docker 目录，也不使用 `/v1/responses`。

## 安装 Hermes 端

把 `hermes_platform` 目录复制到所用 Hermes profile 的插件目录，目录名改为 `astrbot`：

```text
$HERMES_HOME/plugins/astrbot/
  __init__.py
  adapter.py
  plugin.yaml
```

为该 profile 设置密钥和监听端口：

```text
ASTRBOT_BRIDGE_TOKEN=填写一个随机密钥
ASTRBOT_BRIDGE_HOST=0.0.0.0
ASTRBOT_BRIDGE_PORT=8643
```

启用插件并重启 Gateway：

```bash
hermes -p PROFILE plugins enable astrbot-platform
hermes -p PROFILE gateway restart
```

Hermes 日志出现 `AstrBot adapter listening` 即表示平台端已启动。确保 AstrBot 容器能够访问该端口。

## 配置 AstrBot

| 配置 | 填写内容 |
| --- | --- |
| `hermes_profile` | Hermes profile 名称 |
| `hermes_gateway_url` | 适配器地址，例如 `http://hermes:8643` |
| `hermes_gateway_auth_token` | 与 `ASTRBOT_BRIDGE_TOKEN` 相同的密钥 |
| `timeout` | 无 working、typing 或响应时的空闲超时 |
| `admin_qq_ids` | 可以使用 `/h` 的 QQ 列表 |
| `admin_qq_id` | 旧版单管理员配置，保留兼容 |

插件不会读取隐藏环境变量，也没有 Agent ID、backend 或 JSON profile 配置。

## 使用

```text
/h 你好
```

可以在同一条消息附带图片、文件、语音或视频，也可以引用含有附件的 QQ 消息再发送 `/h`。

私聊按 QQ 用户隔离；群聊按群号隔离，同一个群共用一个 Hermes 会话。`working` 活动会刷新 `timeout`，不会因任务总耗时超过该值而中断。
