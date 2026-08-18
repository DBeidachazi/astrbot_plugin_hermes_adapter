# Hermes Gateway Universal

给 AstrBot 使用 Hermes 网关的简易桥接插件。插件只处理 `/h` 命令，普通消息不会被拦截。

## 使用

1. 在 AstrBot 插件配置中填写管理员 QQ。
2. `hermes_profile` 填 Hermes 已存在的 profile 名称。
3. 填网关地址和访问密钥。
4. 管理员发送：

```text
/h 你好，请介绍一下你自己
```

## 配置

| 配置 | 说明 |
| --- | --- |
| `hermes_profile` | Hermes 使用的 profile 名称 |
| `hermes_gateway_url` | Hermes 网关地址，默认 `http://host.docker.internal:8642` |
| `hermes_gateway_auth_token` | 网关访问密钥，建议直接填写；留空时读取 AstrBot 进程环境变量 `HERMES_GATEWAY_AUTH_TOKEN` 或 `API_SERVER_KEY` |
| `timeout` | 单次读取空闲超时，建议 `300` 秒 |
| `admin_qq_ids` | 可以使用 `/h` 的 QQ 列表 |
| `admin_qq_id` | 单个管理员 QQ，兼容旧配置 |

`profile` 是 Hermes 侧的配置选择，不是 Agent ID。插件不会要求填写 Agent ID；对话身份由 QQ 会话自动生成。

## 会话隔离

- 同一个群使用同一个会话。
- 不同群互相隔离。
- 每个私聊用户独立会话。
- 会话标识由平台、用户和群号自动生成。

## 超时说明

`timeout` 是流式响应两次数据之间允许的最大空闲时间，不是整个任务的总时长。Hermes 发送 working 消息会刷新计时，因此长任务不会因为总耗时超过 300 秒而被插件主动终止。

## 常见问题

### 插件加载失败

确认 AstrBot 已更新到 GitHub 仓库的最新版本，并删除旧插件目录后重新安装。

### 401 认证失败

检查 `hermes_gateway_auth_token` 是否与 Hermes 的网关访问密钥一致。上游模型密钥不应填写在这里。

### 无法连接网关

确认 AstrBot 容器可以访问 `hermes_gateway_url`，并确认 Hermes 容器正在运行。
