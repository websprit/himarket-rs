# HiMarket-RS (Rust Low-Memory Edition)

`himarket-rs` 是基于 `Axum` + `Tokio` + `SQLx` 构建的超低内存（常驻 `< 25 MB`）企业级 AI 开放平台与实时语音会话控制平面，完整实现：

1. **`crates/himarket-market`（HiMarket 扩展）**：多租户（Portal / Developer / Consumer / Product）、业务 API Key 凭证配额管理与按租户/Key 聚合账单查询。
2. **`crates/himarket-access`（业务接入服务）**：验证业务 API Key、登记可信语音会话、签发短期 LiveKit JWT（锁定 `canUpdateOwnMetadata = false` 防止客户端篡改租户身份）并调用 LiveKit `AgentDispatchService` 调度语音 AI Agent。
3. **`crates/himarket-metering`（统一用量服务）**：汇总 **LiveKit SFU（RTC 媒体时长与流量）**、**Agent 会话任务（STT / TTS / Realtime 语音用量）** 与 **CLIProxyAPI（文本 LLM Token 用量）**，实现幂等去重、身份自动补齐与小时级预聚合。

## 控制台

服务启动后：

- 首页：`/`
- 管理后台：`/admin`，默认账号 `admin` / `admin123`
- 开发者门户：`/portal`，默认账号 `ugreen_voice_dev` / `voice123`

管理后台可以管理门户、产品、调用方和 API Key，并查看语音会话与用量。开发者门户可以浏览产品、创建应用、签发 API Key，并查看自己的语音会话和用量。

## 快速运行与测试

```bash
cargo test
cargo run --release -p himarket-bootstrap
```
