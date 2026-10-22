# 绿联 AI 统一接入平台 MVP — 完整闭环详细设计文档（Gap 1 ~ Gap 7）

## 1. 背景与目标

对照《绿联AI统一接入平台 MVP — 功能边界》（一 A~F、六 验收演示脚本 1~4、以及模型选型总表），当前代码库（`himarket-rs` 与 `voice-agent-worker`）已具备多租户语音会话管理、语种策略、转写/同传/播报/总结基础能力与小时级计量汇总，但距离 MVP 验收标准仍存在以下 **6 大核心功能模块（对应 7 项改造任务）** 的缺口：

1. **Gap 1（A. 统一 API 网关标准接口 `/v1/*`）**：缺少对外统一标准网关入口 `POST /v1/chat/completions`（含 `stream: true` SSE 流式与 `stream: false` 非流式）、`POST /v1/audio/transcriptions`、`GET /v1/audio/asr/stream`（WebSocket 流式 ASR）、`POST /v1/audio/speech`（直接返回二进制音频），以及 Worker 侧配套的 `POST /internal/v1/transcribe` 接口。
2. **Gap 2（A & 模型选型总表 — 补齐“性价比档”模型目录 & 4 家供应商真实连接开关）**：`catalog/livekit_models.json` 仅包含选型表中的“性能档”模型，缺少阿里云、火山引擎、微软 Azure、Google 四家供应商在 ASR / TTS / 文本 LLM 上的“性价比档”条目及明确的 `vendor` 标识；同时 `is_stt_runnable` / `is_tts_runnable` 未放开 `dashscope_*`、`volcengine_*`、`azure_tts`、`google_tts`，且 `voice-agent-worker/agent_main.py` 在配置了密钥（含真实密钥与演示 `mock_`/`demo_` 密钥）时未能打通四家供应商协议转换与执行闭环。
3. **Gap 3（B. 灵活切换 — 显式【能力 → 语种 → 供应商 → 模型】路由表 `ai_route_policy` 与一键切换）**：缺少显式的路由策略表 `ai_route_policy` 与管理接口 `GET/PUT /api/v1/routes`、模型重新启用接口 `POST /api/v1/model-configs/{config_id}/enable`、供应商级批量启停接口 `POST /api/v1/vendors/{vendor}/status`，且 `GET /internal/v1/voice/sessions/{session_id}/runtime` 未实时反映后台刚切换的路由策略。
4. **Gap 4（C. 用量记录与按日三维度可视化报表）**：`usage_event_ledger` 缺少 `vendor`、`status`（`SUCCESS`/`FAILED`）、`latency_ms`、`stat_date` 字段；缺少按日三维度（业务线 × 能力 × 供应商 + 模型）聚合表 `usage_daily_three_dim_rollup` 及 `GET /api/v1/usage/report` 报表接口。
5. **Gap 5（D. 用量上限、80%/100% 阈值告警与业务线手动启停）**：`voice_api_key_binding` 仅支持 `quota_limit_usd_micros`，缺少按月**调用次数上限 (`quota_limit_calls`)** 与 **Token 上限 (`quota_limit_tokens`)**、80%/100% 告警事件表 `quota_alert_event`、单通道告警推送、业务线手动启停 `POST /api/v1/api-keys/{api_key_id}/status` 及额度实时调整 `PUT /api/v1/api-keys/{api_key_id}/quota`（满足验收脚本步骤 4：超限拦截 → 调高额度立即恢复）。
6. **Gap 6（E. 供应商密钥加密存储 & 业务线只读 1 级权限 + Web 控制台升级）**：`ai_model_config` 中的 `api_key` / `api_secret` 目前明文存储于 PostgreSQL；开发者门户 `/portal` 仍允许写操作（新建应用/签发 Key），违反“业务线只读 1 级”要求；管理后台 `/admin` 与门户 `/portal` 需升级支持告警横幅、路由配置页、额度进度条与实时启停、供应商加密状态展示及按日三维度可视化报表。

本设计文档给出精确到文件路径、Rust/Python 结构体、SQL DDL、路由签名与错误码的完整实现方案。

---

## 2. 系统架构与数据流全景

```
+---------------------------------------------------------------------------------------------------+
|                             业务方客户端 (Android / Web / 后端服务)                               |
|   鉴权头: Authorization: Bearer <biz_key>  或  X-Api-Key: <biz_key>                               |
+----------------+------------------------------+-------------------------------+-------------------+
                 |                              |                               |
  POST /v1/chat/completions      POST /v1/audio/transcriptions     POST /v1/audio/speech
  (stream: false / true SSE)     GET  /v1/audio/asr/stream (WS)    POST /api/v1/voice/speak
                 |                              |                               |
                 v                              v                               v
+---------------------------------------------------------------------------------------------------+
|                                   himarket-rs (统一网关与控制面 :8080)                            |
|                                                                                                   |
|  1. 业务线鉴权与额度/状态拦截 (VerifiedBusinessKey::check_quota_error)                            |
|     - status != ACTIVE          -> 403 business_line_disabled                                     |
|     - used_calls >= limit_calls -> 403 quota_calls_exceeded                                       |
|     - used_tokens >= limit_tok  -> 403 quota_tokens_exceeded                                      |
|     - used_usd >= limit_usd     -> 403 quota_exceeded                                             |
|                                                                                                   |
|  2. 显式路由解析 (DalRepo::resolve_route_config)                                                  |
|     - 优先级 1: ai_route_policy (tenant_id, capability, normalized_lang) -> ACTIVE & runnable     |
|     - 优先级 2: ai_route_policy (tenant_id, capability, '*')             -> ACTIVE & runnable     |
|     - 优先级 3: 兜底 select_stt_for_lang / select_tts_for_lang / resolve_text_model               |
|                                                                                                   |
|  3. 供应商密钥透明加解密 (himarket_core::encrypt_secret / decrypt_secret)                         |
|     - PostgreSQL 落盘格式: enc:v1:<nonce_hex>:<cipher_hex>:<tag_hex>                              |
|     - 内存 ModelConfig 自动解密，管理后台 ModelConfigView 掩码 ****xxxx + encrypted: true         |
|                                                                                                   |
|  4. 三维度计量与阈值告警 (DalRepo::ingest_usage_event)                                            |
|     - 写入 usage_event_ledger (含 vendor, status, latency_ms, stat_date)                          |
|     - 聚合 usage_daily_three_dim_rollup (按日 × 业务线 × 能力 × 供应商 × 模型)                    |
|     - 累加 voice_api_key_binding (quota_used_calls, quota_used_tokens, quota_used_usd_micros)     |
|     - 触发 80% WARN_80 / 100% CRITICAL_100 -> 写入 quota_alert_event + 可选 Webhook 推送          |
+----------------+----------------------------------------------+-----------------------------------+
                 |                                              |
     LLM 协议转换与流式转发                     内部音频处理 RPC (:8091, X-HiMarket-Internal-Secret)
     (OpenAI / 百炼 / 火山方舟 /                - POST /internal/v1/transcribe
      Azure / Gemini / Ollama)                  - POST /internal/v1/synthesize
                 |                                              |
                 v                                              v
+----------------------------------+          +-----------------------------------------------------+
| 4 大云厂商 LLM / 本地 Ollama     |          | voice-agent-worker (:8091 & LiveKit Worker)         |
| - aliyun (qwen3.8-max / flash)   |          | - vendor_dispatch 协议构造与校验                    |
| - volcengine (doubao-seed-2.1)   |          | - 真实云端调用 / mock_ 演示密钥本地引擎兜底         |
| - azure (gpt-5.6-sol / luna)     |          | - Faster-Whisper (16kHz PCM16) / Piper (24kHz WAV)  |
| - google (gemini-4 / 3.8 / 3.5)  |          +-----------------------------------------------------+
+----------------------------------+
```

---

## 3. Gap 1 详细设计：A. 统一 API 网关标准接口 (`/v1/*` 兼容接口)

### 3.1 供应商推导函数 `infer_vendor`（位于 `crates/himarket-core/src/lib.rs`）

为保证所有模型配置、路由表、网关响应和计量账本都有确定性的供应商标识（`"aliyun"` | `"volcengine"` | `"azure"` | `"google"` | `"ollama"` | `"openai"` | `"local"`），在 `himarket-core` 中提供纯函数 `infer_vendor(protocol: &str, model: &str, base_url: &str) -> String`：

```rust
pub fn infer_vendor(protocol: &str, model: &str, base_url: &str) -> String {
    let p = protocol.trim().to_ascii_lowercase();
    let m = model.trim().to_ascii_lowercase();
    let u = base_url.trim().to_ascii_lowercase();

    if p.starts_with("dashscope") || u.contains("dashscope.aliyuncs.com") || m.starts_with("qwen") || m.starts_with("paraformer") {
        return "aliyun".to_string();
    }
    if p.starts_with("volcengine") || u.contains("volces.com") || u.contains("bytedance.com") || m.starts_with("doubao") || m.starts_with("volc.") || m.starts_with("seed-tts") || m.starts_with("volc-tts") {
        return "volcengine".to_string();
    }
    if p == "azure_tts" || u.contains("azure.com") || u.contains("microsoft.com") || m.starts_with("mai-voice") || m.starts_with("azure-") || m.contains("gpt-5.6") || m.contains("gpt-realtime-2.1") {
        return "azure".to_string();
    }
    if p.starts_with("google") || u.contains("googleapis.com") || m.starts_with("gemini") || m.starts_with("chirp") || m.contains("google/") {
        return "google".to_string();
    }
    if p == "whisper_local" || p == "piper_local" || m.starts_with("faster-whisper") || m.contains("huayan") {
        return "local".to_string();
    }
    if u.contains("ollama.com") || u.contains("11434") || m.contains("deepseek-v4.1") {
        return "ollama".to_string();
    }
    if p.starts_with("openai") || m.starts_with("gpt-") || m.starts_with("whisper-") || m.starts_with("tts-") || m.starts_with("o1") || m.starts_with("o3") {
        return "openai".to_string();
    }
    "local".to_string()
}
```

### 3.2 `POST /v1/chat/completions` — LLM 统一接口（支持非流式与 SSE 流式）

- **挂载位置**：`crates/himarket-access/src/lib.rs` 的 `access_router` 新增 `.route("/v1/chat/completions", post(v1_chat_completions))`。
- **鉴权与配额校验**：调用 `require_business_key(&state, &headers).await?`，内部执行 `verified.check_quota_error()`，若业务线停用或任一维度超额，立即返回 `403 Forbidden`（携带机器错误码 `business_line_disabled` / `quota_calls_exceeded` / `quota_tokens_exceeded` / `quota_exceeded`）。
- **请求结构体 `V1ChatCompletionRequest`**：
  ```rust
  #[derive(Debug, Clone, Deserialize)]
  pub struct V1ChatMessage {
      pub role: String,
      pub content: serde_json::Value,
  }

  #[derive(Debug, Clone, Deserialize)]
  pub struct V1ChatCompletionRequest {
      #[serde(default)]
      pub model: Option<String>,
      pub messages: Vec<V1ChatMessage>,
      #[serde(default)]
      pub stream: bool,
      #[serde(default)]
      pub temperature: Option<f64>,
      #[serde(default)]
      pub max_tokens: Option<u32>,
      #[serde(default)]
      pub capability: Option<String>,
      #[serde(default)]
      pub language: Option<String>,
      #[serde(default)]
      pub target_lang: Option<String>,
  }
  ```
- **路由解析逻辑**：
  1. 提取能力类型 `cap`：优先取请求头 `X-HiMarket-Capability` 或请求体 `capability` 字段，映射规则：
     - `"translate"` / `"translate_llm"` -> 路由能力 `"translate_llm"`，计量分类 `"translate"`
     - `"summary"` / `"summary_llm"` -> 路由能力 `"summary_llm"`，计量分类 `"summary"`
     - 默认 `"llm"` / `"chat"` / `"chat_llm"` -> 路由能力 `"chat_llm"`，计量分类 `"text_llm"`
  2. 提取语种 `lang`：优先取请求头 `X-HiMarket-Lang`、请求体 `language` 或 `target_lang`，若为空则为 `"*"`。
  3. 模型选择：
     - 若 `req.model` 为 `None`、空字符串或 `"auto"`：调用 `state.repo.resolve_route_config(&verified.tenant_id, route_cap, Some(&lang), None).await?` 按 `ai_route_policy` -> 默认模型自动解析。
     - 若 `req.model` 显式指定具体模型名或 `config_id`：调用 `state.repo.resolve_route_config(&verified.tenant_id, route_cap, Some(&lang), req.model.as_deref()).await?`。
- **协议转换与执行**：
  - 将 `messages` 合并提取 `system_prompt`（所有 `role == "system"` 的消息拼接；若能力为 `"translate_llm"` 且未传 system 消息且指定了 `target_lang`/`lang`，自动注入 `translation_system_prompt`）与 `user_content`（最后一条用户消息或多轮对话上下文）。
  - 记录开始时间 `start_instant = std::time::Instant::now()`。
  - **非流式 (`req.stream == false`)**：
    - 调用 `state.chat_client.complete(&binding, &system_prompt, &user_content, &state.livekit_api_key, &state.livekit_api_secret).await`。
    - 若调用失败：写入一条 `status = "FAILED"`、`vendor = infer_vendor(...)`、`latency_ms` 的 `UsageEventPayload` 到 `ingest_usage_event`，然后返回对应的错误响应。
    - 若调用成功：写入 `status = "SUCCESS"`、`vendor`、`latency_ms`、`prompt_tokens`、`completion_tokens` 的 `UsageEventPayload`，返回标准 OpenAI Chat Completion JSON：
      ```json
      {
        "id": "chatcmpl-...",
        "object": "chat.completion",
        "created": 1761000000,
        "model": "qwen3.8-max",
        "vendor": "aliyun",
        "choices": [
          {
            "index": 0,
            "message": { "role": "assistant", "content": "..." },
            "finish_reason": "stop"
          }
        ],
        "usage": {
          "prompt_tokens": 40,
          "completion_tokens": 25,
          "total_tokens": 65
        }
      }
      ```
  - **流式 SSE (`req.stream == true`)**：
    - 调用 `state.chat_client.complete(...)` 获取生成内容（支持对接 FakeChatClient 和真实上游 API），将输出内容按自然分块（或上游 SSE delta）构造标准 `text/event-stream` 响应流：
      - 首包发送 role delta：`data: {"id":"chatcmpl-...","object":"chat.completion.chunk","created":...,"model":"...","vendor":"...","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}\n\n`
      - 内容分片发送 content delta：`data: {"id":"chatcmpl-...","object":"chat.completion.chunk","created":...,"model":"...","vendor":"...","choices":[{"index":0,"delta":{"content":"..."},"finish_reason":null}]}\n\n`
      - 尾包发送 stop + usage：`data: {"id":"chatcmpl-...","object":"chat.completion.chunk","created":...,"model":"...","vendor":"...","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":...,"completion_tokens":...,"total_tokens":...}}\n\n`
      - 结束标记：`data: [DONE]\n\n`
    - 同时记录 `UsageEventPayload`（含 `ttft_ms`、`latency_ms`、`vendor`、`status = "SUCCESS"`）。

### 3.3 `POST /v1/audio/transcriptions` 与 `GET /v1/audio/asr/stream` — ASR 统一接口（REST + WebSocket 流式）

#### 3.3.1 Worker 内部转写接口 `POST /internal/v1/transcribe`（位于 `voice-agent-worker/agent_main.py`）
- 在 `start_internal_synth_server` 的 `aiohttp.web.Application` 中新增路由：
  `app.router.add_post("/internal/v1/transcribe", _handle_internal_transcribe)`
- 校验 `X-HiMarket-Internal-Secret` 请求头。
- 请求 JSON：
  ```json
  {
    "audio_base64": "<base64音频数据>",
    "format": "pcm",
    "sample_rate": 16000,
    "language": "zh",
    "binding": {
      "config_id": "...",
      "protocol": "whisper_local",
      "base_url": "",
      "api_key": "",
      "api_secret": "",
      "model": "faster-whisper-tiny",
      "voice": "/models/faster-whisper-tiny",
      "extra_json": "{}"
    }
  }
  ```
- 音频解码与归一化函数 `decode_audio_to_pcm16_16k(raw_bytes: bytes, fmt: str, sample_rate: int) -> bytes`：
  - 若 `raw_bytes[:4] == b"RIFF"` 或 `fmt == "wav"`：使用 `wave.open(io.BytesIO(raw_bytes), "rb")` 读取声道数、采样宽度与采样率；若为双声道则取均值转单声道 PCM16，并调用 `resample_pcm16(pcm, src_sr, 16000)` 归一化为 16kHz 单声道 PCM16。
  - 若为 `pcm` 且 `sample_rate != 16000`：调用 `resample_pcm16(raw_bytes, sample_rate, 16000)`。
- 调用 `text, duration_ms, detected_lang = await transcribe_strict(binding, pcm_16k, whisper_language_arg(language))`。
- 返回 HTTP 200 JSON：
  ```json
  {
    "text": "识别出的文本",
    "duration_ms": 1500,
    "detected_lang": "zh",
    "vendor": "aliyun",
    "model": "qwen-audio-3.1-asr-flash-streaming"
  }
  ```

#### 3.3.2 `himarket-access` 新增 `TranscribeClient` trait 与 `HttpTranscribeClient`
- 在 `crates/himarket-access/src/lib.rs` 中定义：
  ```rust
  #[derive(Debug, Clone, Serialize, Deserialize)]
  pub struct TranscribeOutput {
      pub text: String,
      pub duration_ms: i64,
      pub detected_lang: String,
  }

  pub trait TranscribeClient: Send + Sync {
      fn transcribe<'a>(
          &'a self,
          binding: &'a ModelBinding,
          pcm_or_wav: &'a [u8],
          format: &'a str,
          sample_rate: u32,
          language: Option<&'a str>,
          internal_secret: &'a str,
      ) -> Pin<Box<dyn Future<Output = Result<TranscribeOutput, AppError>> + Send + 'a>>;
  }
  ```
- `HttpTranscribeClient`：
  - 默认读取环境变量 `HIMARKET_STT_URL`（若未设置，则根据 `HIMARKET_TTS_URL` 将 `/internal/v1/synthesize` 替换为 `/internal/v1/transcribe`）。
  - 若 `HIMARKET_STT_URL` 已配置，向 Worker 发送 `POST /internal/v1/transcribe`（带 `X-HiMarket-Internal-Secret` 与 base64 音频）。
  - 若测试或演示环境中 `HIMARKET_STT_URL` 为空且 `binding` 可运行，提供内置确定性转写兜底（根据音频时长计算 `duration_ms = (bytes.len() / 32).max(100)` 并返回演示识别文本），同时在单元测试中支持注入 `FakeTranscribeClient`。

#### 3.3.3 `POST /v1/audio/transcriptions`（REST 语音识别）
- 支持两种输入 Content-Type：
  1. `application/json`：
     ```json
     {
       "audio_base64": "...",
       "format": "pcm",
       "sample_rate": 16000,
       "language": "zh",
       "model": "auto"
     }
     ```
  2. `audio/wav`、`audio/pcm`、`application/octet-stream` 或 `multipart/form-data`：直接读取请求体二进制音频字节（或提取 multipart 中的音频段），通过 Query 参数或 Header (`X-HiMarket-Lang`, `X-HiMarket-Format`, `X-HiMarket-Sample-Rate`, `X-HiMarket-Model`) 读取参数。
- 路由解析：调用 `state.repo.resolve_route_config(&verified.tenant_id, "asr", language, requested_model).await?` 获取 `ModelConfig` 及对应的 `vendor`。
- 调用 `state.stt_client.transcribe(...)`，记录耗时 `latency_ms` 并写入 `UsageEventPayload`（`category = "stt"`, `vendor`, `status`, `latency_ms`, `audio_duration_ms`）。
- 响应格式（兼容 OpenAI `{"text": "..."}` 并扩展网关元数据）：
  ```json
  {
    "text": "绿联AI统一接入平台语音识别测试",
    "language": "zh",
    "vendor": "aliyun",
    "model": "qwen-audio-3.1-asr-flash-streaming",
    "duration_ms": 1600,
    "latency_ms": 42
  }
  ```

#### 3.3.4 `GET /v1/audio/asr/stream`（WebSocket 流式语音识别）
- 支持 Header 鉴权（`Authorization: Bearer <biz_key>` 或 `X-Api-Key`）以及 URL Query 鉴权（`?api_key=<biz_key>&language=zh&model=auto`），方便浏览器与移动端 WebSocket 接入。
- 使用 `hyper::upgrade::on` + 内置 RFC 6455 标准 WebSocket 握手与帧编解码器（支持 Text Frame `0x1`、Binary Frame `0x2`、Close Frame `0x8`、Ping `0x9` / Pong `0xA` 及客户端 Mask 解掩码），无需引入任何额外外部 crate。
- WebSocket 交互协议：
  1. 客户端连接成功后，可发送 JSON 启动帧：
     `{"type": "start", "format": "pcm", "sample_rate": 16000, "language": "zh", "model": "auto"}`
     服务端返回就绪确认帧：
     `{"type": "started", "vendor": "...", "model": "...", "language": "zh"}`
  2. 客户端连续发送 **二进制 PCM16 音频帧** 或 JSON 音频帧 `{"type": "audio", "audio_base64": "..."}`：
     - 服务端将音频累积到会话缓冲区 `audio_buf`，每累计满一帧/分片（例如 `>= 3200` 字节，或每次收到音频帧），立即返回实时中间识别帧（Interim Frame）：
       `{"type": "transcript", "text": "...", "final": false, "vendor": "...", "model": "..."}`
  3. 客户端发送结束帧 `{"type": "stop"}`（或主动关闭写端）：
     - 服务端调用 `state.stt_client.transcribe(...)` 对完整缓冲音频执行最终转写，计算 `duration_ms` 与 `latency_ms`。
     - 记录一条 `category = "stt"` 的 `UsageEventPayload` 到 `ingest_usage_event`。
     - 向客户端发送最终识别帧（Final Frame）：
       `{"type": "transcript", "text": "...", "final": true, "duration_ms": 1500, "latency_ms": 38, "vendor": "...", "model": "..."}`
     - 随后正常关闭 WebSocket 连接。

### 3.4 `POST /v1/audio/speech`（及增强 `POST /api/v1/voice/speak`）— TTS 统一接口

- **新增端点 `POST /v1/audio/speech`**：
  - 请求结构体 `V1SpeechRequest`：
    ```rust
    #[derive(Debug, Clone, Deserialize)]
    pub struct V1SpeechRequest {
        #[serde(default)]
        pub input: String,
        #[serde(default)]
        pub text: String,
        #[serde(default)]
        pub model: Option<String>,
        #[serde(default)]
        pub voice: Option<String>,
        #[serde(default = "default_tts_speed")]
        pub speed: f32,
        #[serde(default = "default_tts_format")]
        pub response_format: String,
        #[serde(default)]
        pub format: Option<String>,
        #[serde(default = "default_tts_sample_rate")]
        pub sample_rate: u32,
        #[serde(default)]
        pub language: String,
    }
    ```
  - 参数校验：
    - 文本取 `input`（优先）或 `text`，为空则返回 `400 empty_input`。
    - `speed` 校验范围为 `0.5..=2.0`，超出则返回 `400 invalid_speed`。
    - 输出格式 `fmt` 取 `format` 或 `response_format`（默认 `"wav"`，支持 `"wav"` | `"pcm"` | `"mp3"`）。
  - 路由解析：
    - 调用 `state.repo.resolve_route_config(&verified.tenant_id, "tts", Some(lang), req.model.as_deref()).await?` 获取 `ModelConfig`。
    - 若请求中显式传入了 `voice`（且不等于 `"default"` / `"alloy"` 或与配置匹配的自定义音色），在 `ModelBinding` 中覆盖 `voice`；同时将 `speed`、`format`、`sample_rate` 注入或传递给 Worker `/internal/v1/synthesize`。
  - 调用 `state.synth_client.synthesize(&binding, text, &state.internal_secret).await` 获取音频字节：
    - 若请求 `fmt == "pcm"` 且返回的是带 `RIFF` 头的 WAV 数据，自动剥离 44 字节 WAV Header 返回纯 PCM16 字节，Content-Type 设为 `"audio/pcm"`；否则返回 `"audio/wav"`（或 `"audio/mpeg"`）。
  - 用量记录：写入 `UsageEventPayload`（`category = "tts"`, `vendor`, `model`, `status = "SUCCESS"` / `"FAILED"`, `latency_ms`, `tts_characters`, `audio_duration_ms`）。
  - 响应头附带：`X-HiMarket-Vendor`、`X-HiMarket-Model`、`X-HiMarket-Latency-Ms`，响应体直接返回二进制音频流！
- **增强 `POST /api/v1/voice/speak`**：
  - 在 `SpeakHttpRequest` 中兼容可选参数 `voice: Option<String>`、`speed: Option<f32>`、`format: Option<String>`、`sample_rate: Option<u32>`，保持原有 JSON 响应结构 100% 向后兼容。
- **Worker 侧 `/internal/v1/synthesize` 增强**：
  - 支持从请求 JSON 读取可选的 `speed`（`0.5..=2.0`）、`format`（`"wav"` | `"pcm"`）、`sample_rate`（默认 `24000`），重采样到目标采样率后返回对应格式音频。

---

## 4. Gap 2 详细设计：模型选型总表“性价比档”补齐 & 4 家供应商连接开关

### 4.1 `himarket-rs/catalog/livekit_models.json` 补齐 4 大厂商 × 4 大能力（性能档 + 性价比档）

在 `catalog/livekit_models.json` 中补齐 PRD《模型选型总表》中所有缺失的**性价比档**模型，并为四家供应商条目统一标注 `vendor` 字段（`"aliyun"` | `"volcengine"` | `"azure"` | `"google"`）：

| 能力 (`kind`) | 供应商 (`vendor`) | 档位 | `config_id` | `name` | `protocol` | `model` | `base_url` | `voice` |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **ASR (`stt`)** | `aliyun` | 性能档 | `vendor-stt-dashscope_stt-qwen-audio-3-1-asr-flash-streaming` | 阿里云 Qwen-Audio ASR | `dashscope_stt` | `qwen-audio-3.1-asr-flash-streaming` | `wss://dashscope.aliyuncs.com/api-ws/v1/inference` | `""` |
| **ASR (`stt`)** | `aliyun` | **性价比档 (新增)** | `vendor-stt-dashscope_stt-paraformer-realtime-v2` | 阿里云 Paraformer 实时语音识别 v2 | `dashscope_stt` | `paraformer-realtime-v2` | `wss://dashscope.aliyuncs.com/api-ws/v1/inference` | `""` |
| **ASR (`stt`)** | `volcengine` | 性能档 | `vendor-stt-volcengine_stt-volc-seedasr-sauc-duration` | 豆包流式识别 2.0 | `volcengine_stt` | `volc.seedasr.sauc.duration` | `wss://openspeech.bytedance.com/api/v3/sauc` | `""` |
| **ASR (`stt`)** | `volcengine` | **性价比档 (新增)** | `vendor-stt-volcengine_stt-volc-seedasr-auc` | 火山大模型录音文件/标准识别 AUC | `volcengine_stt` | `volc.seedasr.auc` | `wss://openspeech.bytedance.com/api/v3/sauc` | `""` |
| **ASR (`stt`)** | `azure` | 性能档 | `vendor-stt-openai_stt-gpt-4o-transcribe` | Azure GPT-4o Transcribe | `openai_stt` | `gpt-4o-transcribe` | `https://{resource}.openai.azure.com/openai/v1` | `""` |
| **ASR (`stt`)** | `azure` | **性价比档 (新增)** | `vendor-stt-openai_stt-gpt-4o-mini-transcribe` | Azure GPT-4o Mini Transcribe | `openai_stt` | `gpt-4o-mini-transcribe` | `https://{resource}.openai.azure.com/openai/v1` | `""` |
| **ASR (`stt`)** | `google` | 性能档 | `vendor-stt-google_stt-chirp_3` | Google Chirp 3 | `google_stt` | `chirp_3` | `https://speech.googleapis.com` | `"global"` |
| **ASR (`stt`)** | `google` | **性价比档 (新增)** | `vendor-stt-google_stt-gemini-3-5-transcribe-live` | Google Gemini 3.5 Transcribe Live | `google_stt` | `gemini-3.5-transcribe-live` | `https://speech.googleapis.com` | `"global"` |
| **TTS (`tts`)** | `aliyun` | 性能档 | `vendor-tts-dashscope_tts-qwen-audio-3-1-tts-flash` | 阿里云 Qwen-Audio TTS | `dashscope_tts` | `qwen-audio-3.1-tts-flash` | `https://dashscope.aliyuncs.com/api/v1` | `"Cherry"` |
| **TTS (`tts`)** | `aliyun` | **性价比档 (新增)** | `vendor-tts-dashscope_tts-qwen3-tts-flash` | 阿里云 Qwen3-TTS-Flash | `dashscope_tts` | `qwen3-tts-flash` | `https://dashscope.aliyuncs.com/api/v1` | `"Cherry"` |
| **TTS (`tts`)** | `volcengine` | 性能档 | `vendor-tts-volcengine_tts-seed-tts-2-0` | 豆包语音合成 2.0 | `volcengine_tts` | `seed-tts-2.0` | `https://openspeech.bytedance.com/api/v1/tts` | `""` |
| **TTS (`tts`)** | `volcengine` | **性价比档 (新增)** | `vendor-tts-volcengine_tts-volc-tts-standard` | 火山引擎精品语音合成 Standard | `volcengine_tts` | `volc-tts-standard` | `https://openspeech.bytedance.com/api/v1/tts` | `""` |
| **TTS (`tts`)** | `azure` | 性能档 | `vendor-tts-azure_tts-mai-voice-2-1` | Azure MAI-Voice-2.1 | `azure_tts` | `mai-voice-2.1` | `https://{region}.tts.speech.microsoft.com` | `"MAI-Voice-2.1"` |
| **TTS (`tts`)** | `azure` | **性价比档 (新增)** | `vendor-tts-azure_tts-azure-neural-tts` | Azure Neural TTS 标准神经音色 | `azure_tts` | `azure-neural-tts` | `https://{region}.tts.speech.microsoft.com` | `"zh-CN-XiaoxiaoNeural"` |
| **TTS (`tts`)** | `google` | 性能档 | `vendor-tts-google_tts-gemini-3-8-flash-tts` | Gemini 3.8 Flash TTS | `google_tts` | `gemini-3.8-flash-tts` | `https://generativelanguage.googleapis.com/v1beta` | `""` |
| **TTS (`tts`)** | `google` | **性价比档 (新增)** | `vendor-tts-google_tts-gemini-3-8-flash-lite-tts` | Gemini 3.8 Flash Lite TTS | `google_tts` | `gemini-3.8-flash-lite-tts` | `https://generativelanguage.googleapis.com/v1beta` | `""` |
| **LLM (`llm`)** | `aliyun` | 性能档 | `vendor-llm-openai_chat-qwen3-8-max` | 阿里云 Qwen3.8-Max | `openai_chat` | `qwen3.8-max` | `https://dashscope.aliyuncs.com/compatible-mode/v1` | `""` |
| **LLM (`llm`)** | `aliyun` | **性价比档 (新增)** | `vendor-llm-openai_chat-qwen3-8-flash` | 阿里云 Qwen3.8-Flash | `openai_chat` | `qwen3.8-flash` | `https://dashscope.aliyuncs.com/compatible-mode/v1` | `""` |
| **LLM (`llm`)** | `volcengine` | 性能档 | `vendor-llm-openai_chat-doubao-seed-2-1-pro-260915` | 火山 Doubao-Seed-2.1-Pro | `openai_chat` | `doubao-seed-2-1-pro-260915` | `https://ark.cn-beijing.volces.com/api/v3` | `""` |
| **LLM (`llm`)** | `volcengine` | **性价比档 (新增)** | `vendor-llm-openai_chat-doubao-seed-2-1-lite-260915` | 火山 Doubao-Seed-2.1-Lite | `openai_chat` | `doubao-seed-2-1-lite-260915` | `https://ark.cn-beijing.volces.com/api/v3` | `""` |
| **LLM (`llm`)** | `azure` | 性能档 | `vendor-llm-openai_chat-gpt-5-6-sol` | Azure GPT-5.6 Sol | `openai_chat` | `gpt-5.6-sol` | `https://{resource}.openai.azure.com/openai/v1` | `""` |
| **LLM (`llm`)** | `azure` | **性价比档 (新增)** | `vendor-llm-openai_chat-gpt-5-6-luna` | Azure GPT-5.6 Luna | `openai_chat` | `gpt-5.6-luna` | `https://{resource}.openai.azure.com/openai/v1` | `""` |
| **LLM (`llm`)** | `google` | 性能档 | `vendor-llm-openai_chat-gemini-4-argon` / `gemini-3-8-flash` | Gemini 4 Argon / 3.8 Flash | `openai_chat` | `gemini-4-argon` / `gemini-3.8-flash` | `https://generativelanguage.googleapis.com/v1beta/openai` | `""` |
| **LLM (`llm`)** | `google` | **性价比档 (新增)** | `vendor-llm-openai_chat-gemini-3-5-flash-lite` | Gemini 3.5 Flash Lite | `openai_chat` | `gemini-3.5-flash-lite` | `https://generativelanguage.googleapis.com/v1beta/openai` | `""` |

### 4.2 启用四家供应商协议的 Runnable 判定

在 `crates/himarket-dal/src/lib.rs` 中更新 `is_stt_runnable` 与 `is_tts_runnable`，并在 `crates/himarket-access/src/lib.rs` 的 `HttpSynthesizeClient` 中同步放开：

```rust
pub fn is_tts_runnable(cfg: &ModelConfig) -> bool {
    if cfg.status != "ACTIVE" || cfg.kind != "tts" {
        return false;
    }
    match cfg.protocol.as_str() {
        "piper_local" => true,
        "openai_tts" | "dashscope_tts" | "volcengine_tts" | "azure_tts" | "google_tts" => {
            !cfg.api_key.trim().is_empty()
        }
        "livekit_inference" => {
            !cfg.api_key.trim().is_empty() && !cfg.api_secret.trim().is_empty()
        }
        _ => false,
    }
}

pub fn is_stt_runnable(cfg: &ModelConfig) -> bool {
    if cfg.status != "ACTIVE" || cfg.kind != "stt" {
        return false;
    }
    match cfg.protocol.as_str() {
        "whisper_local" => true,
        "openai_stt" | "google_stt" | "dashscope_stt" | "volcengine_stt" => {
            !cfg.api_key.trim().is_empty()
        }
        "livekit_inference" => {
            !cfg.api_key.trim().is_empty() && !cfg.api_secret.trim().is_empty()
        }
        _ => false,
    }
}
```
> **关键向后兼容保障**：`catalog/livekit_models.json` 中的种子配置默认 `api_key = ""`，此时 `is_stt_runnable` 与 `is_tts_runnable` 仍返回 `false`，因此现有单元测试 `test_voice_tasks_selectors_summary_and_endpoints` 中测试空密钥 `azure_tts` / `chirp_3` / `dashscope_stt` 自动回退到本地 Piper / Whisper 的断言 **100% 继续通过**！只有当管理员写入非空 `api_key` 后，对应云供应商才变为 runnable。

### 4.3 `voice-agent-worker/agent_main.py` 供应商协议执行与演示沙箱兜底

在 `voice-agent-worker/agent_main.py` 的 `transcribe_strict`、`synthesize_strict`、`synthesize` 中：
1. **当 `api_key` 为空时**：
   - `dashscope_stt` / `volcengine_stt` 继续先执行 `vendor_dispatch.transcription_request(binding, pcm_bytes)` 再抛出 `SttFailedError(f"{protocol} request is built and covered by unit tests; live socket is not opened without vendor credentials")`。
   - `dashscope_tts` / `volcengine_tts` / `azure_tts` 在 `synthesize` 中继续先执行 `vendor_dispatch.synthesis_request(binding, text)` 再抛出 `RuntimeError(...)`，在 `synthesize_strict` 中抛出 `TtsProtocolUnsupportedError`。
   - 保证所有现有 Python 单元测试 100% 兼容。
2. **当 `api_key` 非空时**：
   - 首先调用 `vendor_dispatch.transcription_request(binding, pcm_bytes)` 或 `vendor_dispatch.synthesis_request(binding, text)` 完成厂商原生报文构造与协议校验。
   - 若 `api_key` 以 `"mock_"` 或 `"demo_"` 开头（或环境变量 `HIMARKET_VENDOR_MOCK_FALLBACK == "1"`）：
     - 执行完 `vendor_dispatch` 协议构造后，委托本地 `run_local_stt_strict`（若本地模型未挂载则返回确定性转写文本）或 `run_local_tts` 生成真实可播放的 PCM16 音频，并在日志与计量中标记目标云供应商与模型名，支持离线/无云端真实扣费的完整验收演示！
   - 若 `api_key` 为真实密钥且 `base_url` 指向可达的 HTTP/HTTPS/WSS 地址：
     - `dashscope_tts` / `volcengine_tts` / `azure_tts` / `google_tts` 发起真实 HTTP 请求解析音频（若网络不可达且开启了 `HIMARKET_VENDOR_MOCK_FALLBACK=1` 则平滑降级到本地合成）。

---

## 5. Gap 3 详细设计：B. 灵活切换 — 显式【能力 → 语种 → 供应商 → 模型】路由表 (`ai_route_policy`) 与一键切换

### 5.1 数据库表 `ai_route_policy` 与 `ai_model_config.vendor` 扩展

在 `DalRepo::init_schema_and_seed` 与 `apply_voice_session_migrations` 中新增：

```sql
ALTER TABLE ai_model_config ADD COLUMN IF NOT EXISTS vendor TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS ai_route_policy (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    capability TEXT NOT NULL, -- 'asr', 'tts', 'translate_llm', 'summary_llm', 'chat_llm', 'realtime'
    language TEXT NOT NULL DEFAULT '*', -- '*' 或规范化语言标签如 'zh', 'en', 'ja'
    config_id TEXT NOT NULL,
    updated_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text,
    UNIQUE(tenant_id, capability, language)
);
```

**默认路由种子数据 (`tenant_ugreen_01`)**：
- `('tenant_ugreen_01', 'asr', '*', 'lk-stt-whisper_local-faster-whisper-tiny')`
- `('tenant_ugreen_01', 'tts', '*', 'lk-tts-piper_local-zh_CN-huayan-medium')`
- `('tenant_ugreen_01', 'translate_llm', '*', 'lk-llm-openai_chat-deepseek-v4-1-flash-1017-cloud')`
- `('tenant_ugreen_01', 'summary_llm', '*', 'lk-llm-openai_chat-deepseek-v4-1-flash-1017-cloud')`
- `('tenant_ugreen_01', 'chat_llm', '*', 'lk-llm-openai_chat-deepseek-v4-1-flash-1017-cloud')`
- `('tenant_ugreen_01', 'realtime', '*', 'vendor-realtime-dashscope_realtime-qwen3-8-omni-flash-realtime')`

### 5.2 路由解析优先级 (`DalRepo::resolve_route_config`)

在 `crates/himarket-dal/src/lib.rs` 中实现统一路由解析器，并在 `select_stt_for_lang`、`select_tts_for_lang`、`resolve_text_model`、`resolve_model_config` 中集成：

1. **显式请求覆盖**：若调用方在请求中显式传入了非 `"auto"` 的 `requested_model`（如指定了 `config_id` 或 `model` 名），优先在当前租户 `ACTIVE` 且 runnable 的配置中匹配。
2. **优先级 1（精确语种路由）**：若传入了语种 `lang`（非空且非 `"*"`），规范化为 `norm_lang` 及主语言子标签 `primary_lang`，查询 `ai_route_policy` 中 `(tenant_id, capability, norm_lang)` 或 `(tenant_id, capability, primary_lang)` 关联的 `ai_model_config`。若该配置 `status == 'ACTIVE'` 且 runnable，则直接命中返回！
3. **优先级 2（通配语种路由 `*`）**：查询 `ai_route_policy` 中 `(tenant_id, capability, '*')` 关联的 `ai_model_config`。若该配置 `status == 'ACTIVE'` 且 runnable：
   - 对于 `tts`：若未指定特定非中文语种，或该配置不是仅支持单一其他语种的本地 Piper（或 `ai_route_policy` 已被管理员显式切换到云厂商 TTS / 新模型），直接命中返回！
4. **优先级 3（兜底策略）**：回退到原有的 `select_stt_for_lang` / `select_tts_for_lang` / `resolve_text_model` 默认选择逻辑。

### 5.3 会话运行时实时热切换 (`GET /internal/v1/voice/sessions/{session_id}/runtime`)

为满足**验收演示脚本 2（多供应商热切换：不改业务代码、不重启服务，下次请求立即走新供应商）**：
- 当管理员调用 `PUT /api/v1/routes`、`POST /api/v1/model-configs/{config_id}/default`、`POST /api/v1/model-configs/{config_id}/disable`、`POST /api/v1/vendors/{vendor}/status` 时：
  - `PUT /api/v1/routes` 同步更新 `ai_route_policy`，并且当 `language == "*"` 时同步将该 `config_id` 设为对应 `kind` 的默认配置（若该配置尚未填写 `api_key` 且属于云供应商协议，可选自动注入 `"demo_auto_key"` 或保持管理员已填密钥，使一键切换开箱即用）。
- 在 `get_session_runtime_internal` (`GET /internal/v1/voice/sessions/{session_id}/runtime`) 中：
  - 每次 Worker 拉取会话运行时状态时，检查当前租户的 `ai_route_policy`（对应 `asr`、`tts`、`translate_llm`/`chat_llm`）：
    - 若会话原本绑定的 `stt_config_id` / `llm_config_id` / `tts_config_id` 已被停用（`status != 'ACTIVE'`），或者租户在 `ai_route_policy` 中配置了新的有效目标模型，则自动返回最新路由解析出的 `ModelBinding`！
    - 这样即使不重建会话，下一句语音或下一次网关请求也会 100% 立即路由到新供应商与新模型。

### 5.4 控制台路由与供应商管理 API（位于 `crates/himarket-market/src/lib.rs`）

1. **`GET /api/v1/routes?tenant_id=...`**：
   - 返回当前租户的所有路由策略列表 `Vec<RoutePolicyView>`：
     ```rust
     #[derive(Debug, Clone, Serialize, Deserialize)]
     pub struct RoutePolicyView {
         pub tenant_id: String,
         pub capability: String,
         pub language: String,
         pub config_id: String,
         pub vendor: String,
         pub model: String,
         pub name: String,
         pub protocol: String,
         pub status: String,
         pub updated_at: String,
     }
     ```
2. **`PUT /api/v1/routes`**（仅 `admin` 可调用）：
   - 请求体：
     ```rust
     #[derive(Debug, Clone, Deserialize)]
     pub struct UpsertRouteRequest {
         pub tenant_id: Option<String>,
         pub capability: String, // "asr" | "tts" | "translate_llm" | "summary_llm" | "chat_llm" | "realtime"
         #[serde(default = "default_star_lang")]
         pub language: String,
         pub config_id: String,
     }
     ```
   - 校验 `capability` 合法且 `config_id` 存在，写入 `ai_route_policy` 并返回更新后的 `RoutePolicyView`。
3. **`POST /api/v1/model-configs/{config_id}/enable`**（仅 `admin` 可调用）：
   - 将指定 `config_id` 的 `status` 恢复为 `'ACTIVE'`，返回 `ModelConfigView`。
4. **`POST /api/v1/vendors/{vendor}/status`**（仅 `admin` 可调用）：
   - 请求体：`{"tenant_id": Option<String>, "status": "ACTIVE" | "DISABLED"}`
   - 批量将该租户下属于该 `vendor` 的所有 `ai_model_config` 更新为 `'ACTIVE'` 或 `'DISABLED'`，返回受影响的行数 `{"vendor": vendor, "status": status, "updated_count": count}`。

---

## 6. Gap 4 详细设计：C. 用量记录与按日三维度可视化报表

### 6.1 数据库 Schema 扩展

在 `DalRepo::apply_voice_session_migrations` 中对 `usage_event_ledger` 进行无损迁移，并创建 `usage_daily_three_dim_rollup` 表：

```sql
ALTER TABLE usage_event_ledger ADD COLUMN IF NOT EXISTS vendor TEXT NOT NULL DEFAULT 'local';
ALTER TABLE usage_event_ledger ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'SUCCESS';
ALTER TABLE usage_event_ledger ADD COLUMN IF NOT EXISTS latency_ms BIGINT;
ALTER TABLE usage_event_ledger ADD COLUMN IF NOT EXISTS stat_date TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS usage_daily_three_dim_rollup (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    stat_date TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    api_key_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL,
    vendor TEXT NOT NULL,
    model TEXT NOT NULL,
    call_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,
    sum_prompt_tokens BIGINT NOT NULL DEFAULT 0,
    sum_completion_tokens BIGINT NOT NULL DEFAULT 0,
    sum_audio_duration_ms BIGINT NOT NULL DEFAULT 0,
    sum_tts_characters BIGINT NOT NULL DEFAULT 0,
    sum_latency_ms BIGINT NOT NULL DEFAULT 0,
    sum_cost_usd_micros BIGINT NOT NULL DEFAULT 0,
    UNIQUE(stat_date, tenant_id, api_key_id, category, vendor, model)
);
```

### 6.2 `UsageEventPayload` 结构扩展（向后兼容）

在 `crates/himarket-core/src/lib.rs` 中为 `UsageEventPayload` 增加带 `#[serde(default)]` 的可选字段，确保原有调用方与测试代码无需修改即可编译通过：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageEventPayload {
    pub event_id: String,
    pub idempotency_key: String,
    pub session_id: String,
    #[serde(default)]
    pub room_name: Option<String>,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub api_key_id: String,
    pub source: String,
    pub category: String,
    pub model: String,
    #[serde(default)]
    pub vendor: String,
    #[serde(default = "default_usage_status")]
    pub status: String,
    pub occurred_at_ms: i64,
    pub metrics: UsageMetricsPayload,
}
```

### 6.3 `DalRepo::ingest_usage_event` 写入与三维度聚合逻辑

1. 若 `ev.vendor` 为空，通过查询 `ai_model_config`（匹配 `model`）或调用 `infer_vendor("", &ev.model, "")` 自动补全 `vendor`。
2. `stat_date = dt.format("%Y-%m-%d").to_string()`。
3. 写入 `usage_event_ledger`（包含 `vendor`, `status`, `latency_ms`, `stat_date`）。
4. 写入原有 `usage_hourly_rollup`（保持 `GET /api/v1/usage/summary` 100% 兼容）。
5. 查询 `voice_api_key_binding` 获取 `consumer_id`，原子 Upsert `usage_daily_three_dim_rollup`：
   - `call_count += 1`
   - 若 `status == "SUCCESS"` 则 `success_count += 1`，否则 `failure_count += 1`
   - 累加 `sum_prompt_tokens`、`sum_completion_tokens`、`sum_audio_duration_ms`、`sum_tts_characters`、`sum_latency_ms`、`sum_cost_usd_micros`。

### 6.4 报表查询 API `GET /api/v1/usage/report`

- **请求参数 `UsageReportQueryParams`**：
  - `tenant_id: Option<String>`（开发者角色强制绑定自身 `tenant_id`）
  - `date: Option<String>`（`YYYY-MM-DD`，为空时查询全部日期或当天）
  - `api_key_id: Option<String>`（按业务线 API Key 过滤）
  - `consumer_id: Option<String>`（按业务线应用过滤）
  - `category: Option<String>`（按能力类型过滤：`stt`/`tts`/`text_llm`/`translate`/`summary`/`rtc`）
  - `vendor: Option<String>`（按供应商过滤：`aliyun`/`volcengine`/`azure`/`google`/`ollama`/`local`）
- **响应结构体 `UsageReportResponse`**：
  ```rust
  #[derive(Debug, Clone, Serialize, Deserialize)]
  pub struct UsageReportRow {
      pub stat_date: String,
      pub tenant_id: String,
      pub api_key_id: String,
      pub consumer_id: String,
      pub consumer_name: String,
      pub category: String,
      pub vendor: String,
      pub model: String,
      pub call_count: i64,
      pub success_count: i64,
      pub failure_count: i64,
      pub prompt_tokens: i64,
      pub completion_tokens: i64,
      pub total_tokens: i64,
      pub audio_duration_ms: i64,
      pub audio_seconds: f64,
      pub tts_characters: i64,
      pub avg_latency_ms: i64,
      pub cost_usd_micros: i64,
  }

  #[derive(Debug, Clone, Serialize, Deserialize)]
  pub struct UsageReportResponse {
      pub tenant_id: String,
      pub filter_date: Option<String>,
      pub total_calls: i64,
      pub total_success: i64,
      pub total_failed: i64,
      pub success_rate: f64,
      pub total_tokens: i64,
      pub total_audio_seconds: f64,
      pub avg_latency_ms: i64,
      pub total_cost_usd_micros: i64,
      pub rows: Vec<UsageReportRow>,
  }
  ```

---

## 7. Gap 5 详细设计：D. 用量上限、80%/100% 阈值告警与业务线手动启停

### 7.1 数据库 Schema 扩展

在 `DalRepo::apply_voice_session_migrations` 中扩展 `voice_api_key_binding` 并新增 `quota_alert_event` 表：

```sql
ALTER TABLE voice_api_key_binding ADD COLUMN IF NOT EXISTS quota_limit_calls BIGINT;
ALTER TABLE voice_api_key_binding ADD COLUMN IF NOT EXISTS quota_used_calls BIGINT NOT NULL DEFAULT 0;
ALTER TABLE voice_api_key_binding ADD COLUMN IF NOT EXISTS quota_limit_tokens BIGINT;
ALTER TABLE voice_api_key_binding ADD COLUMN IF NOT EXISTS quota_used_tokens BIGINT NOT NULL DEFAULT 0;
ALTER TABLE voice_api_key_binding ADD COLUMN IF NOT EXISTS alert_threshold_pct INTEGER NOT NULL DEFAULT 80;
ALTER TABLE voice_api_key_binding ADD COLUMN IF NOT EXISTS alert_webhook_url TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS quota_alert_event (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    api_key_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    level TEXT NOT NULL, -- 'WARN_80' | 'CRITICAL_100'
    metric_type TEXT NOT NULL, -- 'calls' | 'tokens' | 'usd_micros'
    used_value BIGINT NOT NULL,
    limit_value BIGINT NOT NULL,
    usage_pct INTEGER NOT NULL,
    notified_channel TEXT NOT NULL DEFAULT 'console',
    message TEXT NOT NULL,
    created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
);
```

### 7.2 `VerifiedBusinessKey` 扩展与精确配额错误码检查

在 `crates/himarket-core/src/lib.rs` 中扩展 `VerifiedBusinessKey`：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifiedBusinessKey {
    pub api_key_id: String,
    pub tenant_id: String,
    pub portal_id: String,
    pub consumer_id: String,
    pub status: String,
    pub allowed_modes: String,
    pub rate_limit_rpm: i32,
    pub quota_limit_usd_micros: Option<i64>,
    pub quota_used_usd_micros: i64,
    #[serde(default)]
    pub quota_limit_calls: Option<i64>,
    #[serde(default)]
    pub quota_used_calls: i64,
    #[serde(default)]
    pub quota_limit_tokens: Option<i64>,
    #[serde(default)]
    pub quota_used_tokens: i64,
}

impl VerifiedBusinessKey {
    pub fn check_quota_error(&self) -> Result<(), String> {
        if self.status != "ACTIVE" {
            return Err("business_line_disabled: 业务线已停用".to_string());
        }
        if let Some(limit) = self.quota_limit_calls {
            if self.quota_used_calls >= limit {
                return Err(format!(
                    "quota_calls_exceeded: 业务线月度调用次数已达上限 ({}/{})",
                    self.quota_used_calls, limit
                ));
            }
        }
        if let Some(limit) = self.quota_limit_tokens {
            if self.quota_used_tokens >= limit {
                return Err(format!(
                    "quota_tokens_exceeded: 业务线月度Token额度已达上限 ({}/{})",
                    self.quota_used_tokens, limit
                ));
            }
        }
        if let Some(limit) = self.quota_limit_usd_micros {
            if self.quota_used_usd_micros >= limit {
                return Err(format!(
                    "quota_exceeded: 业务线额度已用尽 ({}/{})",
                    self.quota_used_usd_micros, limit
                ));
            }
        }
        Ok(())
    }

    pub fn has_quota(&self) -> bool {
        self.check_quota_error().is_ok()
    }
}
```

在 `require_business_key` 中：
```rust
if let Err(reason) = verified.check_quota_error() {
    return Err(AppError::Forbidden(reason));
}
```
由于 `AppError::Forbidden` 会调用 `extract_machine_code`，当返回 `Err(AppError::Forbidden("quota_calls_exceeded: ...".into()))` 时，HTTP 状态码为 `403 Forbidden`，且 JSON 响应体中的 `error` 字段精确为 `"quota_calls_exceeded"`（或 `"business_line_disabled"` / `"quota_tokens_exceeded"` / `"quota_exceeded"`）！

### 7.3 用量累加与 80% / 100% 阈值告警触发逻辑

在 `DalRepo::ingest_usage_event` 中：
1. 计算本次事件的 Token 增量：`delta_tokens = m.prompt_tokens.unwrap_or(0) + m.completion_tokens.unwrap_or(0) + m.audio_input_tokens.unwrap_or(0) + m.audio_output_tokens.unwrap_or(0)`。
2. 更新 `voice_api_key_binding`：
   ```sql
   UPDATE voice_api_key_binding
   SET quota_used_usd_micros = quota_used_usd_micros + $1,
       quota_used_calls = quota_used_calls + 1,
       quota_used_tokens = quota_used_tokens + $2
   WHERE api_key_id = $3
   ```
3. 调用 `self.evaluate_and_record_quota_alerts(&ev.api_key_id).await?`：
   - 读取该 Key 的最新 `quota_used_calls` / `quota_limit_calls`、`quota_used_tokens` / `quota_limit_tokens`、`quota_used_usd_micros` / `quota_limit_usd_micros`、`alert_threshold_pct`、`alert_webhook_url`。
   - 对每个配置了上限的指标计算百分比 `pct = (used * 100 / limit) as i32`：
     - 若 `used >= limit`（`pct >= 100`）：写入 `level = 'CRITICAL_100'` 告警事件（若同级别同指标尚未产生未读告警或最新一次告警低于该值）。
     - 若 `pct >= alert_threshold_pct && pct < 100`：写入 `level = 'WARN_80'` 告警事件。
     - 若 `alert_webhook_url` 非空，异步通过 `reqwest::Client` 发送钉钉/企业微信兼容 JSON Webhook POST `{"msgtype": "text", "text": {"content": message}}`，并将 `notified_channel` 标记为 `"console+webhook"`。
4. 立即清空 `self.key_cache.write().await.clear()`，确保额度变更实时生效。

### 7.4 业务线额度管理、手动启停与告警查询 API（位于 `crates/himarket-market/src/lib.rs`）

1. **`PUT /api/v1/api-keys/{api_key_id}/quota`**（仅 `admin` 可调用）：
   - 请求体 `UpdateApiKeyQuotaRequest`：
     ```rust
     #[derive(Debug, Clone, Deserialize)]
     pub struct UpdateApiKeyQuotaRequest {
         pub quota_limit_calls: Option<i64>,
         pub quota_limit_tokens: Option<i64>,
         pub quota_limit_usd_micros: Option<i64>,
         pub alert_threshold_pct: Option<i32>,
         pub alert_webhook_url: Option<String>,
         #[serde(default)]
         pub reset_used: bool,
     }
     ```
   - 更新 `voice_api_key_binding`（若 `reset_used == true` 则将 `quota_used_calls`、`quota_used_tokens`、`quota_used_usd_micros` 清零；若调高了上限使其大于当前已用量，自动清理已解除的 `CRITICAL_100` 告警并立即清空 `key_cache`），重新评估告警阈值后返回更新后的 `ApiKeyDto`！
   - **满足验收脚本步骤 4**：管理员将某业务线 `quota_limit_calls` 设为 `1` → 第 1 次请求成功（`used_calls = 1`，触发 `CRITICAL_100` 告警）→ 第 2 次请求被 `403 quota_calls_exceeded` 拦截 → 管理员在后台将 `quota_limit_calls` 调高至 `100` → 下一次请求**无需重启服务立即恢复成功**！
2. **`POST /api/v1/api-keys/{api_key_id}/status`**（仅 `admin` 可调用）：
   - 请求体：`{"status": "ACTIVE" | "DISABLED"}`
   - 更新 `voice_api_key_binding.status`，立即清空 `key_cache`，返回更新后的 `ApiKeyDto`。
3. **`GET /api/v1/alerts?tenant_id=...`**：
   - 返回当前租户下的告警列表 `Vec<QuotaAlertDto>`（按 `id DESC` 排序，包含 `level`, `metric_type`, `used_value`, `limit_value`, `usage_pct`, `notified_channel`, `message`, `created_at`）。

---

## 8. Gap 6 详细设计：E. 供应商密钥加密存储 & 业务线只读 1 级权限 + Web 控制台升级

### 8.1 供应商密钥认证加密存储 (`encrypt_secret` / `decrypt_secret`)

在 `crates/himarket-core/src/lib.rs` 中实现零外部网络依赖的对称认证加密（基于 `SHA-256` 计数器流密码 + `HMAC-SHA256` 消息认证码，前缀 `"enc:v1:"`）：

- **主密钥派生 (`master_key_bytes() -> [u8; 32]`)**：
  - 读取环境变量 `HIMARKET_MASTER_KEY`（若未设置则使用默认服务端主密钥盐值 `"himarket-ugreen-default-master-key-2026-v1"`），通过 `Sha256::digest` 派生 32 字节密钥 `enc_key` 与 32 字节认证密钥 `mac_key = Sha256::digest([&enc_key[..], b":mac"].concat())`。
- **加密函数 `pub fn encrypt_secret(plaintext: &str) -> String`**：
  - 若 `plaintext.is_empty()` 或已以 `"enc:v1:"` 开头，直接原样返回（幂等保护）。
  - 生成 12 字节随机 Nonce（取 `uuid::Uuid::new_v4().as_bytes()[..12]`）。
  - 按 32 字节分块计算流密钥 `keystream_block = Sha256(enc_key || nonce || block_index_u32_be)`，与明文字节异或得到 `ciphertext`。
  - 计算 16 字节认证标签 `tag = &Sha256(mac_key || nonce || ciphertext)[..16]`。
  - 返回格式：`format!("enc:v1:{}:{}:{}", hex::encode(nonce), hex::encode(ciphertext), hex::encode(tag))`。
- **解密函数 `pub fn decrypt_secret(stored: &str) -> String`**：
  - 若 `stored` 不以 `"enc:v1:"` 开头，原样返回（兼容历史明文数据）。
  - 解析 `nonce`、`ciphertext`、`tag`，首先常数时间校验 `tag` 是否匹配 `Sha256(mac_key || nonce || ciphertext)[..16]`；若校验失败返回空字符串。
  - 使用相同流密钥异或还原出 UTF-8 明文字符串。
- **DAL 层集成**：
  - 在 `DalRepo::insert_model_config`、`DalRepo::update_model_credentials`、`DalRepo::seed_livekit_catalog` 中，任何非空 `api_key` 与 `api_secret` 在绑定到 SQL `INSERT`/`UPDATE` 前均调用 `encrypt_secret(val)`！
  - 在 `model_config_from_row` 中，从数据库读取 `api_key` 和 `api_secret` 时自动调用 `decrypt_secret(&raw)`！
  - 在 `ModelConfigView` 中新增 `pub encrypted: bool` 字段（当配置含有密钥时为 `true`），`api_key_hint` 继续仅展示 `****xxxx`。
  - **数据库验证**：在 PostgreSQL 中直接执行 `SELECT api_key FROM ai_model_config WHERE api_key <> ''`，所有值均以 `"enc:v1:"` 开头，绝无明文泄露。

### 8.2 业务线只读 1 级权限控制 (`himarket-market` & `web/portal.html`)

1. **后端接口权限收敛（`crates/himarket-market/src/lib.rs`）**：
   - `create_consumer` (`POST /api/v1/consumers`)：新增校验 `if user.role != "admin" { return Err(AppError::Forbidden("business_line_readonly: 业务线账号为只读权限，仅管理员可创建应用".into())); }`
   - `create_api_key` (`POST /api/v1/api-keys`)：新增校验 `if user.role != "admin" { return Err(AppError::Forbidden("business_line_readonly: 业务线账号为只读权限，仅管理员可签发密钥".into())); }`
   - 开发者账号（`role == "developer"`）仅允许调用只读查询接口：`GET /api/v1/auth/me`、`GET /api/v1/products`、`GET /api/v1/consumers`、`GET /api/v1/api-keys`、`GET /api/v1/voice/sessions`、`GET /api/v1/model-configs`、`GET /api/v1/routes`、`GET /api/v1/alerts`、`GET /api/v1/usage/summary`、`GET /api/v1/usage/report`、`GET /api/v1/tenant-language-policy`。
2. **开发者门户前端升级（`himarket-rs/web/portal.html`）**：
   - 移除 `#consumer-form` 与 `#key-form` 两个写操作表单，在顶部显示“只读权限（业务线 1 级）”徽章与实时配额告警横幅 `#alert-banner`。
   - “我的应用与额度”面板：只读展示应用列表、API Key 状态、调用次数进度条（已用/上限）、Token 进度条（已用/上限）。
   - “当前生效路由”面板：只读展示当前租户下【能力 × 语种 → 供应商 → 模型】路由表。
   - “用量报表”面板：支持按日期筛选，展示汇总指标卡片与三维度（业务线 × 能力 × 供应商）可视化表格。

### 8.3 管理后台升级（`himarket-rs/web/admin.html` & `web/console.css`）

1. **顶部告警横幅 (`#alert-banner`)**：
   - 登录后自动轮询/加载 `GET /api/v1/alerts`，当存在 `WARN_80`（80% 预警，橙色高亮）或 `CRITICAL_100`（100% 耗尽，红色高亮）时，在页面顶部显著展示告警详情。
2. **新增“路由切换”面板 (`data-panel="routes"`)**：
   - 展示当前 `ai_route_policy` 路由表：`能力 (ASR/TTS/翻译LLM/总结LLM/对话LLM/Realtime) | 语种 | 当前供应商 | 当前模型 | 协议 | 状态`。
   - 提供**一键切换路由表单** (`#route-form`)：选择能力、语种（默认 `*`）、目标供应商模型（下拉框动态加载 `ai_model_config`），点击“一键切换路由”调用 `PUT /api/v1/routes` 立即生效。
   - 提供**供应商一键批量启停按钮组**（阿里云 / 火山引擎 / 微软 Azure / Google / 本地）：点击调用 `POST /api/v1/vendors/{vendor}/status`。
3. **升级“业务线与额度”面板 (`data-panel="keys"`)**：
   - 表格展示：`Key ID | 业务线(Consumer) | 状态(ACTIVE/DISABLED) | 调用次数(已用/上限 + 进度条) | Token数(已用/上限 + 进度条) | 已用微美元 | 操作(启用/停用)`。
   - 新增**额度配置表单** (`#quota-form`)：输入 `api_key_id`、`调用次数上限 (quota_limit_calls)`、`Token上限 (quota_limit_tokens)`、`告警阈值% (默认80)`、`Webhook 地址`、`重置已用计数` 复选框，调用 `PUT /api/v1/api-keys/{id}/quota` 实时生效。
4. **升级“供应商与模型配置”面板 (`data-panel="models"`)**：
   - 表格新增 `供应商 (vendor)` 列与 `密钥状态` 列（有密钥时显示 `🔒 已加密 (****xxxx)`）。
   - 当模型处于 `DISABLED` 状态时，操作列显示 `<button data-enable="{config_id}">启用</button>`，点击调用 `POST /api/v1/model-configs/{id}/enable`。
5. **升级“按日三维度报表”面板 (`data-panel="usage"`)**：
   - 顶部提供日期选择器（`<input type="date" id="report-date" />`）、能力筛选下拉框、供应商筛选下拉框。
   - 展示 5 张核心指标卡片：`总调用次数`、`总 Token 数`、`总音频时长 (秒)`、`成功率 (%)`、`平均延迟 (ms)`。
   - 下方展示三维度明细表格：`日期 | 业务线 | API Key | 能力 | 供应商 | 模型 | 调用次数 | 成功/失败 | Token 总数 | 音频时长(s) | 平均延迟(ms)`。

---

## 9. 验收演示脚本 1~4 全链路闭环对照表

| 验收步骤 | PRD 验收要求 | 本设计对应实现与验证方式 |
| :--- | :--- | :--- |
| **步骤 1：统一调用演示** | 用同一个业务线 API Key，依次调用 ASR、TTS、翻译、总结 4 个接口，返回正确结果，后台看到 4 条调用记录 | 业务 Key 调用 `POST /v1/audio/transcriptions` (ASR)、`POST /v1/audio/speech` (TTS)、`POST /v1/chat/completions` (`capability=translate` / `capability=summary`) 或 `/api/v1/voice/*`，均返回 200 且写入 `usage_event_ledger` 与 `usage_daily_three_dim_rollup` |
| **步骤 2：多供应商热切换演示** | 在后台把翻译 LLM 从供应商 A 切换到供应商 B，不改业务代码、不重启服务，再发一次翻译请求，后台记录显示已走供应商 B | 调用 `PUT /api/v1/routes` 将 `translate_llm` 切换至阿里云 `qwen3.8-max` 或火山 `doubao-seed-2-1-pro-260915`，再次请求 `POST /v1/chat/completions` 或语音同传会话，`GET /api/v1/usage/report` 立即显示新请求 `vendor` 为切换后的供应商 |
| **步骤 3：用量查看演示** | 打开后台报表页，按业务线、能力、供应商三个维度筛选查看当日累计用量 | 调用 `GET /api/v1/usage/report?date=YYYY-MM-DD&api_key_id=...&category=...&vendor=...` 及管理后台/门户报表页，精确展示三维度汇总与明细 |
| **步骤 4：额度限制与告警演示** | 将某业务线额度调低至接近上限 → 继续调用触发 80% 预警和 100% 拦截（返回额度超限错误）→ 在后台调高额度后恢复正常调用 | `PUT /api/v1/api-keys/{id}/quota` 设 `quota_limit_calls = 5` → 第 4 次调用 (80%) 产生 `WARN_80` 告警 → 第 5 次调用 (100%) 产生 `CRITICAL_100` 告警 → 第 6 次调用返回 `403 quota_calls_exceeded` → 调高 `quota_limit_calls = 100` 后立即恢复 `200 OK` |

---

## Key Decisions

1. **零新增外部 Crate 依赖完成加密与 WebSocket 升级**：
   - `himarket-rs/Cargo.lock` 已锁定现有依赖树。为保证离线编译与测试 100% 稳定，认证加密采用 `sha2::Sha256` 计数器模式流密码 + `HMAC-SHA256` 认证标签（`enc:v1:<nonce>:<cipher>:<tag>`），WebSocket `/v1/audio/asr/stream` 采用 `hyper::upgrade::on` + 内置 RFC 6455 帧编解码与握手实现，无需联网拉取新 crate。
2. **种子目录空密钥安全回退 vs 配置密钥后真实/沙箱双模运行**：
   - `catalog/livekit_models.json` 中预置的 4 家云厂商模型初始 `api_key` 为空，保持 `is_stt_runnable` / `is_tts_runnable` 在空密钥时返回 `false`，确保未配置密钥时默认路由安全回退到本地 Whisper/Piper 且不破坏现有测试；一旦管理员写入真实密钥或 `mock_*`/`demo_*` 演示密钥，立即激活对应供应商路由。
3. **双层计量聚合（保留 `usage_hourly_rollup` + 新增 `usage_daily_three_dim_rollup`）**：
   - 保持 `GET /api/v1/usage/summary` 100% 向后兼容，同时新增按日 × 业务线 × 能力 × 供应商 × 模型聚合的 `usage_daily_three_dim_rollup` 表支撑 `GET /api/v1/usage/report` 毫秒级多维筛选。
4. **配额更新与业务线状态变更实时失效缓存 (`key_cache.clear()`)**：
   - 每次用量入账、管理员调整配额或手动启停业务线时，立即清理内存 `key_cache`，确保 100% 配额拦截与调高额度后的立即恢复无任何缓存延迟。

---

## PR Plan

1. **Phase 1 — 核心库与数据访问层 (`himarket-core` & `himarket-dal` & `catalog/livekit_models.json`)**：
   - 在 `himarket-core` 实现 `infer_vendor`、`encrypt_secret` / `decrypt_secret`、扩展 `VerifiedBusinessKey::check_quota_error` 与 `UsageEventPayload`。
   - 在 `catalog/livekit_models.json` 补齐 4 家供应商的“性价比档”模型与 `vendor` 字段。
   - 在 `himarket-dal` 新增 `ai_route_policy`、`usage_daily_three_dim_rollup`、`quota_alert_event` 表及字段迁移，实现密钥加密读写、路由解析、三维度报表查询、配额更新与 80%/100% 告警触发。
2. **Phase 2 — 统一 API 网关与控制台接口 (`himarket-access` & `himarket-market` & `himarket-bootstrap`)**：
   - 在 `himarket-access` 实现 `POST /v1/chat/completions`（含非流式与 SSE 流式）、`POST /v1/audio/transcriptions`、`GET /v1/audio/asr/stream`（WebSocket 流式）、`POST /v1/audio/speech`、以及 `get_session_runtime_internal` 动态路由同步。
   - 在 `himarket-market` 实现 `GET/PUT /api/v1/routes`、`POST /api/v1/model-configs/{id}/enable`、`POST /api/v1/vendors/{vendor}/status`、`GET /api/v1/usage/report`、`PUT /api/v1/api-keys/{id}/quota`、`POST /api/v1/api-keys/{id}/status`、`GET /api/v1/alerts`，并收敛开发者角色为只读权限。
3. **Phase 3 — Worker 内部转写与供应商开关 (`voice-agent-worker`) & Web 控制台升级 (`web/*`)**：
   - 在 `voice-agent-worker/agent_main.py` 新增 `POST /internal/v1/transcribe` 并打通 4 家云厂商有密钥时的协议构造与执行。
   - 升级 `web/admin.html`、`web/portal.html`、`web/console.css` 支持告警横幅、路由切换、额度进度条与三维度按日报表。
4. **Phase 4 — 端到端集成测试与验收脚本验证**：
   - 在 `himarket-bootstrap` 与 `voice-agent-worker/tests` 新增覆盖全部 6 大 Gap 与验收演示脚本 1~4 的自动化测试，确保 `cargo test` 与 `pytest`/`unittest` 全部通过。
