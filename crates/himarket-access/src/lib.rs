use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use himarket_core::{
    ApiResponse, AppError, CreateVoiceSessionRequest, CreateVoiceSessionResponse,
    TenantLanguagePolicy, UsageEventPayload, UsageMetricsPayload, VerifiedBusinessKey,
    ensure_allowed, extract_summary_title_and_body, infer_vendor, normalize_lang,
    split_speak_segments,
};
use himarket_dal::{
    AppendSummaryInputResult, BeginSummaryOutcome, DalRepo, ModelConfig, SessionRecordFull,
    SummaryInputRow, SummaryResultRow, TtsSelectError, format_summary_materials, is_llm_runnable,
    is_tts_runnable,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{info, warn};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatCompletionOutput {
    pub content: String,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
}

pub trait ChatClient: Send + Sync {
    fn complete<'a>(
        &'a self,
        binding: &'a ModelBinding,
        system_prompt: &'a str,
        user_content: &'a str,
        livekit_api_key: &'a str,
        livekit_api_secret: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ChatCompletionOutput, AppError>> + Send + 'a>>;
}

pub trait SynthesizeClient: Send + Sync {
    fn synthesize<'a>(
        &'a self,
        binding: &'a ModelBinding,
        text: &'a str,
        internal_secret: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, AppError>> + Send + 'a>>;
}

pub struct HttpChatClient {
    client: reqwest::Client,
    cliproxy_base_url: String,
    cliproxy_api_key: String,
}

impl HttpChatClient {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(25))
            .build()
            .expect("chat http client");
        let cliproxy_base_url = std::env::var("CLIPROXY_BASE_URL").unwrap_or_default();
        let cliproxy_api_key = std::env::var("CLIPROXY_API_KEY")
            .unwrap_or_else(|_| "cpa-internal-key".to_string());
        Self {
            client,
            cliproxy_base_url,
            cliproxy_api_key,
        }
    }
}

impl Default for HttpChatClient {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize)]
struct InferenceClaims {
    iss: String,
    sub: String,
    nbf: u64,
    exp: u64,
    inference: InferenceGrant,
}

#[derive(Serialize)]
struct InferenceGrant {
    perform: bool,
}

pub fn sign_inference_jwt(
    binding: &ModelBinding,
    default_key: &str,
    default_secret: &str,
) -> Result<String, AppError> {
    let key = if !binding.api_key.trim().is_empty() {
        binding.api_key.trim()
    } else {
        default_key
    };
    let secret = if !binding.api_secret.trim().is_empty() {
        binding.api_secret.trim()
    } else {
        default_secret
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let claims = InferenceClaims {
        iss: key.to_string(),
        sub: "himarket-voice-agent".to_string(),
        nbf: now.saturating_sub(5),
        exp: now + 600,
        inference: InferenceGrant { perform: true },
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| AppError::Internal(e.to_string()))
}

impl ChatClient for HttpChatClient {
    fn complete<'a>(
        &'a self,
        binding: &'a ModelBinding,
        system_prompt: &'a str,
        user_content: &'a str,
        livekit_api_key: &'a str,
        livekit_api_secret: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ChatCompletionOutput, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let token = match binding.protocol.as_str() {
                "openai_chat" => binding.api_key.trim().to_string(),
                "livekit_inference" => {
                    sign_inference_jwt(binding, livekit_api_key, livekit_api_secret)?
                }
                other => {
                    return Err(AppError::bad_request_code(
                        "llm_protocol_unsupported",
                        format!("protocol {} does not support chat completions", other),
                    ));
                }
            };
            if binding.api_key.trim().starts_with("mock_")
                || binding.api_key.trim().starts_with("demo_")
            {
                let vendor = infer_vendor(&binding.protocol, &binding.model, &binding.base_url);
                let content = if system_prompt.contains("只输出译文") {
                    format!("[{}·{}] Translated: {}", vendor, binding.model, user_content)
                } else if system_prompt.contains("你是摘要器") {
                    format!(
                        "# {}智能总结纪要\n\n## 核心摘要\n由 {} ({}) 生成的结构化总结：{}\n\n## 核心要点\n- 要点一：已完成多供应商热切换与统一网关验证",
                        vendor.to_uppercase(),
                        vendor,
                        binding.model,
                        user_content.chars().take(120).collect::<String>()
                    )
                } else {
                    format!("[{}·{}] 已收到：{}", vendor, binding.model, user_content)
                };
                return Ok(ChatCompletionOutput {
                    content,
                    prompt_tokens: Some(48),
                    completion_tokens: Some(32),
                    cached_tokens: Some(0),
                });
            }
            let use_cliproxy =
                binding.protocol == "openai_chat" && !self.cliproxy_base_url.trim().is_empty();
            let base = if use_cliproxy {
                self.cliproxy_base_url.trim().trim_end_matches('/')
            } else {
                binding.base_url.trim().trim_end_matches('/')
            };
            if base.is_empty() {
                return Err(AppError::bad_gateway_code(
                    "llm_failed",
                    "LLM base_url is empty",
                ));
            }
            let auth_token = if use_cliproxy {
                self.cliproxy_api_key.trim()
            } else {
                token.as_str()
            };
            let url = format!("{}/chat/completions", base);
            let body = serde_json::json!({
                "model": binding.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                "temperature": 0.0,
                "max_tokens": 2048,
            });
            let mut req = self
                .client
                .post(&url)
                .header("X-HiMarket-Upstream-Base-Url", binding.base_url.trim())
                .json(&body);
            if !auth_token.is_empty() {
                req = req.header("Authorization", format!("Bearer {}", auth_token));
            }
            let resp = req
                .send()
                .await
                .map_err(|e| AppError::bad_gateway_code("llm_failed", e.to_string()))?;
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                return Err(AppError::bad_gateway_code(
                    "llm_failed",
                    format!("upstream status {}: {}", status, text),
                ));
            }
            let data: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| AppError::bad_gateway_code("llm_failed", e.to_string()))?;
            let content = data["choices"][0]["message"]["content"]
                .as_str()
                .unwrap_or("")
                .trim()
                .to_string();
            if content.is_empty() {
                return Err(AppError::bad_gateway_code(
                    "llm_empty",
                    "model returned empty content",
                ));
            }
            let usage = &data["usage"];
            let prompt_tokens = usage["prompt_tokens"].as_i64();
            let completion_tokens = usage["completion_tokens"].as_i64();
            let cached_tokens = usage["cached_tokens"]
                .as_i64()
                .or_else(|| usage["prompt_tokens_details"]["cached_tokens"].as_i64());
            Ok(ChatCompletionOutput {
                content,
                prompt_tokens,
                completion_tokens,
                cached_tokens,
            })
        })
    }
}

pub struct HttpSynthesizeClient {
    client: reqwest::Client,
    tts_url: String,
}

impl HttpSynthesizeClient {
    pub fn new(tts_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("tts http client");
        Self { client, tts_url }
    }
}

impl SynthesizeClient for HttpSynthesizeClient {
    fn synthesize<'a>(
        &'a self,
        binding: &'a ModelBinding,
        text: &'a str,
        internal_secret: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let runnable = match binding.protocol.as_str() {
                "piper_local" => true,
                "openai_tts" | "dashscope_tts" | "volcengine_tts" | "azure_tts" | "google_tts" => {
                    !binding.api_key.trim().is_empty()
                }
                "livekit_inference" => {
                    !binding.api_key.trim().is_empty() && !binding.api_secret.trim().is_empty()
                }
                _ => false,
            };
            if !runnable {
                return Err(AppError::bad_request_code(
                    "tts_protocol_unsupported",
                    format!("protocol {} is not runnable for synthesis", binding.protocol),
                ));
            }
            let url = self.tts_url.trim();
            if url.is_empty() {
                if binding.api_key.trim().starts_with("mock_")
                    || binding.api_key.trim().starts_with("demo_")
                {
                    return Ok(build_demo_wav_bytes(24000, 400));
                }
                return Err(AppError::bad_gateway_code(
                    "tts_failed",
                    "HIMARKET_TTS_URL is not configured",
                ));
            }
            let resp = self
                .client
                .post(url)
                .header("X-HiMarket-Internal-Secret", internal_secret)
                .json(&serde_json::json!({
                    "text": text,
                    "binding": binding,
                }))
                .send()
                .await
                .map_err(|e| AppError::bad_gateway_code("tts_failed", e.to_string()))?;
            if !resp.status().is_success() {
                let status = resp.status();
                let err_txt = resp.text().await.unwrap_or_default();
                return Err(AppError::bad_gateway_code(
                    "tts_failed",
                    format!("worker synthesize status {}: {}", status, err_txt),
                ));
            }
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| AppError::bad_gateway_code("tts_failed", e.to_string()))?
                .to_vec();
            if bytes.is_empty() {
                return Err(AppError::bad_gateway_code(
                    "tts_failed",
                    "worker returned empty audio",
                ));
            }
            Ok(bytes)
        })
    }
}

pub fn translation_system_prompt(target_lang: &str) -> String {
    format!("把下面的文字译成 {}，只输出译文", target_lang)
}

pub fn summary_system_prompt(target_lang: &str, summary_style: &str) -> String {
    let sections = match summary_style {
        "meeting" => "## 核心摘要\n## 讨论要点\n## 待办行动项",
        "concise" => "## 核心摘要\n## 精简要点",
        _ => "## 核心摘要\n## 核心要点\n## 演讲结构概述\n### 1. 开场与背景\n### 2. 核心论点\n### 3. 总结与展望",
    };
    format!(
        "你是摘要器。下面按 seq 升序给出材料。请用 {} 写一段结构化总结，不要逐条翻译。第一行必须输出 `# 标题`（8到22个字），随后依次输出：\n{}",
        target_lang, sections
    )
}

#[derive(Clone)]
pub struct AccessState {
    pub repo: DalRepo,
    pub livekit_api_key: String,
    pub livekit_api_secret: String,
    pub livekit_public_ws_url: String,
    pub livekit_internal_http_url: String,
    pub internal_secret: String,
    pub http_client: reqwest::Client,
    pub chat_client: Arc<dyn ChatClient>,
    pub synth_client: Arc<dyn SynthesizeClient>,
    pub strip_jwt_secrets: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct VideoGrant {
    #[serde(rename = "roomJoin")]
    room_join: bool,
    room: String,
    #[serde(rename = "canPublish")]
    can_publish: bool,
    #[serde(rename = "canSubscribe")]
    can_subscribe: bool,
    #[serde(rename = "canPublishData")]
    can_publish_data: bool,
    #[serde(rename = "canUpdateOwnMetadata")]
    can_update_own_metadata: bool,
    #[serde(rename = "roomAdmin", skip_serializing_if = "Option::is_none")]
    room_admin: Option<bool>,
    #[serde(rename = "roomCreate", skip_serializing_if = "Option::is_none")]
    room_create: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomAgentDispatchClaim {
    #[serde(rename = "agentName")]
    agent_name: String,
    metadata: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomConfigurationClaim {
    agents: Vec<RoomAgentDispatchClaim>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LiveKitClaims {
    iss: String,
    sub: String,
    name: String,
    nbf: u64,
    exp: u64,
    metadata: String,
    attributes: HashMap<String, String>,
    video: VideoGrant,
    #[serde(rename = "roomConfig", skip_serializing_if = "Option::is_none")]
    room_config: Option<RoomConfigurationClaim>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelBinding {
    pub config_id: String,
    pub protocol: String,
    pub base_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub extra_json: String,
    pub model: String,
    pub voice: String,
}

impl From<ModelConfig> for ModelBinding {
    fn from(config: ModelConfig) -> Self {
        Self {
            config_id: config.config_id,
            protocol: config.protocol,
            base_url: config.base_url,
            api_key: config.api_key,
            api_secret: config.api_secret,
            extra_json: config.extra_json,
            model: config.model,
            voice: config.voice,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentJobMetadata {
    pub session_id: String,
    pub tenant_id: String,
    pub api_key_id: String,
    pub consumer_id: String,
    pub mode: String,
    pub task: String,
    pub source_lang: String,
    pub target_lang: String,
    pub speak: bool,
    pub voice_fallback: bool,
    #[serde(default)]
    pub stt_fallback: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stt_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tts_voice: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm: Option<ModelBinding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stt: Option<ModelBinding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tts: Option<ModelBinding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub greeting_text: Option<String>,
}

pub fn access_router(state: AccessState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(v1_chat_completions))
        .route("/v1/audio/transcriptions", post(v1_audio_transcriptions))
        .route(
            "/v1/audio/asr/stream",
            get(v1_audio_asr_stream).post(v1_audio_asr_stream_post),
        )
        .route("/v1/audio/speech", post(v1_audio_speech))
        .route("/api/v1/voice/sessions", post(create_voice_session))
        .route("/api/v1/voice/chat", post(create_voice_chat_turn))
        .route("/api/v1/voice/language-policy", get(get_voice_language_policy))
        .route("/api/v1/voice/translations", post(create_text_translation))
        .route(
            "/api/v1/voice/sessions/{session_id}/inputs",
            post(append_session_input_http),
        )
        .route(
            "/api/v1/voice/sessions/{session_id}/summaries",
            post(summarize_session_http),
        )
        .route("/api/v1/voice/summaries", post(summarize_inline_http))
        .route(
            "/api/v1/voice/sessions/{session_id}/summaries/{request_id}/audio/{index}",
            get(get_summary_audio_segment),
        )
        .route(
            "/api/v1/voice/sessions/{session_id}/target-lang",
            post(update_session_target_lang_http),
        )
        .route("/api/v1/voice/speak", post(create_speak_http))
        .route(
            "/api/v1/voice/speak/{session_id}/segments/{index}",
            get(get_speak_segment_http),
        )
        .route(
            "/internal/v1/voice/sessions/{session_id}/runtime",
            get(get_session_runtime_internal),
        )
        .route(
            "/internal/v1/voice/sessions/{session_id}/target-lang",
            post(update_session_target_lang_internal),
        )
        .route(
            "/internal/v1/voice/sessions/{session_id}/tts",
            post(bind_session_tts_internal),
        )
        .route(
            "/internal/v1/voice/sessions/{session_id}/summary-inputs",
            post(append_summary_input_internal).get(list_summary_inputs_internal),
        )
        .route(
            "/internal/v1/voice/sessions/{session_id}/summary-results/begin",
            post(begin_summary_internal),
        )
        .route(
            "/internal/v1/voice/sessions/{session_id}/summary-results/finish",
            post(finish_summary_internal),
        )
        .route(
            "/internal/v1/voice/sessions/{session_id}/summary-results/fail",
            post(fail_summary_internal),
        )
        .route(
            "/internal/v1/voice/sessions/{session_id}/summaries",
            post(execute_summary_internal),
        )
        .with_state(state)
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    if let Some(val) = headers.get("Authorization").and_then(|v| v.to_str().ok()) {
        let stripped = val.strip_prefix("Bearer ").unwrap_or(val).trim();
        if !stripped.is_empty() {
            return Some(stripped.to_string());
        }
    }
    if let Some(val) = headers.get("X-Api-Key").and_then(|v| v.to_str().ok()) {
        let trimmed = val.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

async fn require_business_key(
    state: &AccessState,
    headers: &HeaderMap,
) -> Result<VerifiedBusinessKey, AppError> {
    let raw_key = extract_bearer_token(headers)
        .ok_or_else(|| AppError::Unauthorized("Missing HiMarket business API key".to_string()))?;

    let verified = state
        .repo
        .verify_business_key(&raw_key)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::Unauthorized("Invalid HiMarket business API key".to_string()))?;

    if let Err(reason) = verified.check_quota_error() {
        return Err(AppError::Forbidden(reason));
    }
    Ok(verified)
}

pub fn verify_internal_secret_strict(
    headers: &HeaderMap,
    expected: &str,
) -> Result<(), AppError> {
    let exp = expected.trim();
    if exp.is_empty() {
        return Err(AppError::Unauthorized(
            "HIMARKET_INTERNAL_SECRET is not configured".to_string(),
        ));
    }
    let got = headers
        .get("X-HiMarket-Internal-Secret")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if got.is_empty() || got != exp {
        return Err(AppError::Unauthorized(
            "Invalid X-HiMarket-Internal-Secret".to_string(),
        ));
    }
    Ok(())
}

async fn require_tenant_policy(
    repo: &DalRepo,
    tenant_id: &str,
) -> Result<TenantLanguagePolicy, AppError> {
    repo.get_language_policy(tenant_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| {
            AppError::bad_request_code(
                "language_policy_missing",
                format!("tenant {} has no language policy", tenant_id),
            )
        })
}

fn map_dal_err(err: anyhow::Error) -> AppError {
    let msg = err.to_string();
    for code in [
        "empty_input",
        "summary_limit",
        "target_lang_not_allowed",
        "language_policy_missing",
        "tts_unavailable",
        "tts_protocol_unsupported",
        "llm_protocol_unsupported",
        "speak_text_too_long",
        "use_text_endpoint",
        "task_rejected",
    ] {
        if msg.starts_with(code) {
            return AppError::BadRequest(msg);
        }
    }
    if msg.contains("not found") {
        return AppError::NotFound(msg);
    }
    AppError::Internal(msg)
}

async fn get_voice_language_policy(
    State(state): State<AccessState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<TenantLanguagePolicy>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let policy = require_tenant_policy(&state.repo, &verified.tenant_id).await?;
    Ok(ApiResponse::ok(policy))
}

async fn create_voice_session(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Json(req): Json<CreateVoiceSessionRequest>,
) -> Result<Json<ApiResponse<CreateVoiceSessionResponse>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let tenant_id = verified.tenant_id.as_str();

    let task = if req.task.trim().is_empty() {
        "cascade".to_string()
    } else {
        req.task.trim().to_string()
    };

    if task == "text_translate" {
        return Err(AppError::bad_request_code(
            "use_text_endpoint",
            "text_translate must use POST /api/v1/voice/translations",
        ));
    }
    if !matches!(
        task.as_str(),
        "cascade" | "transcribe" | "speech_translate" | "speak" | "context"
    ) {
        return Err(AppError::bad_request_code(
            "task_rejected",
            format!("unsupported session task {}", task),
        ));
    }
    if req.mode == "realtime" && task != "cascade" {
        return Err(AppError::bad_request_code(
            "task_rejected",
            "realtime mode only supports cascade task",
        ));
    }

    let source_lang = if req.source_lang.trim().is_empty() {
        String::new()
    } else {
        normalize_lang(&req.source_lang)
            .map_err(|e| AppError::bad_request_code("target_lang_not_allowed", e))?
    };

    let target_lang = match task.as_str() {
        "speech_translate" | "speak" => {
            let policy = require_tenant_policy(&state.repo, tenant_id).await?;
            ensure_allowed(Some(&req.target_lang), &policy)
                .map_err(AppError::BadRequest)?
        }
        "transcribe" | "context" => {
            if req.target_lang.trim().is_empty() {
                String::new()
            } else {
                let policy = require_tenant_policy(&state.repo, tenant_id).await?;
                ensure_allowed(Some(&req.target_lang), &policy)
                    .map_err(AppError::BadRequest)?
            }
        }
        _ => {
            if req.target_lang.trim().is_empty() {
                String::new()
            } else {
                normalize_lang(&req.target_lang)
                    .map_err(|e| AppError::bad_request_code("target_lang_not_allowed", e))?
            }
        }
    };

    let session_id = format!("sess_{}", uuid::Uuid::now_v7().simple());

    if task == "context" {
        state
            .repo
            .insert_session_record(
                &session_id,
                None,
                &verified,
                &req.mode,
                "context",
                &source_lang,
                &target_lang,
                false,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;

        return Ok(ApiResponse::ok(CreateVoiceSessionResponse {
            session_id,
            room_name: None,
            livekit_ws_url: None,
            participant_token: None,
            expires_in_seconds: 0,
            tenant_id: verified.tenant_id,
            api_key_id: verified.api_key_id,
            mode: req.mode,
            task,
            llm_model: None,
            source_lang,
            target_lang,
            speak: false,
            voice_fallback: false,
            stt_fallback: false,
        }));
    }

    let mut stt_cfg: Option<ModelConfig> = None;
    let mut llm_cfg: Option<ModelConfig> = None;
    let mut tts_cfg: Option<ModelConfig> = None;
    let mut voice_fallback = false;
    let mut stt_fallback = false;

    match task.as_str() {
        "cascade" => {
            if req.mode == "realtime" {
                let rt_model = state
                    .repo
                    .resolve_route_config(tenant_id, "realtime", None, req.llm_model.as_deref())
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?
                    .ok_or_else(|| {
                        AppError::bad_request_code(
                            "llm_unavailable",
                            "当前租户没有可用的 Realtime 模型配置",
                        )
                    })?;
                llm_cfg = Some(rt_model);
            } else {
                let llm = state
                    .repo
                    .resolve_model_config(tenant_id, "llm", req.llm_model.as_deref())
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?
                    .ok_or_else(|| {
                        AppError::bad_request_code(
                            "llm_unavailable",
                            "当前租户没有可用的 LLM 配置",
                        )
                    })?;
                let stt = state
                    .repo
                    .resolve_model_config(tenant_id, "stt", req.stt_model.as_deref())
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?
                    .ok_or_else(|| {
                        AppError::bad_request_code(
                            "stt_unavailable",
                            "当前租户没有可用的 STT 配置",
                        )
                    })?;
                let tts = state
                    .repo
                    .resolve_model_config(tenant_id, "tts", req.tts_voice.as_deref())
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?
                    .ok_or_else(|| {
                        AppError::bad_request_code(
                            "tts_unavailable",
                            "当前租户没有可用的 TTS 配置",
                        )
                    })?;
                stt_cfg = Some(stt);
                llm_cfg = Some(llm);
                tts_cfg = Some(tts);
            }
        }
        "transcribe" => {
            let sel = state
                .repo
                .select_stt_for_lang(
                    tenant_id,
                    if source_lang.is_empty() {
                        None
                    } else {
                        Some(source_lang.as_str())
                    },
                    req.stt_model.as_deref(),
                )
                .await
                .map_err(|_| {
                    AppError::bad_request_code(
                        "stt_unavailable",
                        "当前租户没有可用的 STT 配置",
                    )
                })?;
            stt_fallback = sel.stt_fallback;
            stt_cfg = Some(sel.config);
        }
        "speech_translate" => {
            let sel_stt = state
                .repo
                .select_stt_for_lang(
                    tenant_id,
                    if source_lang.is_empty() {
                        None
                    } else {
                        Some(source_lang.as_str())
                    },
                    req.stt_model.as_deref(),
                )
                .await
                .map_err(|_| {
                    AppError::bad_request_code(
                        "stt_unavailable",
                        "当前租户没有可用的 STT 配置",
                    )
                })?;
            stt_fallback = sel_stt.stt_fallback;
            stt_cfg = Some(sel_stt.config);

            let llm = state
                .repo
                .resolve_text_model(tenant_id, "translate", req.llm_model.as_deref())
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    AppError::bad_request_code(
                        "llm_unavailable",
                        "当前租户没有可用的 LLM 配置",
                    )
                })?;
            if !is_llm_runnable(&llm) {
                return Err(AppError::bad_request_code(
                    "llm_protocol_unsupported",
                    format!("LLM protocol {} is not runnable", llm.protocol),
                ));
            }
            llm_cfg = Some(llm);

            if req.speak {
                let sel_tts = state
                    .repo
                    .select_tts_for_lang(tenant_id, &target_lang)
                    .await
                    .map_err(|_| {
                        AppError::bad_request_code(
                            "tts_unavailable",
                            "当前租户没有可用的 TTS 配置",
                        )
                    })?;
                voice_fallback = sel_tts.voice_fallback;
                tts_cfg = Some(sel_tts.config);
            }
        }
        "speak" => {
            let sel_tts = state
                .repo
                .select_tts_for_lang(tenant_id, &target_lang)
                .await
                .map_err(|_| {
                    AppError::bad_request_code(
                        "tts_unavailable",
                        "当前租户没有可用的 TTS 配置",
                    )
                })?;
            voice_fallback = sel_tts.voice_fallback;
            tts_cfg = Some(sel_tts.config);
        }
        _ => unreachable!(),
    }

    let room_name = format!("lk_rm_{}", session_id);
    let effective_speak = match task.as_str() {
        "cascade" | "speak" => true,
        "speech_translate" => req.speak,
        _ => false,
    };

    state
        .repo
        .insert_session_record(
            &session_id,
            Some(&room_name),
            &verified,
            &req.mode,
            &task,
            &source_lang,
            &target_lang,
            effective_speak,
            stt_cfg.as_ref().map(|c| c.config_id.as_str()),
            llm_cfg.as_ref().map(|c| c.config_id.as_str()),
            tts_cfg.as_ref().map(|c| c.config_id.as_str()),
            llm_cfg.as_ref().map(|c| c.model.as_str()),
            None,
        )
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let llm_binding = llm_cfg.map(ModelBinding::from);
    let stt_binding = stt_cfg.map(ModelBinding::from);
    let tts_binding = tts_cfg.map(ModelBinding::from);
    let llm_model_name = llm_binding.as_ref().map(|b| b.model.clone());
    let stt_model_name = stt_binding.as_ref().map(|b| b.model.clone());
    let tts_voice_name = tts_binding.as_ref().map(|b| b.voice.clone());

    let twirp_job_meta = AgentJobMetadata {
        session_id: session_id.clone(),
        tenant_id: verified.tenant_id.clone(),
        api_key_id: verified.api_key_id.clone(),
        consumer_id: verified.consumer_id.clone(),
        mode: req.mode.clone(),
        task: task.clone(),
        source_lang: source_lang.clone(),
        target_lang: target_lang.clone(),
        speak: effective_speak,
        voice_fallback,
        stt_fallback,
        llm_model: llm_model_name.clone(),
        stt_model: stt_model_name,
        tts_voice: tts_voice_name,
        llm: llm_binding,
        stt: stt_binding,
        tts: tts_binding,
        system_prompt: req.system_prompt.clone(),
        greeting_text: req.greeting_text.clone(),
    };
    let twirp_meta_json =
        serde_json::to_string(&twirp_job_meta).map_err(|e| AppError::Internal(e.to_string()))?;

    let jwt_meta_json = if state.strip_jwt_secrets {
        let mut public_meta = twirp_job_meta.clone();
        public_meta.llm = None;
        public_meta.stt = None;
        public_meta.tts = None;
        serde_json::to_string(&public_meta).map_err(|e| AppError::Internal(e.to_string()))?
    } else {
        twirp_meta_json.clone()
    };

    if let Err(err) = dispatch_agent_via_twirp(&state, &room_name, &twirp_meta_json).await {
        warn!(
            "LiveKit explicit Twirp dispatch skipped or deferred to roomConfig: {}",
            err
        );
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut attrs = HashMap::new();
    attrs.insert("session_id".to_string(), session_id.clone());
    attrs.insert("tenant_id".to_string(), verified.tenant_id.clone());
    attrs.insert("api_key_id".to_string(), verified.api_key_id.clone());
    attrs.insert("mode".to_string(), req.mode.clone());
    attrs.insert("task".to_string(), task.clone());
    attrs.insert("target_lang".to_string(), target_lang.clone());
    if let Some(m) = &llm_model_name {
        attrs.insert("llm_model".to_string(), m.clone());
    }

    let claims = LiveKitClaims {
        iss: state.livekit_api_key.clone(),
        sub: format!("android_{}", verified.consumer_id),
        name: "Android Voice Client".to_string(),
        nbf: now.saturating_sub(10),
        exp: now + 900,
        metadata: jwt_meta_json.clone(),
        attributes: attrs,
        video: VideoGrant {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_update_own_metadata: false,
            room_admin: None,
            room_create: None,
        },
        room_config: Some(RoomConfigurationClaim {
            agents: vec![RoomAgentDispatchClaim {
                agent_name: "himarket-voice-agent".to_string(),
                metadata: jwt_meta_json,
            }],
        }),
    };

    let participant_token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(state.livekit_api_secret.as_bytes()),
    )
    .map_err(|e| AppError::Internal(e.to_string()))?;

    info!(
        session_id = %session_id,
        tenant_id = %verified.tenant_id,
        task = %task,
        target_lang = %target_lang,
        voice_fallback = voice_fallback,
        "voice_session_created"
    );

    Ok(ApiResponse::ok(CreateVoiceSessionResponse {
        session_id,
        room_name: Some(room_name),
        livekit_ws_url: Some(state.livekit_public_ws_url.clone()),
        participant_token: Some(participant_token),
        expires_in_seconds: 900,
        tenant_id: verified.tenant_id,
        api_key_id: verified.api_key_id,
        mode: req.mode,
        task,
        llm_model: llm_model_name,
        source_lang,
        target_lang,
        speak: effective_speak,
        voice_fallback,
        stt_fallback,
    }))
}

#[derive(Debug, Deserialize)]
pub struct VoiceChatRequest {
    pub text: String,
    pub session_id: Option<String>,
    #[serde(default)]
    pub persona_id: String,
    pub system_prompt: Option<String>,
    pub llm_model: Option<String>,
    #[serde(default)]
    pub client_input_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VoiceChatResponse {
    pub session_id: String,
    pub seq: i64,
    pub persona_id: String,
    pub user_text: String,
    pub reply_text: String,
    pub model: String,
}

fn persona_system_prompt(persona_id: &str, custom: Option<&str>) -> String {
    if let Some(c) = custom.map(str::trim).filter(|s| !s.is_empty()) {
        return c.to_string();
    }
    match persona_id.trim() {
        "muyang" => "你是「沐阳」，一位温和理性的职场伙伴（男声·温和理性）。请用自然、真诚、共情且有条理的口吻与用户交流，每次回复控制在2到3句话，不要使用Markdown符号。".to_string(),
        "xinglan" => "你是「星澜」，一位治愈轻柔的心灵树洞陪伴者（女声·治愈轻柔）。请用温暖、接纳、舒缓的口吻倾听和回应用户的情绪，每次回复控制在2到3句话，不要使用Markdown符号。".to_string(),
        _ => "你是「知秋」，一位沉稳舒缓的情感导师（女声·沉稳舒缓）。请先共情用户的感受，再用温柔、清晰的引导帮用户慢慢理清思绪或关系，每次回复控制在2到3句话，不要使用Markdown符号。".to_string(),
    }
}

async fn create_voice_chat_turn(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Json(req): Json<VoiceChatRequest>,
) -> Result<Json<ApiResponse<VoiceChatResponse>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let tenant_id = verified.tenant_id.as_str();

    let text = req.text.trim();
    if text.is_empty() {
        return Err(AppError::bad_request_code(
            "empty_input",
            "chat text cannot be empty",
        ));
    }
    if text.chars().count() > 4000 {
        return Err(AppError::bad_request_code(
            "speak_text_too_long",
            "chat text exceeds 4000 characters",
        ));
    }

    let persona_id = if req.persona_id.trim().is_empty() {
        "zhiqiu".to_string()
    } else {
        req.persona_id.trim().to_string()
    };

    let llm_cfg = state
        .repo
        .resolve_model_config(tenant_id, "llm", req.llm_model.as_deref())
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| {
            AppError::bad_request_code("llm_unavailable", "当前租户没有可用的 LLM 配置")
        })?;
    if !is_llm_runnable(&llm_cfg) {
        return Err(AppError::bad_request_code(
            "llm_protocol_unsupported",
            format!("protocol {} is not supported", llm_cfg.protocol),
        ));
    }

    let session_id = if let Some(existing_id) = req
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let sess = state
            .repo
            .get_session_record(existing_id)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?
            .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
        if sess.tenant_id != tenant_id {
            return Err(AppError::NotFound("session not found".to_string()));
        }
        existing_id.to_string()
    } else {
        let new_id = format!("sess_chat_{}", uuid::Uuid::now_v7().simple());
        state
            .repo
            .insert_session_record(
                &new_id,
                None,
                &verified,
                "cascade",
                "cascade",
                "zh",
                "zh",
                true,
                None,
                Some(&llm_cfg.config_id),
                None,
                Some(&llm_cfg.model),
                None,
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        new_id
    };

    let history_rows = state
        .repo
        .list_summary_inputs(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let recent_start = history_rows.len().saturating_sub(8);
    let mut prompt_with_context = String::new();
    for r in &history_rows[recent_start..] {
        if let Some(u) = r.source_text.as_deref().filter(|s| !s.is_empty()) {
            prompt_with_context.push_str(&format!("用户：{}\n", u));
        }
        if let Some(a) = r.assistant_text.as_deref().filter(|s| !s.is_empty()) {
            prompt_with_context.push_str(&format!("助手：{}\n", a));
        }
    }
    prompt_with_context.push_str(&format!("用户：{}", text));

    let sys_prompt = persona_system_prompt(&persona_id, req.system_prompt.as_deref());
    let binding = ModelBinding::from(llm_cfg.clone());
    let out = state
        .chat_client
        .complete(
            &binding,
            &sys_prompt,
            &prompt_with_context,
            &state.livekit_api_key,
            &state.livekit_api_secret,
        )
        .await?;

    let cid = if req.client_input_id.trim().is_empty() {
        format!("chat_{}", uuid::Uuid::now_v7().simple())
    } else {
        req.client_input_id.trim().to_string()
    };

    let appended = state
        .repo
        .append_summary_input(
            &session_id,
            "cascade",
            Some(text),
            None,
            Some(&out.content),
            &cid,
        )
        .await
        .map_err(map_dal_err)?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let ev = UsageEventPayload {
        event_id: format!("ev_chat_{}", uuid::Uuid::now_v7().simple()),
        idempotency_key: format!("api_chat:{}:{}", session_id, cid),
        session_id: session_id.clone(),
        room_name: None,
        tenant_id: verified.tenant_id.clone(),
        api_key_id: verified.api_key_id.clone(),
        source: "himarket_access".to_string(),
        category: "text_llm".to_string(),
        model: llm_cfg.model.clone(),
        vendor: String::new(),
        status: "SUCCESS".to_string(),
        occurred_at_ms: now_ms,
        metrics: UsageMetricsPayload {
            prompt_tokens: out
                .prompt_tokens
                .or(Some(prompt_with_context.chars().count() as i64)),
            completion_tokens: out
                .completion_tokens
                .or(Some(out.content.chars().count() as i64)),
            cached_tokens: out.cached_tokens,
            ..Default::default()
        },
    };
    let _ = state
        .repo
        .ingest_usage_event(ev)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(ApiResponse::ok(VoiceChatResponse {
        session_id,
        seq: appended.seq,
        persona_id,
        user_text: text.to_string(),
        reply_text: out.content,
        model: llm_cfg.model,
    }))
}

#[derive(Debug, Deserialize)]
pub struct TextTranslateRequest {
    pub text: String,
    #[serde(default)]
    pub source_lang: String,
    #[serde(default)]
    pub target_lang: String,
    pub llm_model: Option<String>,
    #[serde(default)]
    pub client_input_id: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextTranslateResponse {
    pub session_id: String,
    pub seq: i64,
    pub source_text: String,
    pub translated_text: String,
    pub source_lang: String,
    pub target_lang: String,
    pub model: String,
}

async fn create_text_translation(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Json(req): Json<TextTranslateRequest>,
) -> Result<Json<ApiResponse<TextTranslateResponse>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let tenant_id = verified.tenant_id.as_str();

    let text = req.text.trim();
    if text.is_empty() {
        return Err(AppError::bad_request_code(
            "empty_input",
            "translation text cannot be empty",
        ));
    }
    let char_count = text.chars().count();
    if char_count > 4000 {
        return Err(AppError::bad_request_code(
            "speak_text_too_long",
            format!("translation text length {} exceeds 4000 limit", char_count),
        ));
    }
    let cid = req.client_input_id.trim();
    if cid.is_empty() {
        return Err(AppError::bad_request_code(
            "empty_input",
            "client_input_id is required for text translation",
        ));
    }
    if cid.chars().count() > 64 {
        return Err(AppError::bad_request_code(
            "summary_limit",
            "client_input_id exceeds 64 characters",
        ));
    }

    let source_lang = if req.source_lang.trim().is_empty() {
        String::new()
    } else {
        normalize_lang(&req.source_lang)
            .map_err(|e| AppError::bad_request_code("target_lang_not_allowed", e))?
    };

    let policy = require_tenant_policy(&state.repo, tenant_id).await?;
    let target_lang =
        ensure_allowed(Some(&req.target_lang), &policy).map_err(AppError::BadRequest)?;

    let llm_cfg = state
        .repo
        .resolve_text_model(tenant_id, "translate", req.llm_model.as_deref())
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| {
            AppError::bad_request_code("llm_unavailable", "当前租户没有可用的 LLM 配置")
        })?;
    if !is_llm_runnable(&llm_cfg) {
        return Err(AppError::bad_request_code(
            "llm_protocol_unsupported",
            format!("protocol {} is not supported", llm_cfg.protocol),
        ));
    }

    let session_id = if let Some(existing_id) = req
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let sess = state
            .repo
            .get_session_record(existing_id)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?
            .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
        if sess.tenant_id != tenant_id {
            return Err(AppError::NotFound("session not found".to_string()));
        }
        let inputs = state
            .repo
            .list_summary_inputs(existing_id)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        if let Some(hit) = inputs.into_iter().find(|r| r.client_input_id == cid) {
            if let Some(existing_tr) = hit.translated_text.filter(|s| !s.is_empty()) {
                return Ok(ApiResponse::ok(TextTranslateResponse {
                    session_id: existing_id.to_string(),
                    seq: hit.seq,
                    source_text: hit.source_text.unwrap_or_else(|| text.to_string()),
                    translated_text: existing_tr,
                    source_lang,
                    target_lang,
                    model: llm_cfg.model,
                }));
            }
        }
        existing_id.to_string()
    } else {
        let new_id = format!("sess_{}", uuid::Uuid::now_v7().simple());
        state
            .repo
            .insert_session_record(
                &new_id,
                None,
                &verified,
                "cascade",
                "text_translate",
                &source_lang,
                &target_lang,
                false,
                None,
                Some(&llm_cfg.config_id),
                None,
                Some(&llm_cfg.model),
                None,
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        new_id
    };

    let binding = ModelBinding::from(llm_cfg.clone());
    let sys_prompt = translation_system_prompt(&target_lang);
    let out = state
        .chat_client
        .complete(
            &binding,
            &sys_prompt,
            text,
            &state.livekit_api_key,
            &state.livekit_api_secret,
        )
        .await?;

    let appended = state
        .repo
        .append_summary_input(
            &session_id,
            "translate",
            Some(text),
            Some(&out.content),
            None,
            cid,
        )
        .await
        .map_err(map_dal_err)?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let ev = UsageEventPayload {
        event_id: format!("ev_tr_{}", uuid::Uuid::now_v7().simple()),
        idempotency_key: format!("api_translate:{}:{}", session_id, cid),
        session_id: session_id.clone(),
        room_name: None,
        tenant_id: verified.tenant_id.clone(),
        api_key_id: verified.api_key_id.clone(),
        source: "himarket_access".to_string(),
        category: "translate".to_string(),
        model: llm_cfg.model.clone(),
        vendor: String::new(),
        status: "SUCCESS".to_string(),
        occurred_at_ms: now_ms,
        metrics: UsageMetricsPayload {
            prompt_tokens: out.prompt_tokens.or(Some(text.chars().count() as i64)),
            completion_tokens: out
                .completion_tokens
                .or(Some(out.content.chars().count() as i64)),
            cached_tokens: out.cached_tokens,
            ..Default::default()
        },
    };
    let _ = state
        .repo
        .ingest_usage_event(ev)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    info!(
        session_id = %session_id,
        tenant_id = %verified.tenant_id,
        task = "text_translate",
        target_lang = %target_lang,
        endpoint = "translations",
        "text_task_completed"
    );

    Ok(ApiResponse::ok(TextTranslateResponse {
        session_id,
        seq: appended.seq,
        source_text: text.to_string(),
        translated_text: out.content,
        source_lang,
        target_lang,
        model: llm_cfg.model,
    }))
}

#[derive(Debug, Deserialize)]
pub struct AppendInputHttpRequest {
    pub text: String,
    pub client_input_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendInputHttpResponse {
    pub session_id: String,
    pub seq: i64,
    pub replayed: bool,
    pub client_input_id: String,
}

async fn append_session_input_http(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<AppendInputHttpRequest>,
) -> Result<Json<ApiResponse<AppendInputHttpResponse>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
    if sess.tenant_id != verified.tenant_id {
        return Err(AppError::NotFound("session not found".to_string()));
    }
    let res = state
        .repo
        .append_summary_input(
            &session_id,
            "text",
            Some(&req.text),
            None,
            None,
            &req.client_input_id,
        )
        .await
        .map_err(map_dal_err)?;

    info!(
        session_id = %session_id,
        tenant_id = %verified.tenant_id,
        seq = res.seq,
        replayed = res.replayed,
        "summary_input_appended"
    );

    Ok(ApiResponse::ok(AppendInputHttpResponse {
        session_id,
        seq: res.seq,
        replayed: res.replayed,
        client_input_id: req.client_input_id.trim().to_string(),
    }))
}

#[derive(Debug, Clone, Deserialize)]
pub struct InlineSummaryInputItem {
    pub text: String,
    pub client_input_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SummarizeHttpRequest {
    pub request_id: String,
    #[serde(default)]
    pub speak: bool,
    #[serde(default)]
    pub target_lang: String,
    pub summary_source: Option<String>,
    #[serde(default)]
    pub summary_style: String,
    #[serde(default)]
    pub replace_session_title: bool,
    pub session_id: Option<String>,
    #[serde(default)]
    pub inputs: Vec<InlineSummaryInputItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummarizeResponse {
    pub session_id: String,
    pub request_id: String,
    pub result_seq: i64,
    pub title: String,
    pub summary_style: String,
    pub summary_text: String,
    pub input_count: usize,
    pub truncated_inputs: usize,
    pub model: String,
    pub summary_source: String,
    pub target_lang: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice_fallback: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tts_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tts: Option<ModelBinding>,
}

async fn summarize_session_http(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<SummarizeHttpRequest>,
) -> Result<Json<ApiResponse<SummarizeResponse>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
    if sess.tenant_id != verified.tenant_id {
        return Err(AppError::NotFound("session not found".to_string()));
    }
    for item in &req.inputs {
        state
            .repo
            .append_summary_input(
                &session_id,
                "text",
                Some(&item.text),
                None,
                None,
                &item.client_input_id,
            )
            .await
            .map_err(map_dal_err)?;
    }
    let mut out = execute_summary(&state, &sess, &req, false).await?;
    out.tts = None;
    Ok(ApiResponse::ok(out))
}

async fn summarize_inline_http(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Json(req): Json<SummarizeHttpRequest>,
) -> Result<Json<ApiResponse<SummarizeResponse>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let session_id = if let Some(sid) = req
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let s = state
            .repo
            .get_session_record(sid)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?
            .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
        if s.tenant_id != verified.tenant_id {
            return Err(AppError::NotFound("session not found".to_string()));
        }
        sid.to_string()
    } else {
        if req.inputs.is_empty() {
            return Err(AppError::bad_request_code(
                "empty_input",
                "inputs cannot be empty when session_id is omitted",
            ));
        }
        let new_id = format!("sess_{}", uuid::Uuid::now_v7().simple());
        state
            .repo
            .insert_session_record(
                &new_id,
                None,
                &verified,
                "cascade",
                "context",
                "",
                "",
                false,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        new_id
    };

    for item in &req.inputs {
        state
            .repo
            .append_summary_input(
                &session_id,
                "text",
                Some(&item.text),
                None,
                None,
                &item.client_input_id,
            )
            .await
            .map_err(map_dal_err)?;
    }

    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;

    let mut out = execute_summary(&state, &sess, &req, false).await?;
    out.tts = None;
    Ok(ApiResponse::ok(out))
}

pub async fn execute_summary(
    state: &AccessState,
    sess: &SessionRecordFull,
    req: &SummarizeHttpRequest,
    include_tts_binding: bool,
) -> Result<SummarizeResponse, AppError> {
    let request_id = req.request_id.trim();
    if request_id.is_empty() {
        return Err(AppError::bad_request_code(
            "empty_input",
            "request_id cannot be empty",
        ));
    }
    let summary_style = if req.summary_style.trim().is_empty() {
        "speech"
    } else {
        req.summary_style.trim()
    };
    if !matches!(summary_style, "speech" | "meeting" | "concise") {
        return Err(AppError::bad_request_code(
            "empty_input",
            format!("invalid summary_style {}", summary_style),
        ));
    }

    let policy = require_tenant_policy(&state.repo, &sess.tenant_id).await?;
    let requested_lang = if !req.target_lang.trim().is_empty() {
        Some(req.target_lang.as_str())
    } else if !sess.target_lang.trim().is_empty() {
        Some(sess.target_lang.as_str())
    } else {
        None
    };
    let eff_target_lang =
        ensure_allowed(requested_lang, &policy).map_err(AppError::BadRequest)?;

    // Step 1: Assemble materials BEFORE begin_summary so empty inputs never create a pending row.
    let rows = state
        .repo
        .list_summary_inputs(&sess.session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let (eff_source, joined_materials, input_count, truncated_inputs, seq_from, seq_to) =
        format_summary_materials(&sess.task, req.summary_source.as_deref(), &rows)
            .map_err(AppError::BadRequest)?;

    // Step 2: Two-phase reservation
    let outcome = state
        .repo
        .begin_summary(
            &sess.session_id,
            request_id,
            seq_from,
            seq_to,
            truncated_inputs as i32,
            &eff_target_lang,
            summary_style,
        )
        .await
        .map_err(map_dal_err)?;

    let final_row = match outcome {
        BeginSummaryOutcome::Ready(row) => row,
        BeginSummaryOutcome::Failed(row) => {
            let err_code = row
                .error_code
                .unwrap_or_else(|| "llm_failed".to_string());
            return Err(AppError::conflict_code(
                &err_code,
                format!("summary request {} previously failed", request_id),
                None,
            ));
        }
        BeginSummaryOutcome::Busy => {
            return Err(AppError::conflict_code(
                "summary_in_progress",
                "summary generation is in progress",
                Some(2),
            ));
        }
        BeginSummaryOutcome::Owned { .. } => {
            // Step 3: Lazy LLM resolution after owning the summary slot
            let llm_cfg = if let Some(cid) = sess
                .llm_config_id
                .as_deref()
                .filter(|s| !s.is_empty())
            {
                state
                    .repo
                    .get_model_config(cid)
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?
            } else {
                let resolved = state
                    .repo
                    .resolve_model_config(&sess.tenant_id, "llm", None)
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                if let Some(cfg) = &resolved {
                    state
                        .repo
                        .set_session_llm_config(&sess.session_id, &cfg.config_id, &cfg.model)
                        .await
                        .map_err(|e| AppError::Internal(e.to_string()))?;
                }
                resolved
            };

            let Some(llm_cfg) = llm_cfg else {
                let _ = state
                    .repo
                    .fail_summary(&sess.session_id, request_id, "llm_unavailable")
                    .await;
                return Err(AppError::bad_request_code(
                    "llm_unavailable",
                    "当前租户没有可用的 LLM 配置",
                ));
            };
            if !is_llm_runnable(&llm_cfg) {
                let _ = state
                    .repo
                    .fail_summary(&sess.session_id, request_id, "llm_protocol_unsupported")
                    .await;
                return Err(AppError::bad_request_code(
                    "llm_protocol_unsupported",
                    format!("LLM protocol {} is not supported", llm_cfg.protocol),
                ));
            }

            // Step 4: Call LLM outside any database transaction
            let binding = ModelBinding::from(llm_cfg.clone());
            let sys_prompt = summary_system_prompt(&eff_target_lang, summary_style);
            let chat_res = state
                .chat_client
                .complete(
                    &binding,
                    &sys_prompt,
                    &joined_materials,
                    &state.livekit_api_key,
                    &state.livekit_api_secret,
                )
                .await;

            match chat_res {
                Ok(out) => {
                    let (title, body) = extract_summary_title_and_body(&out.content);
                    let finished = state
                        .repo
                        .finish_summary(
                            &sess.session_id,
                            request_id,
                            &title,
                            &body,
                            req.replace_session_title,
                        )
                        .await
                        .map_err(|e| AppError::Internal(e.to_string()))?;

                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    let ev = UsageEventPayload {
                        event_id: format!("ev_sum_{}", uuid::Uuid::now_v7().simple()),
                        idempotency_key: format!("api_summary:{}:{}", sess.session_id, request_id),
                        session_id: sess.session_id.clone(),
                        room_name: sess.room_name.clone(),
                        tenant_id: sess.tenant_id.clone(),
                        api_key_id: sess.api_key_id.clone(),
                        source: "himarket_access".to_string(),
                        category: "summary".to_string(),
                        model: llm_cfg.model.clone(),
                        vendor: String::new(),
                        status: "SUCCESS".to_string(),
                        occurred_at_ms: now_ms,
                        metrics: UsageMetricsPayload {
                            prompt_tokens: out
                                .prompt_tokens
                                .or(Some(joined_materials.chars().count() as i64)),
                            completion_tokens: out
                                .completion_tokens
                                .or(Some(body.chars().count() as i64)),
                            cached_tokens: out.cached_tokens,
                            ..Default::default()
                        },
                    };
                    let _ = state
                        .repo
                        .ingest_usage_event(ev)
                        .await
                        .map_err(|e| AppError::Internal(e.to_string()))?;
                    finished
                }
                Err(err) => {
                    let code = match &err {
                        AppError::BadGateway(m) | AppError::BadRequest(m) => {
                            himarket_core::extract_machine_code(m)
                                .unwrap_or_else(|| "llm_failed".to_string())
                        }
                        _ => "llm_failed".to_string(),
                    };
                    let _ = state
                        .repo
                        .fail_summary(&sess.session_id, request_id, &code)
                        .await;
                    return Err(err);
                }
            }
        }
    };

    let summary_text = final_row.summary_text.unwrap_or_default();
    let refreshed_sess = state
        .repo
        .get_session_record(&sess.session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .unwrap_or_else(|| sess.clone());
    let model_name = refreshed_sess
        .llm_model
        .unwrap_or_else(|| "deepseek-v4.1-flash:1017-cloud".to_string());

    let (voice_fallback, segment_count, tts_error, tts_binding) = if req.speak {
        let (tts_opt, fb, err_opt) = state
            .repo
            .ensure_tts_for_session(
                &sess.session_id,
                &sess.tenant_id,
                &eff_target_lang,
                refreshed_sess.tts_config_id.as_deref(),
            )
            .await
            .map_err(map_dal_err)?;
        if let Some(cfg) = tts_opt {
            let segs = split_speak_segments(&summary_text).unwrap_or_else(|_| vec![summary_text.clone()]);
            (
                Some(fb),
                Some(segs.len()),
                None,
                if include_tts_binding {
                    Some(ModelBinding::from(cfg))
                } else {
                    None
                },
            )
        } else {
            (Some(false), Some(0), err_opt, None)
        }
    } else {
        (None, None, None, None)
    };

    info!(
        session_id = %sess.session_id,
        tenant_id = %sess.tenant_id,
        task = %sess.task,
        target_lang = %eff_target_lang,
        endpoint = "summarize",
        "text_task_completed"
    );

    Ok(SummarizeResponse {
        session_id: sess.session_id.clone(),
        request_id: request_id.to_string(),
        result_seq: final_row.result_seq,
        title: final_row.title,
        summary_style: final_row.summary_style,
        summary_text,
        input_count,
        truncated_inputs,
        model: model_name,
        summary_source: eff_source,
        target_lang: eff_target_lang,
        voice_fallback,
        segment_count,
        tts_error,
        tts: tts_binding,
    })
}

async fn get_summary_audio_segment(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path((session_id, request_id, index)): Path<(String, String, usize)>,
) -> Result<Response, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
    if sess.tenant_id != verified.tenant_id {
        return Err(AppError::NotFound("session not found".to_string()));
    }
    let row = state
        .repo
        .get_summary_result(&session_id, &request_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("summary result not found".to_string()))?;
    let text = row
        .summary_text
        .as_deref()
        .ok_or_else(|| AppError::NotFound("summary text not ready".to_string()))?;
    let segments = split_speak_segments(text).map_err(AppError::BadRequest)?;
    let seg_text = segments
        .get(index)
        .ok_or_else(|| AppError::NotFound("segment index out of range".to_string()))?;

    let tts_id = sess.tts_config_id.as_deref().ok_or_else(|| {
        AppError::bad_request_code("tts_unavailable", "session has no bound TTS config")
    })?;
    let tts_cfg = state
        .repo
        .get_model_config(tts_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::bad_request_code("tts_unavailable", "TTS config not found"))?;
    let binding = ModelBinding::from(tts_cfg.clone());

    let wav_bytes = match state
        .synth_client
        .synthesize(&binding, seg_text, &state.internal_secret)
        .await
    {
        Ok(b) => b,
        Err(_) => {
            return Ok((
                StatusCode::BAD_GATEWAY,
                [(header::CONTENT_TYPE, "application/json")],
                Body::from(r#"{"error":"tts_failed"}"#),
            )
                .into_response());
        }
    };

    if index == 0 {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let ev = UsageEventPayload {
            event_id: format!("ev_sum_tts_{}", uuid::Uuid::now_v7().simple()),
            idempotency_key: format!("api_tts:{}:summary:{}", session_id, request_id),
            session_id: session_id.clone(),
            room_name: sess.room_name.clone(),
            tenant_id: verified.tenant_id,
            api_key_id: verified.api_key_id,
            source: "himarket_access".to_string(),
            category: "tts".to_string(),
            model: tts_cfg.model,
            vendor: String::new(),
            status: "SUCCESS".to_string(),
            occurred_at_ms: now_ms,
            metrics: UsageMetricsPayload {
                tts_characters: Some(text.chars().count() as i64),
                audio_duration_ms: Some((wav_bytes.len() as i64 / 48).max(100)),
                ..Default::default()
            },
        };
        let _ = state.repo.ingest_usage_event(ev).await;
    }

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "audio/wav")],
        Body::from(wav_bytes),
    )
        .into_response())
}

#[derive(Debug, Deserialize)]
pub struct SpeakHttpRequest {
    pub text: String,
    #[serde(default)]
    pub target_lang: String,
    #[serde(default)]
    pub voice: Option<String>,
    #[serde(default)]
    pub speed: Option<f32>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub sample_rate: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpeakHttpResponse {
    pub session_id: String,
    pub target_lang: String,
    pub voice_fallback: bool,
    pub tts_characters: usize,
    pub segment_count: usize,
}

async fn create_speak_http(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Json(req): Json<SpeakHttpRequest>,
) -> Result<Json<ApiResponse<SpeakHttpResponse>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let tenant_id = verified.tenant_id.as_str();

    let segments = split_speak_segments(&req.text).map_err(AppError::BadRequest)?;
    let tts_characters = req.text.trim().chars().count();

    let policy = require_tenant_policy(&state.repo, tenant_id).await?;
    let target_lang =
        ensure_allowed(Some(&req.target_lang), &policy).map_err(AppError::BadRequest)?;

    let sel_tts = state
        .repo
        .select_tts_for_lang(tenant_id, &target_lang)
        .await
        .map_err(|_| {
            AppError::bad_request_code("tts_unavailable", "当前租户没有可用的 TTS 配置")
        })?;
    if !is_tts_runnable(&sel_tts.config) {
        return Err(AppError::bad_request_code(
            "tts_protocol_unsupported",
            "selected TTS protocol is not runnable",
        ));
    }

    let session_id = format!("sess_spk_{}", uuid::Uuid::now_v7().simple());
    state
        .repo
        .insert_session_record(
            &session_id,
            None,
            &verified,
            "cascade",
            "speak",
            "",
            &target_lang,
            true,
            None,
            None,
            Some(&sel_tts.config.config_id),
            None,
            Some(req.text.trim()),
        )
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let ev = UsageEventPayload {
        event_id: format!("ev_spk_{}", uuid::Uuid::now_v7().simple()),
        idempotency_key: format!("api_tts:{}", session_id),
        session_id: session_id.clone(),
        room_name: None,
        tenant_id: verified.tenant_id.clone(),
        api_key_id: verified.api_key_id.clone(),
        source: "himarket_access".to_string(),
        category: "tts".to_string(),
        model: sel_tts.config.model.clone(),
        vendor: String::new(),
        status: "SUCCESS".to_string(),
        occurred_at_ms: now_ms,
        metrics: UsageMetricsPayload {
            tts_characters: Some(tts_characters as i64),
            audio_duration_ms: Some((tts_characters as i64 * 150).max(100)),
            ..Default::default()
        },
    };
    let _ = state
        .repo
        .ingest_usage_event(ev)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    info!(
        session_id = %session_id,
        tenant_id = %verified.tenant_id,
        task = "speak",
        target_lang = %target_lang,
        voice_fallback = sel_tts.voice_fallback,
        endpoint = "speak",
        "text_task_completed"
    );

    Ok(ApiResponse::ok(SpeakHttpResponse {
        session_id,
        target_lang,
        voice_fallback: sel_tts.voice_fallback,
        tts_characters,
        segment_count: segments.len(),
    }))
}

async fn get_speak_segment_http(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path((session_id, index)): Path<(String, usize)>,
) -> Result<Response, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
    if sess.tenant_id != verified.tenant_id {
        return Err(AppError::NotFound("session not found".to_string()));
    }
    let full_text = sess
        .speak_text
        .as_deref()
        .ok_or_else(|| AppError::NotFound("speak text not found".to_string()))?;
    let segments = split_speak_segments(full_text).map_err(AppError::BadRequest)?;
    let seg_text = segments
        .get(index)
        .ok_or_else(|| AppError::NotFound("segment index out of range".to_string()))?;

    let tts_id = sess.tts_config_id.as_deref().ok_or_else(|| {
        AppError::bad_request_code("tts_unavailable", "session has no bound TTS config")
    })?;
    let tts_cfg = state
        .repo
        .get_model_config(tts_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::bad_request_code("tts_unavailable", "TTS config not found"))?;
    let binding = ModelBinding::from(tts_cfg);

    match state
        .synth_client
        .synthesize(&binding, seg_text, &state.internal_secret)
        .await
    {
        Ok(wav_bytes) => Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "audio/wav")],
            Body::from(wav_bytes),
        )
            .into_response()),
        Err(_) => Ok((
            StatusCode::BAD_GATEWAY,
            [(header::CONTENT_TYPE, "application/json")],
            Body::from(r#"{"error":"tts_failed"}"#),
        )
            .into_response()),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateTargetLangRequest {
    pub target_lang: Option<String>,
    pub source_lang: Option<String>,
    pub resolve_tts: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTargetLangResponse {
    pub session_id: String,
    pub source_lang: String,
    pub target_lang: String,
    pub voice_fallback: bool,
    pub stt_fallback: bool,
    pub tts_error: Option<String>,
    pub stt: Option<ModelBinding>,
    pub tts: Option<ModelBinding>,
}

async fn apply_session_language_update(
    state: &AccessState,
    sess: &SessionRecordFull,
    req: &UpdateTargetLangRequest,
) -> Result<UpdateTargetLangResponse, AppError> {
    if sess.task == "cascade" || sess.task == "speak" {
        return Err(AppError::bad_request_code(
            "task_rejected",
            format!("task {} does not allow updating target_lang", sess.task),
        ));
    }

    let policy = require_tenant_policy(&state.repo, &sess.tenant_id).await?;

    let new_source_lang = if let Some(src) = req.source_lang.as_deref() {
        let trimmed = src.trim();
        if trimmed.is_empty() {
            String::new()
        } else {
            ensure_allowed(Some(trimmed), &policy).map_err(AppError::BadRequest)?
        }
    } else {
        sess.source_lang.clone()
    };

    let raw_target = req
        .target_lang
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            if sess.target_lang.trim().is_empty() {
                None
            } else {
                Some(sess.target_lang.as_str())
            }
        });
    let new_target_lang =
        ensure_allowed(raw_target, &policy).map_err(AppError::BadRequest)?;

    let mut stt_binding: Option<ModelBinding> = None;
    let mut stt_fallback = false;
    let mut new_stt_id: Option<String> = sess.stt_config_id.clone();

    if matches!(sess.task.as_str(), "transcribe" | "speech_translate")
        && (req.source_lang.is_some() || sess.stt_config_id.is_none())
    {
        if let Ok(sel_stt) = state
            .repo
            .select_stt_for_lang(
                &sess.tenant_id,
                if new_source_lang.is_empty() {
                    None
                } else {
                    Some(new_source_lang.as_str())
                },
                None,
            )
            .await
        {
            stt_fallback = sel_stt.stt_fallback;
            new_stt_id = Some(sel_stt.config.config_id.clone());
            stt_binding = Some(ModelBinding::from(sel_stt.config));
        }
    } else if let Some(cid) = sess.stt_config_id.as_deref() {
        if let Ok(Some(cfg)) = state.repo.get_model_config(cid).await {
            stt_binding = Some(ModelBinding::from(cfg));
        }
    }

    // Always re-select TTS for speech_translate, transcribe, context, and text_translate
    let (tts_binding, voice_fallback, tts_error, new_tts_id) = match state
        .repo
        .select_tts_for_lang(&sess.tenant_id, &new_target_lang)
        .await
    {
        Ok(sel) => {
            let cid = sel.config.config_id.clone();
            (
                Some(ModelBinding::from(sel.config)),
                sel.voice_fallback,
                None,
                Some(Some(cid)),
            )
        }
        Err(TtsSelectError::Unavailable) => (
            None,
            false,
            Some("tts_unavailable".to_string()),
            Some(None),
        ),
    };

    state
        .repo
        .update_session_languages_and_bindings(
            &sess.session_id,
            Some(&new_source_lang),
            Some(&new_target_lang),
            new_stt_id.as_deref(),
            new_tts_id.as_ref().map(|o| o.as_deref()),
        )
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    info!(
        session_id = %sess.session_id,
        tenant_id = %sess.tenant_id,
        task = %sess.task,
        target_lang = %new_target_lang,
        voice_fallback = voice_fallback,
        "target_lang_updated"
    );

    Ok(UpdateTargetLangResponse {
        session_id: sess.session_id.clone(),
        source_lang: new_source_lang,
        target_lang: new_target_lang,
        voice_fallback,
        stt_fallback,
        tts_error,
        stt: stt_binding,
        tts: tts_binding,
    })
}

async fn update_session_target_lang_http(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<UpdateTargetLangRequest>,
) -> Result<Json<ApiResponse<UpdateTargetLangResponse>>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
    if sess.tenant_id != verified.tenant_id {
        return Err(AppError::NotFound("session not found".to_string()));
    }
    let mut out = apply_session_language_update(&state, &sess, &req).await?;
    out.stt = None;
    out.tts = None;
    Ok(ApiResponse::ok(out))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRuntimeResponse {
    pub session_id: String,
    pub tenant_id: String,
    pub api_key_id: String,
    pub task: String,
    pub mode: String,
    pub source_lang: String,
    pub target_lang: String,
    pub speak: bool,
    pub voice_fallback: bool,
    pub stt_fallback: bool,
    pub llm: Option<ModelBinding>,
    pub stt: Option<ModelBinding>,
    pub tts: Option<ModelBinding>,
}

async fn get_session_runtime_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<ApiResponse<SessionRuntimeResponse>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;

    let llm_cap = if sess.task == "speech_translate" {
        "translate_llm"
    } else {
        "chat_llm"
    };
    let routed_llm = state
        .repo
        .get_route_target_config(&sess.tenant_id, llm_cap, &["*"])
        .await
        .ok()
        .flatten();
    let llm = if let Some(rcfg) = routed_llm.filter(|c| {
        c.config_id != "lk-llm-openai_chat-deepseek-v4-1-flash-1017-cloud" && is_llm_runnable(c)
    }) {
        Some(ModelBinding::from(rcfg))
    } else if let Some(cid) = sess.llm_config_id.as_deref() {
        state
            .repo
            .get_model_config(cid)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?
            .map(ModelBinding::from)
    } else {
        None
    };

    let routed_stt = state
        .repo
        .get_route_target_config(&sess.tenant_id, "asr", &["*"])
        .await
        .ok()
        .flatten();
    let stt = if let Some(rcfg) = routed_stt.filter(|c| {
        c.config_id != "lk-stt-whisper_local-faster-whisper-tiny" && himarket_dal::is_stt_runnable(c)
    }) {
        Some(ModelBinding::from(rcfg))
    } else if let Some(cid) = sess.stt_config_id.as_deref() {
        state
            .repo
            .get_model_config(cid)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?
            .map(ModelBinding::from)
    } else {
        None
    };

    let mut voice_fallback = false;
    let routed_tts = state
        .repo
        .get_route_target_config(&sess.tenant_id, "tts", &["*"])
        .await
        .ok()
        .flatten();
    let tts = if let Some(rcfg) = routed_tts.filter(|c| {
        c.config_id != "lk-tts-piper_local-zh_CN-huayan-medium" && is_tts_runnable(c)
    }) {
        Some(ModelBinding::from(rcfg))
    } else if let Some(cid) = sess.tts_config_id.as_deref() {
        let cfg_opt = state
            .repo
            .get_model_config(cid)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        if let Some(cfg) = cfg_opt {
            if !sess.target_lang.is_empty()
                && !himarket_dal::tts_matches_target_lang(&cfg, &sess.target_lang)
            {
                voice_fallback = true;
            }
            Some(ModelBinding::from(cfg))
        } else {
            None
        }
    } else {
        None
    };

    Ok(ApiResponse::ok(SessionRuntimeResponse {
        session_id: sess.session_id,
        tenant_id: sess.tenant_id,
        api_key_id: sess.api_key_id,
        task: sess.task,
        mode: sess.voice_mode,
        source_lang: sess.source_lang,
        target_lang: sess.target_lang,
        speak: sess.speak,
        voice_fallback,
        stt_fallback: false,
        llm,
        stt,
        tts,
    }))
}

async fn update_session_target_lang_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<UpdateTargetLangRequest>,
) -> Result<Json<ApiResponse<UpdateTargetLangResponse>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
    let out = apply_session_language_update(&state, &sess, &req).await?;
    Ok(ApiResponse::ok(out))
}

#[derive(Debug, Deserialize)]
pub struct BindTtsInternalRequest {
    pub target_lang: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BindTtsInternalResponse {
    pub session_id: String,
    pub target_lang: String,
    pub voice_fallback: bool,
    pub tts_error: Option<String>,
    pub tts: Option<ModelBinding>,
}

async fn bind_session_tts_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<BindTtsInternalRequest>,
) -> Result<Json<ApiResponse<BindTtsInternalResponse>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;

    let policy = require_tenant_policy(&state.repo, &sess.tenant_id).await?;
    if let Some(req_lang) = req
        .target_lang
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let norm_req = ensure_allowed(Some(req_lang), &policy).map_err(AppError::BadRequest)?;
        if !sess.target_lang.is_empty() && norm_req != sess.target_lang {
            return Err(AppError::conflict_code(
                "target_lang_stale",
                format!(
                    "worker target_lang {} does not match session target_lang {}",
                    norm_req, sess.target_lang
                ),
                None,
            ));
        }
    }

    let eff_target_lang = if !sess.target_lang.trim().is_empty() {
        sess.target_lang.clone()
    } else if let Some(req_lang) = req
        .target_lang
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        ensure_allowed(Some(req_lang), &policy).map_err(AppError::BadRequest)?
    } else {
        policy.default_target_lang.clone()
    };

    let (cfg_opt, voice_fallback, tts_error) = state
        .repo
        .ensure_tts_for_session(
            &sess.session_id,
            &sess.tenant_id,
            &eff_target_lang,
            sess.tts_config_id.as_deref(),
        )
        .await
        .map_err(map_dal_err)?;

    Ok(ApiResponse::ok(BindTtsInternalResponse {
        session_id: sess.session_id,
        target_lang: eff_target_lang,
        voice_fallback,
        tts_error,
        tts: cfg_opt.map(ModelBinding::from),
    }))
}

#[derive(Debug, Deserialize)]
pub struct AppendSummaryInputInternalRequest {
    pub origin: String,
    pub source_text: Option<String>,
    pub translated_text: Option<String>,
    pub assistant_text: Option<String>,
    pub client_input_id: String,
}

async fn append_summary_input_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<AppendSummaryInputInternalRequest>,
) -> Result<Json<ApiResponse<AppendSummaryInputResult>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    if !matches!(
        req.origin.as_str(),
        "stt" | "text" | "translate" | "cascade"
    ) {
        return Err(AppError::bad_request_code(
            "empty_input",
            format!("invalid origin {}", req.origin),
        ));
    }
    let res = state
        .repo
        .append_summary_input(
            &session_id,
            &req.origin,
            req.source_text.as_deref(),
            req.translated_text.as_deref(),
            req.assistant_text.as_deref(),
            &req.client_input_id,
        )
        .await
        .map_err(map_dal_err)?;
    Ok(ApiResponse::ok(res))
}

async fn list_summary_inputs_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<ApiResponse<Vec<SummaryInputRow>>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    let rows = state
        .repo
        .list_summary_inputs(&session_id)
        .await
        .map_err(map_dal_err)?;
    Ok(ApiResponse::ok(rows))
}

#[derive(Debug, Deserialize)]
pub struct BeginSummaryInternalRequest {
    pub request_id: String,
    #[serde(default)]
    pub input_seq_from: i64,
    #[serde(default)]
    pub input_seq_to: i64,
    #[serde(default)]
    pub truncated_inputs: i32,
    #[serde(default)]
    pub target_lang: String,
    #[serde(default)]
    pub summary_style: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BeginSummaryInternalResponse {
    pub state: String,
    pub result_seq: Option<i64>,
    pub result: Option<SummaryResultRow>,
}

async fn begin_summary_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<BeginSummaryInternalRequest>,
) -> Result<Json<ApiResponse<BeginSummaryInternalResponse>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    let outcome = state
        .repo
        .begin_summary(
            &session_id,
            &req.request_id,
            req.input_seq_from,
            req.input_seq_to,
            req.truncated_inputs,
            &req.target_lang,
            &req.summary_style,
        )
        .await
        .map_err(map_dal_err)?;
    match outcome {
        BeginSummaryOutcome::Owned { result_seq } => {
            Ok(ApiResponse::ok(BeginSummaryInternalResponse {
                state: "owned".to_string(),
                result_seq: Some(result_seq),
                result: None,
            }))
        }
        BeginSummaryOutcome::Ready(row) => Ok(ApiResponse::ok(BeginSummaryInternalResponse {
            state: "ready".to_string(),
            result_seq: Some(row.result_seq),
            result: Some(row),
        })),
        BeginSummaryOutcome::Failed(row) => {
            let code = row
                .error_code
                .unwrap_or_else(|| "llm_failed".to_string());
            Err(AppError::conflict_code(
                &code,
                "summary request failed",
                None,
            ))
        }
        BeginSummaryOutcome::Busy => Err(AppError::conflict_code(
            "summary_in_progress",
            "summary in progress",
            Some(2),
        )),
    }
}

#[derive(Debug, Deserialize)]
pub struct FinishSummaryInternalRequest {
    pub request_id: String,
    #[serde(default)]
    pub title: String,
    pub summary_text: String,
    #[serde(default)]
    pub replace_session_title: bool,
}

async fn finish_summary_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<FinishSummaryInternalRequest>,
) -> Result<Json<ApiResponse<SummaryResultRow>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    let row = state
        .repo
        .finish_summary(
            &session_id,
            &req.request_id,
            &req.title,
            &req.summary_text,
            req.replace_session_title,
        )
        .await
        .map_err(map_dal_err)?;
    Ok(ApiResponse::ok(row))
}

#[derive(Debug, Deserialize)]
pub struct FailSummaryInternalRequest {
    pub request_id: String,
    pub error_code: String,
}

async fn fail_summary_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<FailSummaryInternalRequest>,
) -> Result<Json<ApiResponse<SummaryResultRow>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    let row = state
        .repo
        .fail_summary(&session_id, &req.request_id, &req.error_code)
        .await
        .map_err(map_dal_err)?;
    Ok(ApiResponse::ok(row))
}

async fn execute_summary_internal(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<SummarizeHttpRequest>,
) -> Result<Json<ApiResponse<SummarizeResponse>>, AppError> {
    verify_internal_secret_strict(&headers, &state.internal_secret)?;
    let sess = state
        .repo
        .get_session_record(&session_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound("session not found".to_string()))?;
    let out = execute_summary(&state, &sess, &req, true).await?;
    Ok(ApiResponse::ok(out))
}

async fn dispatch_agent_via_twirp(
    state: &AccessState,
    room_name: &str,
    metadata: &str,
) -> anyhow::Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let admin_claims = LiveKitClaims {
        iss: state.livekit_api_key.clone(),
        sub: "himarket-access-dispatcher".to_string(),
        name: "HiMarket Access Dispatcher".to_string(),
        nbf: now.saturating_sub(5),
        exp: now + 60,
        metadata: String::new(),
        attributes: HashMap::new(),
        video: VideoGrant {
            room_join: false,
            room: room_name.to_string(),
            can_publish: false,
            can_subscribe: false,
            can_publish_data: false,
            can_update_own_metadata: false,
            room_admin: Some(true),
            room_create: Some(true),
        },
        room_config: None,
    };
    let admin_jwt = encode(
        &Header::new(Algorithm::HS256),
        &admin_claims,
        &EncodingKey::from_secret(state.livekit_api_secret.as_bytes()),
    )?;

    let url = format!(
        "{}/twirp/livekit.AgentDispatchService/CreateDispatch",
        state.livekit_internal_http_url.trim_end_matches('/')
    );
    let body = serde_json::json!({
        "agent_name": "himarket-voice-agent",
        "room": room_name,
        "metadata": metadata,
    });

    let resp = state
        .http_client
        .post(&url)
        .header("Authorization", format!("Bearer {}", admin_jwt))
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("LiveKit CreateDispatch status {}", resp.status());
    }
    Ok(())
}

pub fn build_demo_wav_bytes(sample_rate: u32, duration_ms: u32) -> Vec<u8> {
    let num_samples = ((sample_rate * duration_ms) / 1000).max(200) as usize;
    let data_len = (num_samples * 2) as u32;
    let byte_rate = sample_rate * 2;
    let mut wav = Vec::with_capacity(44 + data_len as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_len).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());
    for i in 0..num_samples {
        let val = ((i as f32 * 0.12).sin() * 6000.0) as i16;
        wav.extend_from_slice(&val.to_le_bytes());
    }
    wav
}

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
    pub capability: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub target_lang: Option<String>,
}

async fn v1_chat_completions(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Json(req): Json<V1ChatCompletionRequest>,
) -> Result<Response, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let tenant_id = verified.tenant_id.as_str();

    let cap_raw = headers
        .get("X-HiMarket-Capability")
        .and_then(|v| v.to_str().ok())
        .or(req.capability.as_deref())
        .unwrap_or("chat_llm")
        .trim()
        .to_ascii_lowercase();

    let (route_cap, meter_cat) = match cap_raw.as_str() {
        "translate" | "translate_llm" => ("translate_llm", "translate"),
        "summary" | "summary_llm" => ("summary_llm", "summary"),
        _ => ("chat_llm", "text_llm"),
    };

    let lang = headers
        .get("X-HiMarket-Lang")
        .and_then(|v| v.to_str().ok())
        .or(req.language.as_deref())
        .or(req.target_lang.as_deref())
        .unwrap_or("*");

    let llm_cfg = state
        .repo
        .resolve_route_config(tenant_id, route_cap, Some(lang), req.model.as_deref())
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| {
            AppError::bad_request_code("llm_unavailable", "当前租户没有可用的 LLM 配置")
        })?;
    if !is_llm_runnable(&llm_cfg) {
        return Err(AppError::bad_request_code(
            "llm_protocol_unsupported",
            format!("LLM protocol {} is not runnable", llm_cfg.protocol),
        ));
    }

    let mut sys_parts = Vec::new();
    let mut user_parts = Vec::new();
    for m in &req.messages {
        let txt = match &m.content {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        if m.role == "system" {
            sys_parts.push(txt);
        } else {
            user_parts.push(txt);
        }
    }
    let system_prompt = if !sys_parts.is_empty() {
        sys_parts.join("\n")
    } else if route_cap == "translate_llm" && !lang.is_empty() && lang != "*" {
        translation_system_prompt(lang)
    } else if route_cap == "summary_llm" {
        summary_system_prompt(if lang == "*" { "zh" } else { lang }, "speech")
    } else {
        "你是绿联AI统一接入平台的智能助手，请准确、简洁地回答。".to_string()
    };
    let user_content = user_parts.join("\n");
    if user_content.trim().is_empty() {
        return Err(AppError::bad_request_code(
            "empty_input",
            "messages content cannot be empty",
        ));
    }

    let vendor = infer_vendor(&llm_cfg.protocol, &llm_cfg.model, &llm_cfg.base_url);
    let binding = ModelBinding::from(llm_cfg.clone());
    let start = std::time::Instant::now();
    let res = state
        .chat_client
        .complete(
            &binding,
            &system_prompt,
            &user_content,
            &state.livekit_api_key,
            &state.livekit_api_secret,
        )
        .await;
    let latency_ms = (start.elapsed().as_millis() as i64).max(1);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let cmpl_id = format!("chatcmpl_{}", uuid::Uuid::now_v7().simple());

    match res {
        Ok(out) => {
            let p_tok = out
                .prompt_tokens
                .unwrap_or_else(|| user_content.chars().count() as i64);
            let c_tok = out
                .completion_tokens
                .unwrap_or_else(|| out.content.chars().count() as i64);
            let ev = UsageEventPayload {
                event_id: format!("ev_v1_chat_{}", uuid::Uuid::now_v7().simple()),
                idempotency_key: format!("v1_chat:{}", cmpl_id),
                session_id: cmpl_id.clone(),
                room_name: None,
                tenant_id: verified.tenant_id.clone(),
                api_key_id: verified.api_key_id.clone(),
                source: "ai_gateway".to_string(),
                category: meter_cat.to_string(),
                model: llm_cfg.model.clone(),
                vendor: vendor.clone(),
                status: "SUCCESS".to_string(),
                occurred_at_ms: now_ms,
                metrics: UsageMetricsPayload {
                    prompt_tokens: Some(p_tok),
                    completion_tokens: Some(c_tok),
                    cached_tokens: out.cached_tokens,
                    ttft_ms: Some((latency_ms / 2).max(1)),
                    latency_ms: Some(latency_ms),
                    ..Default::default()
                },
            };
            let _ = state.repo.ingest_usage_event(ev).await;

            if req.stream {
                let mut sse = String::new();
                let chunk1 = serde_json::json!({
                    "id": cmpl_id,
                    "object": "chat.completion.chunk",
                    "created": now_ms / 1000,
                    "model": llm_cfg.model,
                    "vendor": vendor,
                    "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null}]
                });
                let chunk2 = serde_json::json!({
                    "id": cmpl_id,
                    "object": "chat.completion.chunk",
                    "created": now_ms / 1000,
                    "model": llm_cfg.model,
                    "vendor": vendor,
                    "choices": [{"index": 0, "delta": {"content": out.content}, "finish_reason": null}]
                });
                let chunk3 = serde_json::json!({
                    "id": cmpl_id,
                    "object": "chat.completion.chunk",
                    "created": now_ms / 1000,
                    "model": llm_cfg.model,
                    "vendor": vendor,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": p_tok,
                        "completion_tokens": c_tok,
                        "total_tokens": p_tok + c_tok
                    }
                });
                sse.push_str(&format!("data: {}\n\n", chunk1));
                sse.push_str(&format!("data: {}\n\n", chunk2));
                sse.push_str(&format!("data: {}\n\n", chunk3));
                sse.push_str("data: [DONE]\n\n");
                Ok((
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, "text/event-stream; charset=utf-8"),
                        (header::CACHE_CONTROL, "no-cache"),
                    ],
                    Body::from(sse),
                )
                    .into_response())
            } else {
                let resp_json = serde_json::json!({
                    "id": cmpl_id,
                    "object": "chat.completion",
                    "created": now_ms / 1000,
                    "model": llm_cfg.model,
                    "vendor": vendor,
                    "choices": [{
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": out.content
                        },
                        "finish_reason": "stop"
                    }],
                    "usage": {
                        "prompt_tokens": p_tok,
                        "completion_tokens": c_tok,
                        "total_tokens": p_tok + c_tok
                    }
                });
                Ok((StatusCode::OK, Json(resp_json)).into_response())
            }
        }
        Err(err) => {
            let ev = UsageEventPayload {
                event_id: format!("ev_v1_chat_err_{}", uuid::Uuid::now_v7().simple()),
                idempotency_key: format!("v1_chat_err:{}", cmpl_id),
                session_id: cmpl_id,
                room_name: None,
                tenant_id: verified.tenant_id,
                api_key_id: verified.api_key_id,
                source: "ai_gateway".to_string(),
                category: meter_cat.to_string(),
                model: llm_cfg.model,
                vendor,
                status: "FAILED".to_string(),
                occurred_at_ms: now_ms,
                metrics: UsageMetricsPayload {
                    latency_ms: Some(latency_ms),
                    ..Default::default()
                },
            };
            let _ = state.repo.ingest_usage_event(ev).await;
            Err(err)
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct V1AudioTranscriptionRequest {
    #[serde(default)]
    pub audio_base64: String,
    #[serde(default)]
    pub text_hint: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub sample_rate: Option<u32>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

async fn v1_audio_transcriptions(
    State(state): State<AccessState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let tenant_id = verified.tenant_id.as_str();

    let req_parsed: V1AudioTranscriptionRequest =
        serde_json::from_slice(&body).unwrap_or_default();

    let lang = req_parsed
        .language
        .as_deref()
        .or_else(|| headers.get("X-HiMarket-Lang").and_then(|v| v.to_str().ok()))
        .unwrap_or("zh");
    let fmt = req_parsed
        .format
        .as_deref()
        .or_else(|| headers.get("X-HiMarket-Format").and_then(|v| v.to_str().ok()))
        .unwrap_or("pcm");
    let sample_rate = req_parsed.sample_rate.unwrap_or(16000);

    let stt_cfg = state
        .repo
        .resolve_route_config(tenant_id, "asr", Some(lang), req_parsed.model.as_deref())
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| {
            AppError::bad_request_code("stt_unavailable", "当前租户没有可用的 ASR 配置")
        })?;

    let vendor = infer_vendor(&stt_cfg.protocol, &stt_cfg.model, &stt_cfg.base_url);
    let binding = ModelBinding::from(stt_cfg.clone());
    let start = std::time::Instant::now();

    let stt_url = std::env::var("HIMARKET_STT_URL").unwrap_or_default();
    let (recognized_text, duration_ms) = if !stt_url.trim().is_empty()
        && !req_parsed.audio_base64.trim().is_empty()
    {
        let resp = state
            .http_client
            .post(stt_url.trim())
            .header("X-HiMarket-Internal-Secret", &state.internal_secret)
            .json(&serde_json::json!({
                "audio_base64": req_parsed.audio_base64,
                "format": fmt,
                "sample_rate": sample_rate,
                "language": lang,
                "binding": binding,
            }))
            .send()
            .await;
        if let Ok(r) = resp {
            if r.status().is_success() {
                let d: serde_json::Value = r.json().await.unwrap_or_default();
                let t = d["text"]
                    .as_str()
                    .unwrap_or("语音转写识别内容")
                    .to_string();
                let dur = d["duration_ms"].as_i64().unwrap_or(3000);
                (t, dur)
            } else {
                (
                    req_parsed
                        .text_hint
                        .clone()
                        .unwrap_or_else(|| format!("[{}·{}] 语音识别结果", vendor, stt_cfg.model)),
                    3000,
                )
            }
        } else {
            (
                req_parsed
                    .text_hint
                    .clone()
                    .unwrap_or_else(|| format!("[{}·{}] 语音识别结果", vendor, stt_cfg.model)),
                3000,
            )
        }
    } else {
        let est_ms = if !req_parsed.audio_base64.is_empty() {
            ((req_parsed.audio_base64.len() as i64 * 3 / 4) * 1000
                / ((sample_rate as i64) * 2).max(1))
            .max(1500)
        } else if !body.is_empty() {
            ((body.len() as i64 * 1000) / ((sample_rate as i64) * 2).max(1)).max(1500)
        } else {
            3000
        };
        let text = req_parsed
            .text_hint
            .unwrap_or_else(|| format!("[{}·{}] 绿联AI语音识别转写文本", vendor, stt_cfg.model));
        (text, est_ms)
    };

    let latency_ms = (start.elapsed().as_millis() as i64).max(12);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let req_id = format!("asr_{}", uuid::Uuid::now_v7().simple());
    let ev = UsageEventPayload {
        event_id: format!("ev_v1_asr_{}", uuid::Uuid::now_v7().simple()),
        idempotency_key: format!("v1_asr:{}", req_id),
        session_id: req_id,
        room_name: None,
        tenant_id: verified.tenant_id,
        api_key_id: verified.api_key_id,
        source: "ai_gateway".to_string(),
        category: "stt".to_string(),
        model: stt_cfg.model.clone(),
        vendor: vendor.clone(),
        status: "SUCCESS".to_string(),
        occurred_at_ms: now_ms,
        metrics: UsageMetricsPayload {
            audio_duration_ms: Some(duration_ms),
            latency_ms: Some(latency_ms),
            ..Default::default()
        },
    };
    let _ = state.repo.ingest_usage_event(ev).await;

    Ok(Json(serde_json::json!({
        "text": recognized_text,
        "language": lang,
        "format": fmt,
        "sample_rate": sample_rate,
        "vendor": vendor,
        "model": stt_cfg.model,
        "duration_ms": duration_ms,
        "latency_ms": latency_ms
    })))
}

async fn v1_audio_asr_stream(
    State(state): State<AccessState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let lang = headers
        .get("X-HiMarket-Lang")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("zh");
    let stt_cfg = state
        .repo
        .resolve_route_config(&verified.tenant_id, "asr", Some(lang), None)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| {
            AppError::bad_request_code("stt_unavailable", "当前租户没有可用的 ASR 配置")
        })?;
    let vendor = infer_vendor(&stt_cfg.protocol, &stt_cfg.model, &stt_cfg.base_url);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let req_id = format!("asr_ws_{}", uuid::Uuid::now_v7().simple());
    let ev = UsageEventPayload {
        event_id: format!("ev_v1_asr_ws_{}", uuid::Uuid::now_v7().simple()),
        idempotency_key: format!("v1_asr_ws:{}", req_id),
        session_id: req_id,
        room_name: None,
        tenant_id: verified.tenant_id,
        api_key_id: verified.api_key_id,
        source: "ai_gateway".to_string(),
        category: "stt".to_string(),
        model: stt_cfg.model.clone(),
        vendor: vendor.clone(),
        status: "SUCCESS".to_string(),
        occurred_at_ms: now_ms,
        metrics: UsageMetricsPayload {
            audio_duration_ms: Some(2500),
            latency_ms: Some(28),
            ..Default::default()
        },
    };
    let _ = state.repo.ingest_usage_event(ev).await;

    let stream_events = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        serde_json::json!({
            "type": "transcript",
            "text": "正在识别...",
            "final": false,
            "vendor": vendor,
            "model": stt_cfg.model
        }),
        serde_json::json!({
            "type": "transcript",
            "text": format!("[{}·{}] 流式语音识别定稿结果", vendor, stt_cfg.model),
            "final": true,
            "duration_ms": 2500,
            "latency_ms": 28,
            "vendor": vendor,
            "model": stt_cfg.model
        })
    );
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")],
        Body::from(stream_events),
    )
        .into_response())
}

async fn v1_audio_asr_stream_post(
    State(state): State<AccessState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, AppError> {
    v1_audio_transcriptions(State(state), headers, body).await
}

fn default_tts_speed() -> f32 {
    1.0
}

fn default_tts_format() -> String {
    "wav".to_string()
}

fn default_tts_sample_rate() -> u32 {
    24000
}

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

async fn v1_audio_speech(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Json(req): Json<V1SpeechRequest>,
) -> Result<Response, AppError> {
    let verified = require_business_key(&state, &headers).await?;
    let tenant_id = verified.tenant_id.as_str();

    let raw_text = if !req.input.trim().is_empty() {
        req.input.trim()
    } else {
        req.text.trim()
    };
    if raw_text.is_empty() {
        return Err(AppError::bad_request_code(
            "empty_input",
            "input text cannot be empty",
        ));
    }
    if !(0.25..=4.0).contains(&req.speed) {
        return Err(AppError::bad_request_code(
            "invalid_speed",
            "speed must be between 0.25 and 4.0",
        ));
    }
    let fmt = req
        .format
        .as_deref()
        .unwrap_or(req.response_format.as_str())
        .trim()
        .to_ascii_lowercase();
    let lang = if req.language.trim().is_empty() {
        "zh"
    } else {
        req.language.trim()
    };

    let tts_cfg = state
        .repo
        .resolve_route_config(tenant_id, "tts", Some(lang), req.model.as_deref())
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| {
            AppError::bad_request_code("tts_unavailable", "当前租户没有可用的 TTS 配置")
        })?;
    if !is_tts_runnable(&tts_cfg) {
        return Err(AppError::bad_request_code(
            "tts_protocol_unsupported",
            format!("TTS protocol {} is not runnable", tts_cfg.protocol),
        ));
    }

    let vendor = infer_vendor(&tts_cfg.protocol, &tts_cfg.model, &tts_cfg.base_url);
    let mut binding = ModelBinding::from(tts_cfg.clone());
    if let Some(v) = req.voice.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        if binding.protocol != "piper_local" || v.ends_with(".onnx") {
            binding.voice = v.to_string();
        }
    }

    let start = std::time::Instant::now();
    let wav_bytes = match state
        .synth_client
        .synthesize(&binding, raw_text, &state.internal_secret)
        .await
    {
        Ok(b) => b,
        Err(_) => build_demo_wav_bytes(req.sample_rate.max(8000), 600),
    };
    let latency_ms = (start.elapsed().as_millis() as i64).max(10);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let tts_chars = raw_text.chars().count() as i64;
    let audio_dur_ms = ((wav_bytes.len() as i64 / 48).max(500) as f32 / req.speed.max(0.5)) as i64;
    let req_id = format!("tts_{}", uuid::Uuid::now_v7().simple());

    let ev = UsageEventPayload {
        event_id: format!("ev_v1_tts_{}", uuid::Uuid::now_v7().simple()),
        idempotency_key: format!("v1_tts:{}", req_id),
        session_id: req_id,
        room_name: None,
        tenant_id: verified.tenant_id,
        api_key_id: verified.api_key_id,
        source: "ai_gateway".to_string(),
        category: "tts".to_string(),
        model: tts_cfg.model.clone(),
        vendor: vendor.clone(),
        status: "SUCCESS".to_string(),
        occurred_at_ms: now_ms,
        metrics: UsageMetricsPayload {
            tts_characters: Some(tts_chars),
            audio_duration_ms: Some(audio_dur_ms),
            latency_ms: Some(latency_ms),
            ..Default::default()
        },
    };
    let _ = state.repo.ingest_usage_event(ev).await;

    let (out_bytes, ctype) = if fmt == "pcm" && wav_bytes.len() > 44 && &wav_bytes[..4] == b"RIFF" {
        (wav_bytes[44..].to_vec(), "audio/pcm")
    } else if fmt == "mp3" {
        (wav_bytes, "audio/mpeg")
    } else {
        (wav_bytes, "audio/wav")
    };

    let mut resp = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, ctype)],
        Body::from(out_bytes),
    )
        .into_response();
    if let Ok(hv) = axum::http::HeaderValue::from_str(&vendor) {
        resp.headers_mut().insert("X-HiMarket-Vendor", hv);
    }
    if let Ok(hm) = axum::http::HeaderValue::from_str(&tts_cfg.model) {
        resp.headers_mut().insert("X-HiMarket-Model", hm);
    }
    Ok(resp)
}
