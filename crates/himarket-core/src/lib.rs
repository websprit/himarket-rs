use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub code: u16,
    pub message: String,
    pub data: Option<T>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Json<Self> {
        Json(Self {
            code: 200,
            message: "success".to_string(),
            data: Some(data),
        })
    }

    pub fn err(code: u16, message: impl Into<String>) -> Json<ApiResponse<()>> {
        Json(ApiResponse {
            code,
            message: message.into(),
            data: None,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Forbidden: {0}")]
    Forbidden(String),
    #[error("Bad Request: {0}")]
    BadRequest(String),
    #[error("Not Found: {0}")]
    NotFound(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            AppError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, 401),
            AppError::Forbidden(_) => (StatusCode::FORBIDDEN, 403),
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, 400),
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, 404),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, 500),
        };
        let body = Json(ApiResponse::<()> {
            code,
            message: self.to_string(),
            data: None,
        });
        (status, body).into_response()
    }
}

pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoiceMode {
    Realtime,
    Cascade,
}

impl VoiceMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            VoiceMode::Realtime => "realtime",
            VoiceMode::Cascade => "cascade",
        }
    }
}

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
}

impl VerifiedBusinessKey {
    pub fn has_quota(&self) -> bool {
        if self.status != "ACTIVE" {
            return false;
        }
        match self.quota_limit_usd_micros {
            Some(limit) => self.quota_used_usd_micros < limit,
            None => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVoiceSessionRequest {
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "default_llm_model")]
    pub llm_model: String,
    pub stt_model: Option<String>,
    pub tts_voice: Option<String>,
    pub system_prompt: Option<String>,
}

fn default_mode() -> String {
    "cascade".to_string()
}

fn default_llm_model() -> String {
    "deepseek-v4.1-flash:1017-cloud".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVoiceSessionResponse {
    pub session_id: String,
    pub room_name: String,
    pub livekit_ws_url: String,
    pub participant_token: String,
    pub expires_in_seconds: u64,
    pub tenant_id: String,
    pub api_key_id: String,
    pub mode: String,
    pub llm_model: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageMetricsPayload {
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub audio_input_tokens: Option<i64>,
    pub audio_output_tokens: Option<i64>,
    pub audio_duration_ms: Option<i64>,
    pub tts_characters: Option<i64>,
    pub rtc_duration_ms: Option<i64>,
    pub rtc_bytes: Option<i64>,
    pub ttft_ms: Option<i64>,
    pub latency_ms: Option<i64>,
    #[serde(default)]
    pub cost_usd_micros: i64,
}

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
    pub occurred_at_ms: i64,
    pub metrics: UsageMetricsPayload,
}

impl UsageEventPayload {
    /// Automatically estimate cost in USD micros if caller left cost_usd_micros == 0
    pub fn compute_cost_micros(&self) -> i64 {
        if self.metrics.cost_usd_micros > 0 {
            return self.metrics.cost_usd_micros;
        }
        let m = &self.metrics;
        let prompt = m.prompt_tokens.unwrap_or(0) * 2; // $2 / 1M tok = 2 micros / tok
        let completion = m.completion_tokens.unwrap_or(0) * 6;
        let audio_in = m.audio_input_tokens.unwrap_or(0) * 15;
        let audio_out = m.audio_output_tokens.unwrap_or(0) * 30;
        let audio_sec = (m.audio_duration_ms.unwrap_or(0) / 1000) * 100; // $0.0001 / sec
        let tts_char = m.tts_characters.unwrap_or(0) * 15;
        let rtc_sec = (m.rtc_duration_ms.unwrap_or(0) / 1000) * 40;
        prompt + completion + audio_in + audio_out + audio_sec + tts_char + rtc_sec
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageSummaryResponse {
    pub tenant_id: String,
    pub api_key_id: Option<String>,
    pub total_events: i64,
    pub total_cost_usd_micros: i64,
    pub total_cost_usd: String,
    pub rtc: RtcUsageBreakdown,
    pub voice_models: VoiceModelUsageBreakdown,
    pub text_llm: TextLlmUsageBreakdown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RtcUsageBreakdown {
    pub rtc_duration_ms: i64,
    pub rtc_bytes: i64,
    pub cost_usd_micros: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VoiceModelUsageBreakdown {
    pub audio_input_tokens: i64,
    pub audio_output_tokens: i64,
    pub stt_audio_duration_ms: i64,
    pub tts_audio_duration_ms: i64,
    pub tts_characters: i64,
    pub cost_usd_micros: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TextLlmUsageBreakdown {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub cached_tokens: i64,
    pub cost_usd_micros: i64,
}
