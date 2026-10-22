use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub code: u16,
    pub message: String,
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Json<Self> {
        Json(Self {
            code: 200,
            message: "success".to_string(),
            data: Some(data),
            error: None,
        })
    }

    pub fn err(code: u16, message: impl Into<String>) -> Json<ApiResponse<()>> {
        let msg = message.into();
        let err_code = extract_machine_code(&msg);
        Json(ApiResponse {
            code,
            message: msg,
            data: None,
            error: err_code,
        })
    }
}

pub fn extract_machine_code(msg: &str) -> Option<String> {
    let trimmed = msg
        .strip_prefix("Bad Request: ")
        .or_else(|| msg.strip_prefix("Unauthorized: "))
        .or_else(|| msg.strip_prefix("Forbidden: "))
        .or_else(|| msg.strip_prefix("Not Found: "))
        .or_else(|| msg.strip_prefix("Conflict: "))
        .or_else(|| msg.strip_prefix("Bad Gateway: "))
        .or_else(|| msg.strip_prefix("Internal error: "))
        .unwrap_or(msg)
        .trim();
    let candidate = trimmed.split(':').next()?.trim();
    if !candidate.is_empty()
        && candidate.contains('_')
        && candidate
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        Some(candidate.to_string())
    } else {
        None
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
    #[error("Conflict: {message}")]
    Conflict {
        code: String,
        message: String,
        retry_after: Option<u32>,
    },
    #[error("Bad Gateway: {0}")]
    BadGateway(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

impl AppError {
    pub fn bad_request_code(code: &str, detail: impl Into<String>) -> Self {
        Self::BadRequest(format!("{}: {}", code, detail.into()))
    }

    pub fn bad_gateway_code(code: &str, detail: impl Into<String>) -> Self {
        Self::BadGateway(format!("{}: {}", code, detail.into()))
    }

    pub fn conflict_code(code: &str, detail: impl Into<String>, retry_after: Option<u32>) -> Self {
        let code_str = code.to_string();
        let msg = format!("{}: {}", code_str, detail.into());
        Self::Conflict {
            code: code_str,
            message: msg,
            retry_after,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, err_code, retry_after, message) = match &self {
            AppError::Unauthorized(m) => {
                (StatusCode::UNAUTHORIZED, 401, extract_machine_code(m), None, self.to_string())
            }
            AppError::Forbidden(m) => {
                (StatusCode::FORBIDDEN, 403, extract_machine_code(m), None, self.to_string())
            }
            AppError::BadRequest(m) => {
                let ec = extract_machine_code(m);
                let msg = if ec.is_some() { m.clone() } else { self.to_string() };
                (StatusCode::BAD_REQUEST, 400, ec, None, msg)
            }
            AppError::NotFound(m) => {
                (StatusCode::NOT_FOUND, 404, extract_machine_code(m), None, self.to_string())
            }
            AppError::Conflict {
                code: c,
                message: m,
                retry_after,
            } => (
                StatusCode::CONFLICT,
                409,
                Some(c.clone()),
                *retry_after,
                m.clone(),
            ),
            AppError::BadGateway(m) => {
                let ec = extract_machine_code(m);
                let msg = if ec.is_some() { m.clone() } else { self.to_string() };
                (StatusCode::BAD_GATEWAY, 502, ec, None, msg)
            }
            AppError::Internal(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                extract_machine_code(m),
                None,
                self.to_string(),
            ),
        };
        let body = Json(ApiResponse::<()> {
            code,
            message,
            data: None,
            error: err_code,
        });
        let mut resp = (status, body).into_response();
        if let Some(secs) = retry_after {
            if let Ok(val) = HeaderValue::from_str(&secs.to_string()) {
                resp.headers_mut().insert(header::RETRY_AFTER, val);
            }
        }
        resp
    }
}

pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn normalize_lang(raw: &str) -> Result<String, String> {
    let cleaned = raw.trim().replace('_', "-");
    if cleaned.is_empty() {
        return Err("language tag cannot be empty".to_string());
    }
    let parts: Vec<&str> = cleaned.split('-').collect();
    if parts.is_empty() || parts.len() > 2 {
        return Err(format!("invalid language tag: {}", raw));
    }
    let primary = parts[0];
    if !(2..=3).contains(&primary.len()) || !primary.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(format!("invalid primary language tag: {}", raw));
    }
    let primary_norm = primary.to_ascii_lowercase();
    if parts.len() == 1 {
        return Ok(primary_norm);
    }
    let region = parts[1];
    if !(2..=3).contains(&region.len()) || !region.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(format!("invalid region subtag: {}", raw));
    }
    Ok(format!("{}-{}", primary_norm, region.to_ascii_uppercase()))
}

pub fn primary_lang_subtag(tag: &str) -> Option<String> {
    let norm = normalize_lang(tag).ok()?;
    Some(norm.split('-').next()?.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantLanguagePolicy {
    pub tenant_id: String,
    pub default_target_lang: String,
    pub allowed_langs: Vec<String>,
}

pub fn validate_language_policy(
    tenant_id: &str,
    default_target_lang: &str,
    allowed_langs: &[String],
) -> Result<TenantLanguagePolicy, String> {
    let norm_default = normalize_lang(default_target_lang)?;
    if allowed_langs.is_empty() {
        return Err("allowed_langs cannot be empty".to_string());
    }
    let mut norm_allowed = Vec::with_capacity(allowed_langs.len());
    for item in allowed_langs {
        let n = normalize_lang(item)?;
        if !norm_allowed.contains(&n) {
            norm_allowed.push(n);
        }
    }
    if !norm_allowed.contains(&norm_default) {
        return Err(format!(
            "default_target_lang {} must be in allowed_langs {:?}",
            norm_default, norm_allowed
        ));
    }
    Ok(TenantLanguagePolicy {
        tenant_id: tenant_id.to_string(),
        default_target_lang: norm_default,
        allowed_langs: norm_allowed,
    })
}

pub fn ensure_allowed(
    requested: Option<&str>,
    policy: &TenantLanguagePolicy,
) -> Result<String, String> {
    let raw = requested.map(str::trim).unwrap_or("");
    let norm = if raw.is_empty() {
        normalize_lang(&policy.default_target_lang)
            .map_err(|e| format!("target_lang_not_allowed: {}", e))?
    } else {
        normalize_lang(raw).map_err(|e| format!("target_lang_not_allowed: {}", e))?
    };
    if policy.allowed_langs.iter().any(|a| a == &norm) {
        Ok(norm)
    } else {
        Err(format!(
            "target_lang_not_allowed: {} is not allowed for tenant {}",
            norm, policy.tenant_id
        ))
    }
}

pub fn task_steps(task: &str, speak: bool) -> Result<Vec<&'static str>, String> {
    match task {
        "cascade" => Ok(vec!["stt", "llm", "tts", "append"]),
        "transcribe" => Ok(vec!["stt", "append"]),
        "speech_translate" => {
            if speak {
                Ok(vec!["stt", "llm", "tts", "append"])
            } else {
                Ok(vec!["stt", "llm", "append"])
            }
        }
        "text_translate" => Ok(vec!["llm", "append"]),
        "speak" => Ok(vec!["tts"]),
        "context" => Ok(vec!["append"]),
        other => Err(format!("task_rejected: unsupported task {}", other)),
    }
}

pub fn split_speak_segments(text: &str) -> Result<Vec<String>, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("empty_input: speak text cannot be empty".to_string());
    }
    let total_chars = trimmed.chars().count();
    if total_chars > 4000 {
        return Err(format!(
            "speak_text_too_long: text length {} exceeds 4000 limit",
            total_chars
        ));
    }

    let is_boundary = |c: char| matches!(c, '。' | '！' | '？' | '.' | '!' | '?' | '\n' | '；' | ';');
    let mut sentences: Vec<String> = Vec::new();
    let mut current = String::new();
    for ch in trimmed.chars() {
        current.push(ch);
        if is_boundary(ch) {
            let s = current.trim().to_string();
            if !s.is_empty() {
                sentences.push(s);
            }
            current.clear();
        }
    }
    if !current.trim().is_empty() {
        sentences.push(current.trim().to_string());
    }

    let mut segments: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut buf_len = 0usize;

    for sentence in sentences {
        let s_len = sentence.chars().count();
        if s_len > 400 {
            if !buf.is_empty() {
                segments.push(std::mem::take(&mut buf));
                buf_len = 0;
            }
            let chars: Vec<char> = sentence.chars().collect();
            for chunk in chars.chunks(400) {
                let piece: String = chunk.iter().collect();
                if !piece.trim().is_empty() {
                    segments.push(piece);
                }
            }
        } else if buf_len + s_len <= 400 {
            if !buf.is_empty() {
                if buf_len + 1 + s_len <= 400 {
                    buf.push(' ');
                    buf.push_str(&sentence);
                    buf_len += 1 + s_len;
                } else {
                    segments.push(std::mem::take(&mut buf));
                    buf = sentence;
                    buf_len = s_len;
                }
            } else {
                buf = sentence;
                buf_len = s_len;
            }
        } else {
            if !buf.is_empty() {
                segments.push(std::mem::take(&mut buf));
            }
            buf = sentence;
            buf_len = s_len;
        }
    }
    if !buf.is_empty() {
        segments.push(buf);
    }
    Ok(segments)
}

pub fn extract_summary_title_and_body(raw: &str) -> (String, String) {
    let trimmed = raw.trim();
    let first_non_empty = trimmed
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    let title = if first_non_empty.starts_with('#') {
        let stripped = first_non_empty.trim_start_matches('#').trim();
        stripped.chars().take(40).collect::<String>()
    } else {
        first_non_empty.chars().take(20).collect::<String>()
    };
    let final_title = if title.is_empty() {
        "AI总结".to_string()
    } else {
        title
    };
    (final_title, trimmed.to_string())
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
    #[serde(default = "default_task")]
    pub task: String,
    #[serde(default)]
    pub llm_model: Option<String>,
    pub stt_model: Option<String>,
    pub tts_voice: Option<String>,
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub greeting_text: Option<String>,
    #[serde(default)]
    pub source_lang: String,
    #[serde(default)]
    pub target_lang: String,
    #[serde(default)]
    pub speak: bool,
}

impl Default for CreateVoiceSessionRequest {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            task: default_task(),
            llm_model: None,
            stt_model: None,
            tts_voice: None,
            system_prompt: None,
            greeting_text: None,
            source_lang: String::new(),
            target_lang: String::new(),
            speak: false,
        }
    }
}

fn default_mode() -> String {
    "cascade".to_string()
}

fn default_task() -> String {
    "cascade".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVoiceSessionResponse {
    pub session_id: String,
    pub room_name: Option<String>,
    pub livekit_ws_url: Option<String>,
    pub participant_token: Option<String>,
    pub expires_in_seconds: u64,
    pub tenant_id: String,
    pub api_key_id: String,
    pub mode: String,
    pub task: String,
    pub llm_model: Option<String>,
    pub source_lang: String,
    pub target_lang: String,
    pub speak: bool,
    pub voice_fallback: bool,
    #[serde(default)]
    pub stt_fallback: bool,
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
    #[serde(default)]
    pub translate: TextLlmUsageBreakdown,
    #[serde(default)]
    pub summary: TextLlmUsageBreakdown,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_lang() {
        assert_eq!(normalize_lang("ja-jp").unwrap(), "ja-JP");
        assert_eq!(normalize_lang("zh_CN").unwrap(), "zh-CN");
        assert_eq!(normalize_lang("  EN ").unwrap(), "en");
        assert!(normalize_lang("*").is_err());
        assert!(normalize_lang("").is_err());
        assert!(normalize_lang("12").is_err());
        assert!(normalize_lang("abcd").is_err());
    }

    #[test]
    fn test_ensure_allowed_and_policy_validation() {
        let policy = validate_language_policy(
            "tenant_ugreen_01",
            "zh",
            &["zh".into(), "en".into(), "ja".into(), "ko".into()],
        )
        .unwrap();
        assert_eq!(ensure_allowed(None, &policy).unwrap(), "zh");
        assert_eq!(ensure_allowed(Some(""), &policy).unwrap(), "zh");
        assert_eq!(ensure_allowed(Some("JA"), &policy).unwrap(), "ja");
        assert!(ensure_allowed(Some("fr"), &policy).is_err());
        assert!(
            validate_language_policy("tenant_ugreen_01", "fr", &["zh".into(), "en".into()])
                .is_err()
        );
    }

    #[test]
    fn test_task_steps() {
        let tr = task_steps("transcribe", false).unwrap();
        assert_eq!(tr, vec!["stt", "append"]);
        let st_no_speak = task_steps("speech_translate", false).unwrap();
        assert_eq!(st_no_speak, vec!["stt", "llm", "append"]);
        let st_speak = task_steps("speech_translate", true).unwrap();
        assert_eq!(st_speak, vec!["stt", "llm", "tts", "append"]);
        let tt = task_steps("text_translate", false).unwrap();
        assert_eq!(tt, vec!["llm", "append"]);
        assert!(task_steps("summarize", false).is_err());
    }

    #[test]
    fn test_split_speak_segments() {
        assert!(split_speak_segments("   ").is_err());
        let over = "啊".repeat(4001);
        assert!(split_speak_segments(&over).unwrap_err().starts_with("speak_text_too_long"));

        let s401 = "阿".repeat(401);
        let segs = split_speak_segments(&s401).unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].chars().count(), 400);
        assert_eq!(segs[1].chars().count(), 1);

        let s4000 = "好".repeat(4000);
        let segs4000 = split_speak_segments(&s4000).unwrap();
        assert_eq!(segs4000.len(), 10);
        assert!(segs4000.iter().all(|s| s.chars().count() == 400));
    }

    #[test]
    fn test_extract_summary_title() {
        let md = "# 关于“做自己”与MBTI测试的自我探索\n\n## 核心摘要\n内容...";
        let (title, body) = extract_summary_title_and_body(md);
        assert_eq!(title, "关于“做自己”与MBTI测试的自我探索");
        assert!(body.contains("## 核心摘要"));

        let plain = "这是一段没有井号标题的总结正文内容超过二十个字测试截断";
        let (fallback_title, _) = extract_summary_title_and_body(plain);
        assert_eq!(fallback_title.chars().count(), 20);
    }
}
