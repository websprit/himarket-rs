use anyhow::Result;
use chrono::{DateTime, Utc};
use himarket_core::{
    CreateVoiceSessionRequest, QuotaAlertDto, RtcUsageBreakdown, TenantLanguagePolicy,
    TextLlmUsageBreakdown, UsageEventPayload, UsageReportResponse, UsageReportRow,
    UsageSummaryResponse, VerifiedBusinessKey, VoiceModelUsageBreakdown, decrypt_secret,
    encrypt_secret, ensure_allowed, infer_vendor, normalize_lang, primary_lang_subtag, sha256_hex,
    validate_language_policy,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DalRepo {
    pool: PgPool,
    key_cache: Arc<RwLock<HashMap<String, VerifiedBusinessKey>>>,
    session_cache: Arc<RwLock<HashMap<String, SessionIdentity>>>,
    auth_sessions: Arc<RwLock<HashMap<String, AuthSession>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSession {
    pub token: String,
    pub role: String,
    pub subject_id: String,
    pub username: String,
    pub portal_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyDto {
    pub api_key_id: String,
    pub tenant_id: String,
    pub portal_id: String,
    pub consumer_id: String,
    pub status: String,
    pub allowed_modes: String,
    pub quota_used_usd_micros: i64,
    pub quota_limit_usd_micros: Option<i64>,
    pub quota_used_calls: i64,
    pub quota_limit_calls: Option<i64>,
    pub quota_used_tokens: i64,
    pub quota_limit_tokens: Option<i64>,
    pub alert_threshold_pct: i32,
    pub alert_webhook_url: String,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyCreated {
    pub api_key_id: String,
    pub raw_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub config_id: String,
    pub tenant_id: String,
    pub name: String,
    pub kind: String,
    pub protocol: String,
    pub base_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub extra_json: String,
    pub model: String,
    pub voice: String,
    pub is_default: bool,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedTts {
    pub config: ModelConfig,
    pub voice_fallback: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtsSelectError {
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedStt {
    pub config: ModelConfig,
    pub stt_fallback: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SttSelectError {
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceSessionDto {
    pub session_id: String,
    pub room_name: Option<String>,
    pub tenant_id: String,
    pub api_key_id: String,
    pub voice_mode: String,
    pub task: String,
    pub source_lang: String,
    pub target_lang: String,
    pub title: String,
    pub llm_model: Option<String>,
    pub state: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionIdentity {
    pub session_id: String,
    pub room_name: Option<String>,
    pub tenant_id: String,
    pub portal_id: String,
    pub consumer_id: String,
    pub api_key_id: String,
    pub voice_mode: String,
    pub task: String,
    pub llm_model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecordFull {
    pub session_id: String,
    pub room_name: Option<String>,
    pub tenant_id: String,
    pub portal_id: String,
    pub consumer_id: String,
    pub api_key_id: String,
    pub voice_mode: String,
    pub task: String,
    pub source_lang: String,
    pub target_lang: String,
    pub title: String,
    pub speak: bool,
    pub stt_config_id: Option<String>,
    pub llm_config_id: Option<String>,
    pub tts_config_id: Option<String>,
    pub llm_model: Option<String>,
    pub speak_text: Option<String>,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryInputRow {
    pub seq: i64,
    pub origin: String,
    pub source_text: Option<String>,
    pub translated_text: Option<String>,
    pub assistant_text: Option<String>,
    pub client_input_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendSummaryInputResult {
    pub seq: i64,
    pub replayed: bool,
    pub source_text: Option<String>,
    pub translated_text: Option<String>,
    pub assistant_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryResultRow {
    pub session_id: String,
    pub result_seq: i64,
    pub request_id: String,
    pub status: String,
    pub title: String,
    pub summary_style: String,
    pub summary_text: Option<String>,
    pub error_code: Option<String>,
    pub input_seq_from: i64,
    pub input_seq_to: i64,
    pub truncated_inputs: i32,
    pub target_lang: String,
}

#[derive(Debug, Clone)]
pub enum BeginSummaryOutcome {
    Owned { result_seq: i64 },
    Ready(SummaryResultRow),
    Failed(SummaryResultRow),
    Busy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortalDto {
    pub portal_id: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeveloperDto {
    pub developer_id: String,
    pub username: String,
    pub email: Option<String>,
    pub portal_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductDto {
    pub product_id: String,
    pub name: String,
    pub product_type: String,
    pub description: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerDto {
    pub consumer_id: String,
    pub name: String,
    pub portal_id: String,
    pub developer_id: String,
}

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

pub fn is_llm_runnable(cfg: &ModelConfig) -> bool {
    if cfg.status != "ACTIVE" || (cfg.kind != "llm" && cfg.kind != "translate") {
        return false;
    }
    match cfg.protocol.as_str() {
        "openai_chat" | "livekit_inference" => true,
        _ => false,
    }
}

pub fn parse_extra_lang_list(extra_json: &str, key: &str) -> Vec<String> {
    let Ok(val) = serde_json::from_str::<serde_json::Value>(extra_json) else {
        return Vec::new();
    };
    let Some(arr) = val.get(key).and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in arr {
        if let Some(s) = item.as_str() {
            if let Ok(norm) = normalize_lang(s) {
                if !out.contains(&norm) {
                    out.push(norm);
                }
            }
        }
    }
    out
}

pub fn infer_piper_filename_langs(model: &str, voice: &str) -> Vec<String> {
    const RECOGNIZED: &[&str] = &[
        "zh", "zh-CN", "zh-TW", "en", "en-US", "en-GB", "ja", "ja-JP", "ko", "ko-KR", "de",
        "es", "fr", "id", "it",
    ];
    let mut out = Vec::new();
    for raw in [model, voice] {
        let base = raw.rsplit('/').next().unwrap_or(raw);
        let base = base
            .strip_suffix(".onnx.json")
            .or_else(|| base.strip_suffix(".onnx"))
            .unwrap_or(base);
        let normalized_sep = base.replace('_', "-");
        let segments: Vec<&str> = normalized_sep
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|s| !s.is_empty())
            .collect();
        for win in segments.windows(2) {
            let candidate = format!("{}-{}", win[0], win[1]);
            if let Ok(norm) = normalize_lang(&candidate) {
                if RECOGNIZED.contains(&norm.as_str()) {
                    if !out.contains(&norm) {
                        out.push(norm.clone());
                    }
                    if let Some(primary) = primary_lang_subtag(&norm) {
                        if !out.contains(&primary) {
                            out.push(primary);
                        }
                    }
                }
            }
        }
        for seg in segments {
            if let Ok(norm) = normalize_lang(seg) {
                if RECOGNIZED.contains(&norm.as_str()) && !out.contains(&norm) {
                    out.push(norm);
                }
            }
        }
    }
    out
}

pub fn tts_candidate_langs(cfg: &ModelConfig) -> Vec<String> {
    if !is_tts_runnable(cfg) {
        return Vec::new();
    }
    let explicit = parse_extra_lang_list(&cfg.extra_json, "tts_languages");
    if !explicit.is_empty() {
        return explicit;
    }
    if cfg.protocol == "piper_local" {
        return infer_piper_filename_langs(&cfg.model, &cfg.voice);
    }
    Vec::new()
}

pub fn tts_matches_target_lang(cfg: &ModelConfig, target_lang: &str) -> bool {
    let Ok(norm_target) = normalize_lang(target_lang) else {
        return false;
    };
    let Some(target_primary) = primary_lang_subtag(&norm_target) else {
        return false;
    };
    let langs = tts_candidate_langs(cfg);
    langs.iter().any(|l| {
        l == &norm_target || primary_lang_subtag(l).as_deref() == Some(target_primary.as_str())
    })
}

pub fn format_summary_materials(
    task: &str,
    summary_source: Option<&str>,
    rows: &[SummaryInputRow],
) -> Result<(String, String, usize, usize, i64, i64), String> {
    let mode = match summary_source.map(str::trim).filter(|s| !s.is_empty()) {
        Some("source") => "source",
        Some("translated") => "translated",
        Some("both") => "both",
        Some("turns") => "turns",
        Some(other) => {
            return Err(format!(
                "empty_input: unsupported summary_source {}",
                other
            ));
        }
        None => match task {
            "speech_translate" | "text_translate" => "translated",
            "cascade" => "turns",
            _ => "source",
        },
    };

    let mut formatted_items: Vec<(i64, String)> = Vec::new();
    for r in rows {
        let src = r.source_text.as_deref().map(str::trim).filter(|s| !s.is_empty());
        let tr = r
            .translated_text
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let ast = r
            .assistant_text
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let line = match mode {
            "source" => src.map(|s| format!("[{}] ({}) {}", r.seq, r.origin, s)),
            "translated" => {
                if r.origin == "text" {
                    src.map(|s| format!("[{}] [note] {}", r.seq, s))
                } else {
                    tr.map(|t| format!("[{}] ({}) {}", r.seq, r.origin, t))
                }
            }
            "both" => {
                if r.origin == "text" && tr.is_none() {
                    src.map(|s| format!("[{}] [note] {}", r.seq, s))
                } else {
                    let mut parts = Vec::new();
                    if let Some(s) = src {
                        parts.push(format!("[{}] ({}) source: {}", r.seq, r.origin, s));
                    }
                    if let Some(t) = tr {
                        if parts.is_empty() {
                            parts.push(format!("[{}] ({}) translation: {}", r.seq, r.origin, t));
                        } else {
                            parts.push(format!("translation: {}", t));
                        }
                    }
                    if parts.is_empty() {
                        None
                    } else {
                        Some(parts.join("\n"))
                    }
                }
            }
            "turns" => {
                if r.origin == "text" && ast.is_none() {
                    src.map(|s| format!("[{}] [note] {}", r.seq, s))
                } else {
                    let mut parts = Vec::new();
                    if let Some(s) = src {
                        parts.push(format!("[{}] ({}) user: {}", r.seq, r.origin, s));
                    }
                    if let Some(a) = ast {
                        if parts.is_empty() {
                            parts.push(format!("[{}] ({}) assistant: {}", r.seq, r.origin, a));
                        } else {
                            parts.push(format!("assistant: {}", a));
                        }
                    }
                    if parts.is_empty() {
                        None
                    } else {
                        Some(parts.join("\n"))
                    }
                }
            }
            _ => None,
        };
        if let Some(l) = line {
            formatted_items.push((r.seq, l));
        }
    }

    if formatted_items.is_empty() {
        return Err("empty_input: no summary materials available".to_string());
    }

    let total_candidates = formatted_items.len();
    let mut start_idx = 0usize;
    let total_chars = |slice: &[(i64, String)]| -> usize {
        slice.iter().map(|(_, s)| s.chars().count() + 1).sum()
    };
    while start_idx + 1 < formatted_items.len() && total_chars(&formatted_items[start_idx..]) > 24000
    {
        start_idx += 1;
    }
    let kept = &formatted_items[start_idx..];
    let seq_from = kept.first().map(|(seq, _)| *seq).unwrap_or(0);
    let seq_to = kept.last().map(|(seq, _)| *seq).unwrap_or(0);
    let mut joined = kept
        .iter()
        .map(|(_, s)| s.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if joined.chars().count() > 24000 {
        joined = joined.chars().take(24000).collect();
    }
    let truncated_inputs = total_candidates.saturating_sub(kept.len());
    Ok((
        mode.to_string(),
        joined,
        kept.len(),
        truncated_inputs,
        seq_from,
        seq_to,
    ))
}

impl DalRepo {
    pub async fn connect_and_init(db_url: &str) -> Result<Self> {
        Self::connect(db_url, false).await
    }

    /// Drop and recreate `public` before seeding. Tests use this so each run
    /// starts from an empty external database. Production startup does not.
    pub async fn connect_and_reset(db_url: &str) -> Result<Self> {
        Self::connect(db_url, true).await
    }

    async fn connect(db_url: &str, reset_schema: bool) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(db_url)
            .await?;
        if reset_schema {
            sqlx::query("DROP SCHEMA IF EXISTS public CASCADE")
                .execute(&pool)
                .await?;
            sqlx::query("CREATE SCHEMA public").execute(&pool).await?;
        }

        let repo = Self {
            pool,
            key_cache: Arc::new(RwLock::new(HashMap::new())),
            session_cache: Arc::new(RwLock::new(HashMap::new())),
            auth_sessions: Arc::new(RwLock::new(HashMap::new())),
        };
        repo.init_schema_and_seed().await?;
        Ok(repo)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn raw_non_empty_api_keys(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT config_id, api_key FROM ai_model_config WHERE api_key <> ''",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get("config_id"), r.get("api_key")))
            .collect())
    }

    async fn init_schema_and_seed(&self) -> Result<()> {
        let ddl = r#"
        CREATE TABLE IF NOT EXISTS administrator (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            admin_id TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS portal (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            portal_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            admin_id TEXT,
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS developer (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            developer_id TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL,
            email TEXT,
            portal_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            password_hash TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS consumer (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            consumer_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            portal_id TEXT NOT NULL,
            developer_id TEXT NOT NULL,
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS product (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            product_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL UNIQUE,
            type TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'PUBLISHED',
            auto_approve INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS product_subscription (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            product_id TEXT NOT NULL,
            consumer_id TEXT NOT NULL,
            developer_id TEXT NOT NULL,
            portal_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            UNIQUE(product_id, consumer_id)
        );
        CREATE TABLE IF NOT EXISTS voice_api_key_binding (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            api_key_id TEXT NOT NULL UNIQUE,
            api_key_sha256 TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL,
            portal_id TEXT NOT NULL,
            consumer_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            allowed_modes TEXT NOT NULL DEFAULT 'realtime,cascade',
            rate_limit_rpm INTEGER NOT NULL DEFAULT 120,
            quota_limit_usd_micros BIGINT,
            quota_used_usd_micros BIGINT NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS voice_session_record (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            session_id TEXT NOT NULL UNIQUE,
            room_name TEXT UNIQUE,
            tenant_id TEXT NOT NULL,
            portal_id TEXT NOT NULL,
            consumer_id TEXT NOT NULL,
            api_key_id TEXT NOT NULL,
            voice_mode TEXT NOT NULL,
            task TEXT NOT NULL DEFAULT 'cascade',
            source_lang TEXT NOT NULL DEFAULT '',
            target_lang TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            speak INTEGER NOT NULL DEFAULT 0,
            stt_config_id TEXT,
            llm_config_id TEXT,
            tts_config_id TEXT,
            next_summary_seq BIGINT NOT NULL DEFAULT 0,
            summary_result_seq BIGINT NOT NULL DEFAULT 0,
            speak_text TEXT,
            llm_model TEXT,
            state TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS tenant_language_policy (
            tenant_id TEXT PRIMARY KEY,
            default_target_lang TEXT NOT NULL,
            allowed_langs TEXT NOT NULL,
            updated_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS voice_session_summary_input (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            session_id TEXT NOT NULL,
            seq BIGINT NOT NULL,
            origin TEXT NOT NULL,
            source_text TEXT,
            translated_text TEXT,
            assistant_text TEXT,
            client_input_id TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (session_id, seq),
            UNIQUE (session_id, client_input_id),
            CHECK (length(btrim(client_input_id)) > 0),
            CHECK (origin IN ('stt','text','translate','cascade')),
            CHECK (source_text IS NOT NULL OR translated_text IS NOT NULL OR assistant_text IS NOT NULL)
        );
        CREATE TABLE IF NOT EXISTS voice_session_summary_result (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            session_id TEXT NOT NULL,
            result_seq BIGINT NOT NULL,
            request_id TEXT NOT NULL,
            status TEXT NOT NULL,
            summary_text TEXT,
            error_code TEXT,
            input_seq_from BIGINT NOT NULL,
            input_seq_to BIGINT NOT NULL,
            truncated_inputs INTEGER NOT NULL DEFAULT 0,
            target_lang TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            title TEXT NOT NULL DEFAULT '',
            summary_style TEXT NOT NULL DEFAULT 'speech',
            UNIQUE (session_id, result_seq),
            UNIQUE (session_id, request_id),
            CHECK (length(btrim(request_id)) > 0),
            CHECK (status IN ('pending', 'ready', 'failed')),
            CHECK (summary_style IN ('speech', 'meeting', 'concise'))
        );
        CREATE TABLE IF NOT EXISTS usage_event_ledger (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            event_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            room_name TEXT,
            tenant_id TEXT NOT NULL,
            api_key_id TEXT NOT NULL,
            source TEXT NOT NULL,
            category TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_tokens BIGINT,
            completion_tokens BIGINT,
            cached_tokens BIGINT,
            audio_input_tokens BIGINT,
            audio_output_tokens BIGINT,
            audio_duration_ms BIGINT,
            tts_characters BIGINT,
            rtc_duration_ms BIGINT,
            rtc_bytes BIGINT,
            cost_usd_micros BIGINT NOT NULL DEFAULT 0,
            occurred_at_ms BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ai_model_config (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            config_id TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL DEFAULT 'tenant_ugreen_01',
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            protocol TEXT NOT NULL,
            base_url TEXT NOT NULL DEFAULT '',
            api_key TEXT NOT NULL DEFAULT '',
            api_secret TEXT NOT NULL DEFAULT '',
            extra_json TEXT NOT NULL DEFAULT '{}',
            model TEXT NOT NULL,
            voice TEXT NOT NULL DEFAULT '',
            is_default INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        CREATE TABLE IF NOT EXISTS usage_hourly_rollup (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            stat_hour TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            api_key_id TEXT NOT NULL,
            category TEXT NOT NULL,
            model TEXT NOT NULL,
            event_count BIGINT NOT NULL DEFAULT 0,
            sum_prompt_tokens BIGINT NOT NULL DEFAULT 0,
            sum_completion_tokens BIGINT NOT NULL DEFAULT 0,
            sum_cached_tokens BIGINT NOT NULL DEFAULT 0,
            sum_audio_input_tokens BIGINT NOT NULL DEFAULT 0,
            sum_audio_output_tokens BIGINT NOT NULL DEFAULT 0,
            sum_audio_duration_ms BIGINT NOT NULL DEFAULT 0,
            sum_tts_characters BIGINT NOT NULL DEFAULT 0,
            sum_rtc_duration_ms BIGINT NOT NULL DEFAULT 0,
            sum_rtc_bytes BIGINT NOT NULL DEFAULT 0,
            sum_cost_usd_micros BIGINT NOT NULL DEFAULT 0,
            UNIQUE(stat_hour, tenant_id, api_key_id, category, model)
        );
        "#;

        for stmt in ddl.split(';') {
            let trimmed = stmt.trim();
            if !trimmed.is_empty() {
                sqlx::query(trimmed).execute(&self.pool).await?;
            }
        }

        self.apply_voice_session_migrations().await?;
        self.seed_livekit_catalog().await?;

        sqlx::query(
            "INSERT INTO tenant_language_policy (tenant_id, default_target_lang, allowed_langs)
             VALUES ('tenant_ugreen_01', 'zh', '[\"zh\",\"de\",\"en\",\"es\",\"fr\",\"id\",\"it\",\"ja\",\"ko\"]')
             ON CONFLICT (tenant_id) DO NOTHING",
        )
        .execute(&self.pool)
        .await?;

        // Seed default Portal, Developer (Tenant), Consumer, Product, and Business API Key
        sqlx::query("INSERT INTO portal (portal_id, name, description, admin_id) VALUES ('portal_default', 'HiMarket AI Voice Portal', 'Enterprise AI Voice & LLM Gateway Portal', 'admin_01') ON CONFLICT DO NOTHING",
        )
        .execute(&self.pool)
        .await?;

        let admin_hash = sha256_hex("admin123");
        sqlx::query("INSERT INTO administrator (admin_id, username, password_hash) VALUES ('admin_01', 'admin', $1) ON CONFLICT DO NOTHING",
        )
        .bind(&admin_hash)
        .execute(&self.pool)
        .await?;

        let developer_hash = sha256_hex("voice123");
        sqlx::query("INSERT INTO developer (developer_id, username, email, portal_id, status, password_hash) VALUES ('tenant_ugreen_01', 'ugreen_voice_dev', 'voice@ugreen.com', 'portal_default', 'ACTIVE', $1) ON CONFLICT DO NOTHING",
        )
        .bind(&developer_hash)
        .execute(&self.pool)
        .await?;
        sqlx::query("UPDATE developer SET password_hash = $1 WHERE developer_id = 'tenant_ugreen_01' AND (password_hash IS NULL OR password_hash = '')")
            .bind(&developer_hash)
            .execute(&self.pool)
            .await?;

        sqlx::query("INSERT INTO consumer (consumer_id, name, description, portal_id, developer_id) VALUES ('cons_android_app', 'Android Voice Intercom App', 'SM_F9460 LiveKit Voice AI Client', 'portal_default', 'tenant_ugreen_01') ON CONFLICT DO NOTHING",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("INSERT INTO product (product_id, name, type, description, status) VALUES ('prod_voice_ai', 'LiveKit + DeepSeek Voice Agent', 'AGENT_API', 'Real-time WebRTC Voice Assistant with Faster-Whisper + DeepSeek-V4.1 + Piper/CosyVoice', 'PUBLISHED') ON CONFLICT DO NOTHING",
        )
        .execute(&self.pool)
        .await?;

        let default_raw_key = "hm_ak_live_ugreen_2026_voice_key";
        let key_hash = sha256_hex(default_raw_key);
        sqlx::query("INSERT INTO voice_api_key_binding (api_key_id, api_key_sha256, tenant_id, portal_id, consumer_id, status, allowed_modes, rate_limit_rpm, quota_limit_usd_micros, quota_used_usd_micros)
             VALUES ('ak_live_ugreen_01', $1, 'tenant_ugreen_01', 'portal_default', 'cons_android_app', 'ACTIVE', 'realtime,cascade', 120, 100000000, 0) ON CONFLICT DO NOTHING",
        )
        .bind(&key_hash)
        .execute(&self.pool)
        .await?;

        let config_count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM ai_model_config")
            .fetch_one(&self.pool)
            .await?
            .get("n");
        if config_count == 0 {
            self.insert_model_config("tenant_ugreen_01", "cfg_llm_default", "Ollama DeepSeek", "llm", "openai_chat", "https://ollama.com/v1", "cb4631cad50c4e3eb1342b9ef0075dd5.Ym7un785NAKQCVzDMK8DjK52", "", "deepseek-v4.1-flash:1017-cloud", "", true).await?;
            self.insert_model_config("tenant_ugreen_01", "cfg_stt_default", "本地 Faster-Whisper", "stt", "whisper_local", "", "", "", "faster-whisper-tiny", "/models/faster-whisper-tiny", true).await?;
            self.insert_model_config("tenant_ugreen_01", "cfg_tts_default", "本地 Piper 中文", "tts", "piper_local", "", "", "", "zh_CN-huayan-medium", "/models/piper/zh_CN-huayan-medium.onnx", true).await?;
        }

        Ok(())
    }

    pub async fn apply_voice_session_migrations(&self) -> Result<()> {
        let alters = r#"
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS task TEXT NOT NULL DEFAULT 'cascade';
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS source_lang TEXT NOT NULL DEFAULT '';
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS target_lang TEXT NOT NULL DEFAULT '';
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT '';
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS speak INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS stt_config_id TEXT;
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS llm_config_id TEXT;
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS tts_config_id TEXT;
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS next_summary_seq BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS summary_result_seq BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE voice_session_record ADD COLUMN IF NOT EXISTS speak_text TEXT;
        ALTER TABLE voice_session_record ALTER COLUMN room_name DROP NOT NULL;
        ALTER TABLE voice_session_record DROP CONSTRAINT IF EXISTS voice_session_task_chk;
        ALTER TABLE voice_session_record ADD CONSTRAINT voice_session_task_chk
            CHECK (task IN ('cascade','transcribe','speech_translate','text_translate','speak','context'));
        ALTER TABLE voice_session_record DROP CONSTRAINT IF EXISTS voice_session_speak_chk;
        ALTER TABLE voice_session_record ADD CONSTRAINT voice_session_speak_chk CHECK (speak IN (0, 1));
        ALTER TABLE ai_model_config ADD COLUMN IF NOT EXISTS vendor TEXT NOT NULL DEFAULT '';
        CREATE TABLE IF NOT EXISTS ai_route_policy (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            capability TEXT NOT NULL,
            language TEXT NOT NULL DEFAULT '*',
            config_id TEXT NOT NULL,
            updated_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text,
            UNIQUE(tenant_id, capability, language)
        );
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
            level TEXT NOT NULL,
            metric_type TEXT NOT NULL,
            used_value BIGINT NOT NULL,
            limit_value BIGINT NOT NULL,
            usage_pct INTEGER NOT NULL,
            notified_channel TEXT NOT NULL DEFAULT 'console',
            message TEXT NOT NULL,
            created_at TEXT DEFAULT (CURRENT_TIMESTAMP)::text
        );
        "#;
        for stmt in alters.split(';') {
            let trimmed = stmt.trim();
            if !trimmed.is_empty() {
                sqlx::query(trimmed).execute(&self.pool).await?;
            }
        }
        Ok(())
    }

    pub async fn get_language_policy(
        &self,
        tenant_id: &str,
    ) -> Result<Option<TenantLanguagePolicy>> {
        let row = sqlx::query(
            "SELECT tenant_id, default_target_lang, allowed_langs
             FROM tenant_language_policy WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else {
            return Ok(None);
        };
        let default_target_lang: String = r.get("default_target_lang");
        let allowed_raw: String = r.get("allowed_langs");
        let allowed_langs: Vec<String> = serde_json::from_str(&allowed_raw).unwrap_or_default();
        Ok(Some(TenantLanguagePolicy {
            tenant_id: r.get("tenant_id"),
            default_target_lang,
            allowed_langs,
        }))
    }

    pub async fn upsert_language_policy(
        &self,
        tenant_id: &str,
        default_target_lang: &str,
        allowed_langs: &[String],
    ) -> Result<TenantLanguagePolicy> {
        let validated = validate_language_policy(tenant_id, default_target_lang, allowed_langs)
            .map_err(|e| anyhow::anyhow!("target_lang_not_allowed: {}", e))?;
        let allowed_json = serde_json::to_string(&validated.allowed_langs)?;
        sqlx::query(
            "INSERT INTO tenant_language_policy (tenant_id, default_target_lang, allowed_langs, updated_at)
             VALUES ($1, $2, $3, (CURRENT_TIMESTAMP)::text)
             ON CONFLICT (tenant_id) DO UPDATE SET
                default_target_lang = excluded.default_target_lang,
                allowed_langs = excluded.allowed_langs,
                updated_at = (CURRENT_TIMESTAMP)::text",
        )
        .bind(&validated.tenant_id)
        .bind(&validated.default_target_lang)
        .bind(&allowed_json)
        .execute(&self.pool)
        .await?;
        Ok(validated)
    }

    pub async fn verify_business_key(
        &self,
        raw_api_key: &str,
    ) -> Result<Option<VerifiedBusinessKey>> {
        let trimmed = raw_api_key.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let key_hash = sha256_hex(trimmed);
        {
            let cache = self.key_cache.read().await;
            if let Some(found) = cache.get(&key_hash) {
                return Ok(Some(found.clone()));
            }
        }

        let row = sqlx::query("SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, rate_limit_rpm, quota_limit_usd_micros, quota_used_usd_micros, quota_limit_calls, quota_used_calls, quota_limit_tokens, quota_used_tokens
             FROM voice_api_key_binding WHERE api_key_sha256 = $1 OR api_key_id = $2",
        )
        .bind(&key_hash)
        .bind(trimmed)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(r) = row {
            let item = VerifiedBusinessKey {
                api_key_id: r.get("api_key_id"),
                tenant_id: r.get("tenant_id"),
                portal_id: r.get("portal_id"),
                consumer_id: r.get("consumer_id"),
                status: r.get("status"),
                allowed_modes: r.get("allowed_modes"),
                rate_limit_rpm: r.get("rate_limit_rpm"),
                quota_limit_usd_micros: r.get("quota_limit_usd_micros"),
                quota_used_usd_micros: r.get("quota_used_usd_micros"),
                quota_limit_calls: r.get("quota_limit_calls"),
                quota_used_calls: r.get("quota_used_calls"),
                quota_limit_tokens: r.get("quota_limit_tokens"),
                quota_used_tokens: r.get("quota_used_tokens"),
            };
            self.key_cache.write().await.insert(key_hash, item.clone());
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }

    pub async fn register_voice_session(
        &self,
        session_id: &str,
        room_name: &str,
        key: &VerifiedBusinessKey,
        req: &CreateVoiceSessionRequest,
    ) -> Result<SessionIdentity> {
        let room_opt = if room_name.trim().is_empty() {
            None
        } else {
            Some(room_name)
        };
        let llm_model = req
            .llm_model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        self.insert_session_record(
            session_id,
            room_opt,
            key,
            &req.mode,
            if req.task.trim().is_empty() {
                "cascade"
            } else {
                req.task.trim()
            },
            &req.source_lang,
            &req.target_lang,
            req.speak,
            None,
            None,
            None,
            llm_model.as_deref(),
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_session_record(
        &self,
        session_id: &str,
        room_name: Option<&str>,
        key: &VerifiedBusinessKey,
        voice_mode: &str,
        task: &str,
        source_lang: &str,
        target_lang: &str,
        speak: bool,
        stt_config_id: Option<&str>,
        llm_config_id: Option<&str>,
        tts_config_id: Option<&str>,
        llm_model: Option<&str>,
        speak_text: Option<&str>,
    ) -> Result<SessionIdentity> {
        let clean_room = room_name.map(str::trim).filter(|s| !s.is_empty());
        let clean_llm = llm_model.map(str::trim).filter(|s| !s.is_empty());
        sqlx::query(
            "INSERT INTO voice_session_record (
                session_id, room_name, tenant_id, portal_id, consumer_id, api_key_id,
                voice_mode, task, source_lang, target_lang, speak,
                stt_config_id, llm_config_id, tts_config_id, llm_model, speak_text, state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 'ACTIVE')",
        )
        .bind(session_id)
        .bind(clean_room)
        .bind(&key.tenant_id)
        .bind(&key.portal_id)
        .bind(&key.consumer_id)
        .bind(&key.api_key_id)
        .bind(voice_mode)
        .bind(task)
        .bind(source_lang)
        .bind(target_lang)
        .bind(if speak { 1 } else { 0 })
        .bind(stt_config_id)
        .bind(llm_config_id)
        .bind(tts_config_id)
        .bind(clean_llm)
        .bind(speak_text)
        .execute(&self.pool)
        .await?;

        let ident = SessionIdentity {
            session_id: session_id.to_string(),
            room_name: clean_room.map(str::to_string),
            tenant_id: key.tenant_id.clone(),
            portal_id: key.portal_id.clone(),
            consumer_id: key.consumer_id.clone(),
            api_key_id: key.api_key_id.clone(),
            voice_mode: voice_mode.to_string(),
            task: task.to_string(),
            llm_model: clean_llm.map(str::to_string),
        };
        let mut cache = self.session_cache.write().await;
        cache.insert(session_id.to_string(), ident.clone());
        if let Some(rn) = clean_room {
            cache.insert(rn.to_string(), ident.clone());
        }
        Ok(ident)
    }

    pub async fn get_session_record(&self, session_id: &str) -> Result<Option<SessionRecordFull>> {
        let trimmed = session_id.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let row = sqlx::query(
            "SELECT session_id, room_name, tenant_id, portal_id, consumer_id, api_key_id,
                    voice_mode, task, source_lang, target_lang, title, speak,
                    stt_config_id, llm_config_id, tts_config_id, llm_model, speak_text, state
             FROM voice_session_record WHERE session_id = $1",
        )
        .bind(trimmed)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| SessionRecordFull {
            session_id: r.get("session_id"),
            room_name: r.get("room_name"),
            tenant_id: r.get("tenant_id"),
            portal_id: r.get("portal_id"),
            consumer_id: r.get("consumer_id"),
            api_key_id: r.get("api_key_id"),
            voice_mode: r.get("voice_mode"),
            task: r.get("task"),
            source_lang: r.get("source_lang"),
            target_lang: r.get("target_lang"),
            title: r.get("title"),
            speak: r.get::<i32, _>("speak") == 1,
            stt_config_id: r.get("stt_config_id"),
            llm_config_id: r.get("llm_config_id"),
            tts_config_id: r.get("tts_config_id"),
            llm_model: r.get("llm_model"),
            speak_text: r.get("speak_text"),
            state: r.get("state"),
        }))
    }

    pub async fn set_session_llm_config(
        &self,
        session_id: &str,
        llm_config_id: &str,
        llm_model: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE voice_session_record SET llm_config_id = $1, llm_model = $2 WHERE session_id = $3",
        )
        .bind(llm_config_id)
        .bind(llm_model)
        .bind(session_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_session_tts_config(
        &self,
        session_id: &str,
        tts_config_id: Option<&str>,
    ) -> Result<()> {
        sqlx::query("UPDATE voice_session_record SET tts_config_id = $1 WHERE session_id = $2")
            .bind(tts_config_id)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_session_languages_and_bindings(
        &self,
        session_id: &str,
        source_lang: Option<&str>,
        target_lang: Option<&str>,
        stt_config_id: Option<&str>,
        tts_config_id: Option<Option<&str>>,
    ) -> Result<()> {
        if let Some(src) = source_lang {
            sqlx::query("UPDATE voice_session_record SET source_lang = $1 WHERE session_id = $2")
                .bind(src)
                .bind(session_id)
                .execute(&self.pool)
                .await?;
        }
        if let Some(tgt) = target_lang {
            sqlx::query("UPDATE voice_session_record SET target_lang = $1 WHERE session_id = $2")
                .bind(tgt)
                .bind(session_id)
                .execute(&self.pool)
                .await?;
        }
        if let Some(stt_id) = stt_config_id {
            sqlx::query("UPDATE voice_session_record SET stt_config_id = $1 WHERE session_id = $2")
                .bind(stt_id)
                .bind(session_id)
                .execute(&self.pool)
                .await?;
        }
        if let Some(tts_opt) = tts_config_id {
            sqlx::query("UPDATE voice_session_record SET tts_config_id = $1 WHERE session_id = $2")
                .bind(tts_opt)
                .bind(session_id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn resolve_session_identity(
        &self,
        session_or_room: &str,
    ) -> Result<Option<SessionIdentity>> {
        let lookup = session_or_room.trim();
        if lookup.is_empty() {
            return Ok(None);
        }
        {
            let cache = self.session_cache.read().await;
            if let Some(found) = cache.get(lookup) {
                return Ok(Some(found.clone()));
            }
        }
        let row = sqlx::query(
            "SELECT session_id, room_name, tenant_id, portal_id, consumer_id, api_key_id, voice_mode, task, llm_model
             FROM voice_session_record WHERE session_id = $1 OR room_name = $2 LIMIT 1",
        )
        .bind(lookup)
        .bind(lookup)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(r) = row {
            let ident = SessionIdentity {
                session_id: r.get("session_id"),
                room_name: r.get::<Option<String>, _>("room_name"),
                tenant_id: r.get("tenant_id"),
                portal_id: r.get("portal_id"),
                consumer_id: r.get("consumer_id"),
                api_key_id: r.get("api_key_id"),
                voice_mode: r.get("voice_mode"),
                task: r.get("task"),
                llm_model: r.get::<Option<String>, _>("llm_model"),
            };
            let mut cache = self.session_cache.write().await;
            cache.insert(ident.session_id.clone(), ident.clone());
            if let Some(rn) = ident
                .room_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                cache.insert(rn.to_string(), ident.clone());
            }
            Ok(Some(ident))
        } else {
            Ok(None)
        }
    }

    pub async fn append_summary_input(
        &self,
        session_id: &str,
        origin: &str,
        source_text: Option<&str>,
        translated_text: Option<&str>,
        assistant_text: Option<&str>,
        client_input_id: &str,
    ) -> Result<AppendSummaryInputResult> {
        let cid = client_input_id.trim();
        if cid.is_empty() {
            anyhow::bail!("empty_input: client_input_id cannot be empty");
        }
        if cid.chars().count() > 64 {
            anyhow::bail!("summary_limit: client_input_id exceeds 64 characters");
        }
        let clean_src = source_text.map(str::trim).filter(|s| !s.is_empty());
        let clean_tr = translated_text.map(str::trim).filter(|s| !s.is_empty());
        let clean_ast = assistant_text.map(str::trim).filter(|s| !s.is_empty());
        if clean_src.is_none() && clean_tr.is_none() && clean_ast.is_none() {
            anyhow::bail!("empty_input: input text cannot be empty");
        }
        for txt in [clean_src, clean_tr, clean_ast].into_iter().flatten() {
            if txt.chars().count() > 8000 {
                anyhow::bail!("summary_limit: single input exceeds 8000 characters");
            }
        }

        let mut tx = self.pool.begin().await?;

        let sess_exists = sqlx::query(
            "SELECT next_summary_seq FROM voice_session_record WHERE session_id = $1 FOR UPDATE",
        )
        .bind(session_id)
        .fetch_optional(&mut *tx)
        .await?;
        if sess_exists.is_none() {
            anyhow::bail!("session not found");
        }

        let existing = sqlx::query(
            "SELECT seq, source_text, translated_text, assistant_text
             FROM voice_session_summary_input
             WHERE session_id = $1 AND client_input_id = $2",
        )
        .bind(session_id)
        .bind(cid)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = existing {
            let seq: i64 = row.get("seq");
            let mut cur_src: Option<String> = row.get("source_text");
            let mut cur_tr: Option<String> = row.get("translated_text");
            let mut cur_ast: Option<String> = row.get("assistant_text");

            let mut changed = false;
            if let Some(new_src) = clean_src {
                if cur_src.as_deref() != Some(new_src) {
                    cur_src = Some(new_src.to_string());
                    changed = true;
                }
            }
            if let Some(new_tr) = clean_tr {
                if cur_tr.as_deref() != Some(new_tr) {
                    cur_tr = Some(new_tr.to_string());
                    changed = true;
                }
            }
            if let Some(new_ast) = clean_ast {
                if cur_ast.as_deref() != Some(new_ast) {
                    cur_ast = Some(new_ast.to_string());
                    changed = true;
                }
            }
            if changed {
                sqlx::query(
                    "UPDATE voice_session_summary_input
                     SET source_text = $1, translated_text = $2, assistant_text = $3
                     WHERE session_id = $4 AND client_input_id = $5",
                )
                .bind(&cur_src)
                .bind(&cur_tr)
                .bind(&cur_ast)
                .bind(session_id)
                .bind(cid)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            return Ok(AppendSummaryInputResult {
                seq,
                replayed: true,
                source_text: cur_src,
                translated_text: cur_tr,
                assistant_text: cur_ast,
            });
        }

        let count: i64 = sqlx::query(
            "SELECT COUNT(*) AS n FROM voice_session_summary_input WHERE session_id = $1",
        )
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await?
        .get("n");
        if count >= 100 {
            anyhow::bail!("summary_limit: session exceeds maximum 100 summary inputs");
        }

        let seq: i64 = sqlx::query(
            "UPDATE voice_session_record
             SET next_summary_seq = next_summary_seq + 1
             WHERE session_id = $1
             RETURNING next_summary_seq",
        )
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await?
        .get("next_summary_seq");

        sqlx::query(
            "INSERT INTO voice_session_summary_input (
                session_id, seq, origin, source_text, translated_text, assistant_text, client_input_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(session_id)
        .bind(seq)
        .bind(origin)
        .bind(clean_src)
        .bind(clean_tr)
        .bind(clean_ast)
        .bind(cid)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(AppendSummaryInputResult {
            seq,
            replayed: false,
            source_text: clean_src.map(str::to_string),
            translated_text: clean_tr.map(str::to_string),
            assistant_text: clean_ast.map(str::to_string),
        })
    }

    pub async fn list_summary_inputs(&self, session_id: &str) -> Result<Vec<SummaryInputRow>> {
        let rows = sqlx::query(
            "SELECT seq, origin, source_text, translated_text, assistant_text, client_input_id
             FROM voice_session_summary_input
             WHERE session_id = $1
             ORDER BY seq ASC",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| SummaryInputRow {
                seq: r.get("seq"),
                origin: r.get("origin"),
                source_text: r.get("source_text"),
                translated_text: r.get("translated_text"),
                assistant_text: r.get("assistant_text"),
                client_input_id: r.get("client_input_id"),
            })
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn begin_summary(
        &self,
        session_id: &str,
        request_id: &str,
        input_seq_from: i64,
        input_seq_to: i64,
        truncated_inputs: i32,
        target_lang: &str,
        summary_style: &str,
    ) -> Result<BeginSummaryOutcome> {
        let req_id = request_id.trim();
        if req_id.is_empty() {
            anyhow::bail!("empty_input: request_id cannot be empty");
        }
        let style = if summary_style.trim().is_empty() {
            "speech"
        } else {
            summary_style.trim()
        };
        if !matches!(style, "speech" | "meeting" | "concise") {
            anyhow::bail!("empty_input: invalid summary_style {}", style);
        }

        let lock_key = format!("{}:summarize:{}", session_id, req_id);
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&lock_key)
            .execute(&mut *tx)
            .await?;

        let existing = sqlx::query(
            "SELECT session_id, result_seq, request_id, status, title, summary_style,
                    summary_text, error_code, input_seq_from, input_seq_to,
                    truncated_inputs, target_lang,
                    (updated_at > now() - interval '60 seconds') AS is_fresh
             FROM voice_session_summary_result
             WHERE session_id = $1 AND request_id = $2",
        )
        .bind(session_id)
        .bind(req_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(r) = existing {
            let status: String = r.get("status");
            let is_fresh: bool = r.get("is_fresh");
            let row = SummaryResultRow {
                session_id: r.get("session_id"),
                result_seq: r.get("result_seq"),
                request_id: r.get("request_id"),
                status: status.clone(),
                title: r.get("title"),
                summary_style: r.get("summary_style"),
                summary_text: r.get("summary_text"),
                error_code: r.get("error_code"),
                input_seq_from: r.get("input_seq_from"),
                input_seq_to: r.get("input_seq_to"),
                truncated_inputs: r.get("truncated_inputs"),
                target_lang: r.get("target_lang"),
            };
            match status.as_str() {
                "ready" => {
                    tx.commit().await?;
                    return Ok(BeginSummaryOutcome::Ready(row));
                }
                "failed" => {
                    tx.commit().await?;
                    return Ok(BeginSummaryOutcome::Failed(row));
                }
                "pending" if is_fresh => {
                    tx.commit().await?;
                    return Ok(BeginSummaryOutcome::Busy);
                }
                _ => {
                    sqlx::query(
                        "UPDATE voice_session_summary_result
                         SET updated_at = now(),
                             input_seq_from = $1,
                             input_seq_to = $2,
                             truncated_inputs = $3,
                             target_lang = $4,
                             summary_style = $5
                         WHERE session_id = $6 AND request_id = $7",
                    )
                    .bind(input_seq_from)
                    .bind(input_seq_to)
                    .bind(truncated_inputs)
                    .bind(target_lang)
                    .bind(style)
                    .bind(session_id)
                    .bind(req_id)
                    .execute(&mut *tx)
                    .await?;
                    tx.commit().await?;
                    return Ok(BeginSummaryOutcome::Owned {
                        result_seq: row.result_seq,
                    });
                }
            }
        }

        let result_seq: i64 = sqlx::query(
            "UPDATE voice_session_record
             SET summary_result_seq = summary_result_seq + 1
             WHERE session_id = $1
             RETURNING summary_result_seq",
        )
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await?
        .get("summary_result_seq");

        sqlx::query(
            "INSERT INTO voice_session_summary_result (
                session_id, result_seq, request_id, status,
                input_seq_from, input_seq_to, truncated_inputs, target_lang, summary_style
             ) VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $8)",
        )
        .bind(session_id)
        .bind(result_seq)
        .bind(req_id)
        .bind(input_seq_from)
        .bind(input_seq_to)
        .bind(truncated_inputs)
        .bind(target_lang)
        .bind(style)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(BeginSummaryOutcome::Owned { result_seq })
    }

    pub async fn finish_summary(
        &self,
        session_id: &str,
        request_id: &str,
        title: &str,
        summary_text: &str,
        replace_session_title: bool,
    ) -> Result<SummaryResultRow> {
        let req_id = request_id.trim();
        sqlx::query(
            "UPDATE voice_session_summary_result
             SET status = 'ready',
                 title = $1,
                 summary_text = $2,
                 error_code = NULL,
                 updated_at = now()
             WHERE session_id = $3 AND request_id = $4 AND status = 'pending'",
        )
        .bind(title)
        .bind(summary_text)
        .bind(session_id)
        .bind(req_id)
        .execute(&self.pool)
        .await?;

        if !title.trim().is_empty() {
            if replace_session_title {
                sqlx::query(
                    "UPDATE voice_session_record SET title = $1 WHERE session_id = $2",
                )
                .bind(title.trim())
                .bind(session_id)
                .execute(&self.pool)
                .await?;
            } else {
                sqlx::query(
                    "UPDATE voice_session_record SET title = $1
                     WHERE session_id = $2 AND (title IS NULL OR btrim(title) = '')",
                )
                .bind(title.trim())
                .bind(session_id)
                .execute(&self.pool)
                .await?;
            }
        }

        self.get_summary_result(session_id, req_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("summary result not found"))
    }

    pub async fn fail_summary(
        &self,
        session_id: &str,
        request_id: &str,
        error_code: &str,
    ) -> Result<SummaryResultRow> {
        let req_id = request_id.trim();
        sqlx::query(
            "UPDATE voice_session_summary_result
             SET status = 'failed',
                 error_code = $1,
                 summary_text = NULL,
                 updated_at = now()
             WHERE session_id = $2 AND request_id = $3 AND status = 'pending'",
        )
        .bind(error_code)
        .bind(session_id)
        .bind(req_id)
        .execute(&self.pool)
        .await?;

        self.get_summary_result(session_id, req_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("summary result not found"))
    }

    pub async fn get_summary_result(
        &self,
        session_id: &str,
        request_id: &str,
    ) -> Result<Option<SummaryResultRow>> {
        let row = sqlx::query(
            "SELECT session_id, result_seq, request_id, status, title, summary_style,
                    summary_text, error_code, input_seq_from, input_seq_to,
                    truncated_inputs, target_lang
             FROM voice_session_summary_result
             WHERE session_id = $1 AND request_id = $2",
        )
        .bind(session_id)
        .bind(request_id.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| SummaryResultRow {
            session_id: r.get("session_id"),
            result_seq: r.get("result_seq"),
            request_id: r.get("request_id"),
            status: r.get("status"),
            title: r.get("title"),
            summary_style: r.get("summary_style"),
            summary_text: r.get("summary_text"),
            error_code: r.get("error_code"),
            input_seq_from: r.get("input_seq_from"),
            input_seq_to: r.get("input_seq_to"),
            truncated_inputs: r.get("truncated_inputs"),
            target_lang: r.get("target_lang"),
        }))
    }

    pub async fn select_tts_for_lang(
        &self,
        tenant_id: &str,
        target_lang: &str,
    ) -> std::result::Result<SelectedTts, TtsSelectError> {
        let norm_target = normalize_lang(target_lang).map_err(|_| TtsSelectError::Unavailable)?;
        let target_primary =
            primary_lang_subtag(&norm_target).ok_or(TtsSelectError::Unavailable)?;

        // 1. Check explicit language route in ai_route_policy ('tts', norm_target or target_primary)
        if let Ok(Some(route_cfg)) = self
            .get_route_target_config(tenant_id, "tts", &[norm_target.as_str(), target_primary.as_str()])
            .await
        {
            if is_tts_runnable(&route_cfg) {
                return Ok(SelectedTts {
                    config: route_cfg,
                    voice_fallback: false,
                });
            }
        }

        // 2. If wildcard route ('tts', '*') was switched to a runnable non-local cloud vendor TTS, honor it directly!
        if let Ok(Some(star_cfg)) = self
            .get_route_target_config(tenant_id, "tts", &["*"])
            .await
        {
            if star_cfg.protocol != "piper_local" && is_tts_runnable(&star_cfg) {
                return Ok(SelectedTts {
                    config: star_cfg,
                    voice_fallback: false,
                });
            }
        }

        let rows = sqlx::query(
            "SELECT config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret,
                    extra_json, model, voice, is_default, status
             FROM ai_model_config
             WHERE tenant_id = $1 AND status = 'ACTIVE' AND kind = 'tts'",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| TtsSelectError::Unavailable)?;

        let configs: Vec<ModelConfig> = rows.into_iter().map(model_config_from_row).collect();
        let mut matched: Vec<(u8, ModelConfig)> = Vec::new();

        for cfg in &configs {
            let langs = tts_candidate_langs(cfg);
            if langs.is_empty() {
                continue;
            }
            if langs.iter().any(|l| l == &norm_target) {
                matched.push((2, cfg.clone()));
            } else if langs
                .iter()
                .any(|l| primary_lang_subtag(l).as_deref() == Some(target_primary.as_str()))
            {
                matched.push((1, cfg.clone()));
            }
        }

        matched.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.is_default.cmp(&a.1.is_default))
                .then_with(|| a.1.config_id.cmp(&b.1.config_id))
        });

        if let Some((_, best)) = matched.into_iter().next() {
            return Ok(SelectedTts {
                config: best,
                voice_fallback: false,
            });
        }

        let mut piper_fallbacks: Vec<ModelConfig> = configs
            .into_iter()
            .filter(|c| c.protocol == "piper_local" && is_tts_runnable(c))
            .collect();
        piper_fallbacks.sort_by(|a, b| {
            b.is_default
                .cmp(&a.is_default)
                .then_with(|| a.config_id.cmp(&b.config_id))
        });

        if let Some(fallback) = piper_fallbacks.into_iter().next() {
            Ok(SelectedTts {
                config: fallback,
                voice_fallback: true,
            })
        } else {
            Err(TtsSelectError::Unavailable)
        }
    }

    pub async fn select_stt_for_lang(
        &self,
        tenant_id: &str,
        source_lang: Option<&str>,
        requested_model: Option<&str>,
    ) -> std::result::Result<SelectedStt, SttSelectError> {
        let rows = sqlx::query(
            "SELECT id, config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret,
                    extra_json, model, voice, is_default, status
             FROM ai_model_config
             WHERE tenant_id = $1 AND status = 'ACTIVE' AND kind = 'stt'
             ORDER BY is_default DESC, id ASC",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| SttSelectError::Unavailable)?;

        let all_configs: Vec<(i64, ModelConfig)> = rows
            .into_iter()
            .map(|r| {
                let id: i64 = r.get("id");
                (id, model_config_from_row(r))
            })
            .collect();

        let mut explicit_failed = false;
        if let Some(req) = requested_model
            .map(str::trim)
            .filter(|s| !s.is_empty() && *s != "auto")
        {
            if let Some((_, matched)) = all_configs
                .iter()
                .find(|(_, c)| c.config_id == req || c.model == req || c.name == req)
            {
                if is_stt_runnable(matched) {
                    return Ok(SelectedStt {
                        config: matched.clone(),
                        stt_fallback: false,
                    });
                } else {
                    explicit_failed = true;
                }
            } else {
                explicit_failed = true;
            }
        }

        if !explicit_failed {
            if let Some(src_raw) = source_lang.map(str::trim).filter(|s| !s.is_empty()) {
                if let Ok(norm_src) = normalize_lang(src_raw) {
                    let src_primary = primary_lang_subtag(&norm_src).unwrap_or_default();
                    if let Ok(Some(route_cfg)) = self
                        .get_route_target_config(
                            tenant_id,
                            "asr",
                            &[norm_src.as_str(), src_primary.as_str()],
                        )
                        .await
                    {
                        if is_stt_runnable(&route_cfg) {
                            return Ok(SelectedStt {
                                config: route_cfg,
                                stt_fallback: false,
                            });
                        }
                    }
                }
            }
            if let Ok(Some(star_cfg)) = self.get_route_target_config(tenant_id, "asr", &["*"]).await
            {
                if star_cfg.protocol != "whisper_local" && is_stt_runnable(&star_cfg) {
                    return Ok(SelectedStt {
                        config: star_cfg,
                        stt_fallback: false,
                    });
                }
            }
        }

        let runnable: Vec<(i64, ModelConfig)> = all_configs
            .into_iter()
            .filter(|(_, c)| is_stt_runnable(c))
            .collect();
        if runnable.is_empty() {
            return Err(SttSelectError::Unavailable);
        }

        if let Some(src_raw) = source_lang.map(str::trim).filter(|s| !s.is_empty()) {
            if let Ok(norm_src) = normalize_lang(src_raw) {
                let src_primary = primary_lang_subtag(&norm_src).unwrap_or_default();
                let mut lang_matched: Vec<(u8, i64, ModelConfig)> = Vec::new();
                for (id, cfg) in &runnable {
                    let stt_langs = parse_extra_lang_list(&cfg.extra_json, "stt_languages");
                    if stt_langs.iter().any(|l| l == &norm_src) {
                        lang_matched.push((2, *id, cfg.clone()));
                    } else if stt_langs
                        .iter()
                        .any(|l| primary_lang_subtag(l).as_deref() == Some(src_primary.as_str()))
                    {
                        lang_matched.push((1, *id, cfg.clone()));
                    }
                }
                lang_matched.sort_by(|a, b| {
                    b.0.cmp(&a.0)
                        .then_with(|| b.2.is_default.cmp(&a.2.is_default))
                        .then_with(|| a.1.cmp(&b.1))
                });
                if let Some((_, _, best)) = lang_matched.into_iter().next() {
                    return Ok(SelectedStt {
                        config: best,
                        stt_fallback: explicit_failed,
                    });
                }
            }
            let (_, default_cfg) = runnable.into_iter().next().unwrap();
            return Ok(SelectedStt {
                config: default_cfg,
                stt_fallback: true,
            });
        }

        let (_, default_cfg) = runnable.into_iter().next().unwrap();
        Ok(SelectedStt {
            config: default_cfg,
            stt_fallback: explicit_failed,
        })
    }

    pub async fn ingest_usage_event(&self, mut ev: UsageEventPayload) -> Result<bool> {
        if ev.tenant_id.is_empty() || ev.api_key_id.is_empty() {
            let lookup_key = if !ev.session_id.is_empty() {
                ev.session_id.as_str()
            } else {
                ev.room_name.as_deref().unwrap_or("")
            };
            if let Some(ident) = self.resolve_session_identity(lookup_key).await? {
                ev.session_id = ident.session_id;
                ev.room_name = ident.room_name;
                ev.tenant_id = ident.tenant_id;
                ev.api_key_id = ident.api_key_id;
            } else {
                if ev.tenant_id.is_empty() {
                    ev.tenant_id = "tenant_ugreen_01".to_string();
                }
                if ev.api_key_id.is_empty() {
                    ev.api_key_id = "ak_live_ugreen_01".to_string();
                }
            }
        }

        let cost_micros = ev.compute_cost_micros();
        let m = &ev.metrics;
        let dt = DateTime::<Utc>::from_timestamp_millis(ev.occurred_at_ms).unwrap_or_else(Utc::now);
        let stat_hour = dt.format("%Y-%m-%d %H:00:00").to_string();
        let stat_date = dt.format("%Y-%m-%d").to_string();

        let vendor = if !ev.vendor.trim().is_empty() {
            ev.vendor.trim().to_string()
        } else {
            let v_opt: Option<(String, String, String)> = sqlx::query(
                "SELECT vendor, protocol, base_url FROM ai_model_config WHERE tenant_id = $1 AND model = $2 LIMIT 1",
            )
            .bind(&ev.tenant_id)
            .bind(&ev.model)
            .fetch_optional(&self.pool)
            .await?
            .map(|r| (r.get("vendor"), r.get("protocol"), r.get("base_url")));
            if let Some((v, p, u)) = v_opt {
                if !v.trim().is_empty() {
                    v
                } else {
                    infer_vendor(&p, &ev.model, &u)
                }
            } else {
                infer_vendor("", &ev.model, "")
            }
        };

        let status = if ev.status.trim().is_empty() {
            "SUCCESS".to_string()
        } else {
            ev.status.trim().to_ascii_uppercase()
        };

        let res = sqlx::query(
            "INSERT INTO usage_event_ledger (
                event_id, idempotency_key, session_id, room_name, tenant_id, api_key_id,
                source, category, model, vendor, status, latency_ms, stat_date,
                prompt_tokens, completion_tokens, cached_tokens,
                audio_input_tokens, audio_output_tokens, audio_duration_ms, tts_characters,
                rtc_duration_ms, rtc_bytes, cost_usd_micros, occurred_at_ms
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16,
                $17, $18, $19, $20,
                $21, $22, $23, $24
            )
            ON CONFLICT (idempotency_key) DO NOTHING",
        )
        .bind(&ev.event_id)
        .bind(&ev.idempotency_key)
        .bind(&ev.session_id)
        .bind(&ev.room_name)
        .bind(&ev.tenant_id)
        .bind(&ev.api_key_id)
        .bind(&ev.source)
        .bind(&ev.category)
        .bind(&ev.model)
        .bind(&vendor)
        .bind(&status)
        .bind(m.latency_ms)
        .bind(&stat_date)
        .bind(m.prompt_tokens)
        .bind(m.completion_tokens)
        .bind(m.cached_tokens)
        .bind(m.audio_input_tokens)
        .bind(m.audio_output_tokens)
        .bind(m.audio_duration_ms)
        .bind(m.tts_characters)
        .bind(m.rtc_duration_ms)
        .bind(m.rtc_bytes)
        .bind(cost_micros)
        .bind(ev.occurred_at_ms)
        .execute(&self.pool)
        .await?;

        if res.rows_affected() == 0 {
            return Ok(false);
        }

        sqlx::query("INSERT INTO usage_hourly_rollup AS rollup (
                stat_hour, tenant_id, api_key_id, category, model, event_count,
                sum_prompt_tokens, sum_completion_tokens, sum_cached_tokens,
                sum_audio_input_tokens, sum_audio_output_tokens, sum_audio_duration_ms,
                sum_tts_characters, sum_rtc_duration_ms, sum_rtc_bytes, sum_cost_usd_micros
            ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT(stat_hour, tenant_id, api_key_id, category, model) DO UPDATE SET
                event_count = rollup.event_count + 1,
                sum_prompt_tokens = rollup.sum_prompt_tokens + excluded.sum_prompt_tokens,
                sum_completion_tokens = rollup.sum_completion_tokens + excluded.sum_completion_tokens,
                sum_cached_tokens = rollup.sum_cached_tokens + excluded.sum_cached_tokens,
                sum_audio_input_tokens = rollup.sum_audio_input_tokens + excluded.sum_audio_input_tokens,
                sum_audio_output_tokens = rollup.sum_audio_output_tokens + excluded.sum_audio_output_tokens,
                sum_audio_duration_ms = rollup.sum_audio_duration_ms + excluded.sum_audio_duration_ms,
                sum_tts_characters = rollup.sum_tts_characters + excluded.sum_tts_characters,
                sum_rtc_duration_ms = rollup.sum_rtc_duration_ms + excluded.sum_rtc_duration_ms,
                sum_rtc_bytes = rollup.sum_rtc_bytes + excluded.sum_rtc_bytes,
                sum_cost_usd_micros = rollup.sum_cost_usd_micros + excluded.sum_cost_usd_micros",
        )
        .bind(&stat_hour)
        .bind(&ev.tenant_id)
        .bind(&ev.api_key_id)
        .bind(&ev.category)
        .bind(&ev.model)
        .bind(m.prompt_tokens.unwrap_or(0))
        .bind(m.completion_tokens.unwrap_or(0))
        .bind(m.cached_tokens.unwrap_or(0))
        .bind(m.audio_input_tokens.unwrap_or(0))
        .bind(m.audio_output_tokens.unwrap_or(0))
        .bind(m.audio_duration_ms.unwrap_or(0))
        .bind(m.tts_characters.unwrap_or(0))
        .bind(m.rtc_duration_ms.unwrap_or(0))
        .bind(m.rtc_bytes.unwrap_or(0))
        .bind(cost_micros)
        .execute(&self.pool)
        .await?;

        let consumer_id: String = sqlx::query(
            "SELECT consumer_id FROM voice_api_key_binding WHERE api_key_id = $1 LIMIT 1",
        )
        .bind(&ev.api_key_id)
        .fetch_optional(&self.pool)
        .await?
        .map(|r| r.get("consumer_id"))
        .unwrap_or_else(|| "cons_android_app".to_string());

        let succ_inc = if status == "SUCCESS" { 1i64 } else { 0i64 };
        let fail_inc = if status == "SUCCESS" { 0i64 } else { 1i64 };

        sqlx::query(
            "INSERT INTO usage_daily_three_dim_rollup AS d (
                stat_date, tenant_id, api_key_id, consumer_id, category, vendor, model,
                call_count, success_count, failure_count,
                sum_prompt_tokens, sum_completion_tokens,
                sum_audio_duration_ms, sum_tts_characters, sum_latency_ms, sum_cost_usd_micros
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (stat_date, tenant_id, api_key_id, category, vendor, model) DO UPDATE SET
                call_count = d.call_count + 1,
                success_count = d.success_count + excluded.success_count,
                failure_count = d.failure_count + excluded.failure_count,
                sum_prompt_tokens = d.sum_prompt_tokens + excluded.sum_prompt_tokens,
                sum_completion_tokens = d.sum_completion_tokens + excluded.sum_completion_tokens,
                sum_audio_duration_ms = d.sum_audio_duration_ms + excluded.sum_audio_duration_ms,
                sum_tts_characters = d.sum_tts_characters + excluded.sum_tts_characters,
                sum_latency_ms = d.sum_latency_ms + excluded.sum_latency_ms,
                sum_cost_usd_micros = d.sum_cost_usd_micros + excluded.sum_cost_usd_micros",
        )
        .bind(&stat_date)
        .bind(&ev.tenant_id)
        .bind(&ev.api_key_id)
        .bind(&consumer_id)
        .bind(&ev.category)
        .bind(&vendor)
        .bind(&ev.model)
        .bind(succ_inc)
        .bind(fail_inc)
        .bind(m.prompt_tokens.unwrap_or(0))
        .bind(m.completion_tokens.unwrap_or(0))
        .bind(m.audio_duration_ms.unwrap_or(0))
        .bind(m.tts_characters.unwrap_or(0))
        .bind(m.latency_ms.unwrap_or(0))
        .bind(cost_micros)
        .execute(&self.pool)
        .await?;

        let delta_tokens = m.prompt_tokens.unwrap_or(0)
            + m.completion_tokens.unwrap_or(0)
            + m.audio_input_tokens.unwrap_or(0)
            + m.audio_output_tokens.unwrap_or(0);

        sqlx::query(
            "UPDATE voice_api_key_binding
             SET quota_used_usd_micros = quota_used_usd_micros + $1,
                 quota_used_calls = quota_used_calls + 1,
                 quota_used_tokens = quota_used_tokens + $2
             WHERE api_key_id = $3",
        )
        .bind(cost_micros)
        .bind(delta_tokens)
        .bind(&ev.api_key_id)
        .execute(&self.pool)
        .await?;

        self.evaluate_and_record_quota_alerts(&ev.api_key_id).await?;
        self.key_cache.write().await.clear();

        Ok(true)
    }

    pub async fn query_usage_summary(
        &self,
        tenant_id: &str,
        api_key_id: Option<&str>,
    ) -> Result<UsageSummaryResponse> {
        let rows = if let Some(ak) = api_key_id {
            sqlx::query("SELECT category,
                        SUM(event_count)::bigint AS cnt,
                        SUM(sum_prompt_tokens)::bigint AS p_tok,
                        SUM(sum_completion_tokens)::bigint AS c_tok,
                        SUM(sum_cached_tokens)::bigint AS cache_tok,
                        SUM(sum_audio_input_tokens)::bigint AS a_in,
                        SUM(sum_audio_output_tokens)::bigint AS a_out,
                        SUM(sum_audio_duration_ms)::bigint AS a_dur,
                        SUM(sum_tts_characters)::bigint AS tts_ch,
                        SUM(sum_rtc_duration_ms)::bigint AS rtc_dur,
                        SUM(sum_rtc_bytes)::bigint AS rtc_b,
                        SUM(sum_cost_usd_micros)::bigint AS cost
                 FROM usage_hourly_rollup
                 WHERE tenant_id = $1 AND api_key_id = $2
                 GROUP BY category",
            )
            .bind(tenant_id)
            .bind(ak)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query("SELECT category,
                        SUM(event_count)::bigint AS cnt,
                        SUM(sum_prompt_tokens)::bigint AS p_tok,
                        SUM(sum_completion_tokens)::bigint AS c_tok,
                        SUM(sum_cached_tokens)::bigint AS cache_tok,
                        SUM(sum_audio_input_tokens)::bigint AS a_in,
                        SUM(sum_audio_output_tokens)::bigint AS a_out,
                        SUM(sum_audio_duration_ms)::bigint AS a_dur,
                        SUM(sum_tts_characters)::bigint AS tts_ch,
                        SUM(sum_rtc_duration_ms)::bigint AS rtc_dur,
                        SUM(sum_rtc_bytes)::bigint AS rtc_b,
                        SUM(sum_cost_usd_micros)::bigint AS cost
                 FROM usage_hourly_rollup
                 WHERE tenant_id = $1
                 GROUP BY category",
            )
            .bind(tenant_id)
            .fetch_all(&self.pool)
            .await?
        };

        let mut rtc = RtcUsageBreakdown::default();
        let mut voice = VoiceModelUsageBreakdown::default();
        let mut text_llm = TextLlmUsageBreakdown::default();
        let mut translate = TextLlmUsageBreakdown::default();
        let mut summary = TextLlmUsageBreakdown::default();
        let mut total_events = 0i64;
        let mut total_cost = 0i64;

        for r in rows {
            let cat: String = r.get("category");
            let cnt: i64 = r.get("cnt");
            let cost: i64 = r.get("cost");
            total_events += cnt;
            total_cost += cost;

            match cat.as_str() {
                "rtc" => {
                    rtc.rtc_duration_ms += r.get::<i64, _>("rtc_dur");
                    rtc.rtc_bytes += r.get::<i64, _>("rtc_b");
                    rtc.cost_usd_micros += cost;
                }
                "stt" => {
                    voice.stt_audio_duration_ms += r.get::<i64, _>("a_dur");
                    voice.cost_usd_micros += cost;
                }
                "tts" => {
                    voice.tts_audio_duration_ms += r.get::<i64, _>("a_dur");
                    voice.tts_characters += r.get::<i64, _>("tts_ch");
                    voice.cost_usd_micros += cost;
                }
                "realtime_audio" => {
                    voice.audio_input_tokens += r.get::<i64, _>("a_in");
                    voice.audio_output_tokens += r.get::<i64, _>("a_out");
                    voice.cost_usd_micros += cost;
                }
                "text_llm" => {
                    text_llm.prompt_tokens += r.get::<i64, _>("p_tok");
                    text_llm.completion_tokens += r.get::<i64, _>("c_tok");
                    text_llm.cached_tokens += r.get::<i64, _>("cache_tok");
                    text_llm.cost_usd_micros += cost;
                }
                "translate" => {
                    translate.prompt_tokens += r.get::<i64, _>("p_tok");
                    translate.completion_tokens += r.get::<i64, _>("c_tok");
                    translate.cached_tokens += r.get::<i64, _>("cache_tok");
                    translate.cost_usd_micros += cost;
                }
                "summary" => {
                    summary.prompt_tokens += r.get::<i64, _>("p_tok");
                    summary.completion_tokens += r.get::<i64, _>("c_tok");
                    summary.cached_tokens += r.get::<i64, _>("cache_tok");
                    summary.cost_usd_micros += cost;
                }
                _ => {}
            }
        }

        Ok(UsageSummaryResponse {
            tenant_id: tenant_id.to_string(),
            api_key_id: api_key_id.map(|s| s.to_string()),
            total_events,
            total_cost_usd_micros: total_cost,
            total_cost_usd: format!("{:.6}", (total_cost as f64) / 1_000_000.0),
            rtc,
            voice_models: voice,
            text_llm,
            translate,
            summary,
        })
    }

    pub async fn list_portals(&self) -> Result<Vec<PortalDto>> {
        let rows = sqlx::query("SELECT portal_id, name, description FROM portal ORDER BY id")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| PortalDto {
                portal_id: r.get("portal_id"),
                name: r.get("name"),
                description: r.get("description"),
            })
            .collect())
    }

    pub async fn list_products(&self) -> Result<Vec<ProductDto>> {
        let rows =
            sqlx::query("SELECT product_id, name, type, description, status FROM product ORDER BY id")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|r| ProductDto {
                product_id: r.get("product_id"),
                name: r.get("name"),
                product_type: r.get("type"),
                description: r.get("description"),
                status: r.get("status"),
            })
            .collect())
    }

    pub async fn list_consumers(&self, tenant_id: Option<&str>) -> Result<Vec<ConsumerDto>> {
        let rows = if let Some(tid) = tenant_id {
            sqlx::query("SELECT consumer_id, name, portal_id, developer_id FROM consumer WHERE developer_id = $1 ORDER BY id")
                .bind(tid)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query("SELECT consumer_id, name, portal_id, developer_id FROM consumer ORDER BY id")
                .fetch_all(&self.pool)
                .await?
        };
        Ok(rows
            .into_iter()
            .map(|r| ConsumerDto {
                consumer_id: r.get("consumer_id"),
                name: r.get("name"),
                portal_id: r.get("portal_id"),
                developer_id: r.get("developer_id"),
            })
            .collect())
    }

    pub async fn login(
        &self,
        role: &str,
        username: &str,
        password: &str,
    ) -> Result<Option<AuthSession>> {
        let hash = sha256_hex(password);
        let found = if role == "admin" {
            sqlx::query("SELECT admin_id, username FROM administrator WHERE username = $1 AND password_hash = $2")
                .bind(username)
                .bind(&hash)
                .fetch_optional(&self.pool)
                .await?
                .map(|r| AuthSession {
                    token: format!("hm_{}", uuid::Uuid::now_v7().simple()),
                    role: "admin".into(),
                    subject_id: r.get("admin_id"),
                    username: r.get("username"),
                    portal_id: None,
                })
        } else {
            sqlx::query("SELECT developer_id, username, portal_id FROM developer WHERE username = $1 AND password_hash = $2 AND status = 'ACTIVE'")
                .bind(username)
                .bind(&hash)
                .fetch_optional(&self.pool)
                .await?
                .map(|r| AuthSession {
                    token: format!("hm_{}", uuid::Uuid::now_v7().simple()),
                    role: "developer".into(),
                    subject_id: r.get("developer_id"),
                    username: r.get("username"),
                    portal_id: Some(r.get("portal_id")),
                })
        };
        if let Some(session) = &found {
            self.auth_sessions
                .write()
                .await
                .insert(session.token.clone(), session.clone());
        }
        Ok(found)
    }

    pub async fn auth_session(&self, token: &str) -> Option<AuthSession> {
        self.auth_sessions.read().await.get(token).cloned()
    }

    pub async fn create_portal(&self, name: &str, description: Option<&str>) -> Result<PortalDto> {
        let portal_id = format!("portal_{}", uuid::Uuid::now_v7().simple());
        sqlx::query(
            "INSERT INTO portal (portal_id, name, description, admin_id) VALUES ($1, $2, $3, 'admin_01')",
        )
        .bind(&portal_id)
        .bind(name)
        .bind(description)
        .execute(&self.pool)
        .await?;
        Ok(PortalDto {
            portal_id,
            name: name.into(),
            description: description.map(|s| s.into()),
        })
    }

    pub async fn create_product(
        &self,
        name: &str,
        product_type: &str,
        description: Option<&str>,
    ) -> Result<ProductDto> {
        let product_id = format!("prod_{}", uuid::Uuid::now_v7().simple());
        sqlx::query(
            "INSERT INTO product (product_id, name, type, description, status) VALUES ($1, $2, $3, $4, 'PUBLISHED')",
        )
        .bind(&product_id)
        .bind(name)
        .bind(product_type)
        .bind(description)
        .execute(&self.pool)
        .await?;
        Ok(ProductDto {
            product_id,
            name: name.into(),
            product_type: product_type.into(),
            description: description.map(|s| s.into()),
            status: "PUBLISHED".into(),
        })
    }

    pub async fn create_consumer(
        &self,
        name: &str,
        portal_id: &str,
        developer_id: &str,
        description: Option<&str>,
    ) -> Result<ConsumerDto> {
        let consumer_id = format!("cons_{}", uuid::Uuid::now_v7().simple());
        sqlx::query(
            "INSERT INTO consumer (consumer_id, name, description, portal_id, developer_id) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&consumer_id)
        .bind(name)
        .bind(description)
        .bind(portal_id)
        .bind(developer_id)
        .execute(&self.pool)
        .await?;
        Ok(ConsumerDto {
            consumer_id,
            name: name.into(),
            portal_id: portal_id.into(),
            developer_id: developer_id.into(),
        })
    }

    pub async fn create_api_key(&self, consumer_id: &str) -> Result<ApiKeyCreated> {
        let consumer =
            sqlx::query("SELECT portal_id, developer_id FROM consumer WHERE consumer_id = $1")
                .bind(consumer_id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| anyhow::anyhow!("consumer not found"))?;
        let raw_key = format!("hm_ak_{}", uuid::Uuid::now_v7().simple());
        let api_key_id = format!("ak_{}", &raw_key[6..18]);
        sqlx::query("INSERT INTO voice_api_key_binding (api_key_id, api_key_sha256, tenant_id, portal_id, consumer_id, status, allowed_modes, rate_limit_rpm, quota_limit_usd_micros, quota_used_usd_micros)
             VALUES ($1, $2, $3, $4, $5, 'ACTIVE', 'realtime,cascade', 120, 100000000, 0)",
        )
        .bind(&api_key_id)
        .bind(sha256_hex(&raw_key))
        .bind(consumer.get::<String, _>("developer_id"))
        .bind(consumer.get::<String, _>("portal_id"))
        .bind(consumer_id)
        .execute(&self.pool)
        .await?;
        self.key_cache.write().await.clear();
        Ok(ApiKeyCreated {
            api_key_id,
            raw_key,
        })
    }

    pub async fn list_api_keys(&self, tenant_id: Option<&str>) -> Result<Vec<ApiKeyDto>> {
        let rows = if let Some(tid) = tenant_id {
            sqlx::query("SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, quota_used_usd_micros, quota_limit_usd_micros, quota_used_calls, quota_limit_calls, quota_used_tokens, quota_limit_tokens, alert_threshold_pct, alert_webhook_url FROM voice_api_key_binding WHERE tenant_id = $1 ORDER BY id DESC")
                .bind(tid)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query("SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, quota_used_usd_micros, quota_limit_usd_micros, quota_used_calls, quota_limit_calls, quota_used_tokens, quota_limit_tokens, alert_threshold_pct, alert_webhook_url FROM voice_api_key_binding ORDER BY id DESC")
                .fetch_all(&self.pool)
                .await?
        };
        Ok(rows
            .into_iter()
            .map(|r| ApiKeyDto {
                api_key_id: r.get("api_key_id"),
                tenant_id: r.get("tenant_id"),
                portal_id: r.get("portal_id"),
                consumer_id: r.get("consumer_id"),
                status: r.get("status"),
                allowed_modes: r.get("allowed_modes"),
                quota_used_usd_micros: r.get("quota_used_usd_micros"),
                quota_limit_usd_micros: r.get("quota_limit_usd_micros"),
                quota_used_calls: r.get("quota_used_calls"),
                quota_limit_calls: r.get("quota_limit_calls"),
                quota_used_tokens: r.get("quota_used_tokens"),
                quota_limit_tokens: r.get("quota_limit_tokens"),
                alert_threshold_pct: r.get("alert_threshold_pct"),
                alert_webhook_url: r.get("alert_webhook_url"),
            })
            .collect())
    }

    pub async fn update_api_key_quota(
        &self,
        api_key_id: &str,
        quota_limit_calls: Option<i64>,
        quota_limit_tokens: Option<i64>,
        quota_limit_usd_micros: Option<i64>,
        alert_threshold_pct: Option<i32>,
        alert_webhook_url: Option<&str>,
        reset_used: bool,
    ) -> Result<ApiKeyDto> {
        if reset_used {
            sqlx::query(
                "UPDATE voice_api_key_binding
                 SET quota_used_calls = 0, quota_used_tokens = 0, quota_used_usd_micros = 0
                 WHERE api_key_id = $1",
            )
            .bind(api_key_id)
            .execute(&self.pool)
            .await?;
            sqlx::query("DELETE FROM quota_alert_event WHERE api_key_id = $1")
                .bind(api_key_id)
                .execute(&self.pool)
                .await?;
        }
        sqlx::query(
            "UPDATE voice_api_key_binding
             SET quota_limit_calls = $1,
                 quota_limit_tokens = $2,
                 quota_limit_usd_micros = COALESCE($3, quota_limit_usd_micros),
                 alert_threshold_pct = COALESCE($4, alert_threshold_pct),
                 alert_webhook_url = COALESCE($5, alert_webhook_url)
             WHERE api_key_id = $6",
        )
        .bind(quota_limit_calls)
        .bind(quota_limit_tokens)
        .bind(quota_limit_usd_micros)
        .bind(alert_threshold_pct)
        .bind(alert_webhook_url)
        .bind(api_key_id)
        .execute(&self.pool)
        .await?;

        self.evaluate_and_record_quota_alerts(api_key_id).await?;
        self.key_cache.write().await.clear();

        let all = self.list_api_keys(None).await?;
        all.into_iter()
            .find(|k| k.api_key_id == api_key_id)
            .ok_or_else(|| anyhow::anyhow!("api_key not found"))
    }

    pub async fn update_api_key_status(&self, api_key_id: &str, status: &str) -> Result<ApiKeyDto> {
        let norm = if status.trim().eq_ignore_ascii_case("DISABLED") {
            "DISABLED"
        } else {
            "ACTIVE"
        };
        sqlx::query("UPDATE voice_api_key_binding SET status = $1 WHERE api_key_id = $2")
            .bind(norm)
            .bind(api_key_id)
            .execute(&self.pool)
            .await?;
        self.key_cache.write().await.clear();
        let all = self.list_api_keys(None).await?;
        all.into_iter()
            .find(|k| k.api_key_id == api_key_id)
            .ok_or_else(|| anyhow::anyhow!("api_key not found"))
    }

    pub async fn evaluate_and_record_quota_alerts(&self, api_key_id: &str) -> Result<()> {
        let Some(row) = sqlx::query(
            "SELECT tenant_id, consumer_id, quota_used_calls, quota_limit_calls,
                    quota_used_tokens, quota_limit_tokens, quota_used_usd_micros, quota_limit_usd_micros,
                    alert_threshold_pct, alert_webhook_url
             FROM voice_api_key_binding WHERE api_key_id = $1",
        )
        .bind(api_key_id)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(());
        };

        let tenant_id: String = row.get("tenant_id");
        let consumer_id: String = row.get("consumer_id");
        let threshold_pct: i32 = row.get::<i32, _>("alert_threshold_pct").clamp(1, 100);
        let webhook_url: String = row.get("alert_webhook_url");
        let channel = if webhook_url.trim().is_empty() {
            "console"
        } else {
            "console+webhook"
        };

        let checks: [(&str, i64, Option<i64>); 3] = [
            (
                "calls",
                row.get("quota_used_calls"),
                row.get("quota_limit_calls"),
            ),
            (
                "tokens",
                row.get("quota_used_tokens"),
                row.get("quota_limit_tokens"),
            ),
            (
                "usd_micros",
                row.get("quota_used_usd_micros"),
                row.get("quota_limit_usd_micros"),
            ),
        ];

        for (metric_type, used, limit_opt) in checks {
            let Some(limit) = limit_opt.filter(|l| *l > 0) else {
                continue;
            };
            let pct = ((used * 100) / limit) as i32;
            if pct < threshold_pct {
                // If admin raised limit so usage dropped below threshold, clean up old CRITICAL_100 alerts for this metric
                sqlx::query(
                    "DELETE FROM quota_alert_event WHERE api_key_id = $1 AND metric_type = $2",
                )
                .bind(api_key_id)
                .bind(metric_type)
                .execute(&self.pool)
                .await?;
                continue;
            }
            let level = if used >= limit {
                "CRITICAL_100"
            } else {
                "WARN_80"
            };
            let msg = if level == "CRITICAL_100" {
                format!(
                    "【额度停用告警 100%】业务线 {} (Key: {}) 的 {} 用量已达 {}/{} ({}%)，后续调用已自动拦截",
                    consumer_id, api_key_id, metric_type, used, limit, pct
                )
            } else {
                format!(
                    "【额度预警 {}%】业务线 {} (Key: {}) 的 {} 用量已达 {}/{} ({}%)",
                    threshold_pct, consumer_id, api_key_id, metric_type, used, limit, pct
                )
            };

            let existing_cnt: i64 = sqlx::query(
                "SELECT COUNT(*) AS n FROM quota_alert_event
                 WHERE api_key_id = $1 AND metric_type = $2 AND level = $3",
            )
            .bind(api_key_id)
            .bind(metric_type)
            .bind(level)
            .fetch_one(&self.pool)
            .await?
            .get("n");

            if existing_cnt == 0 {
                sqlx::query(
                    "INSERT INTO quota_alert_event (
                        tenant_id, api_key_id, consumer_id, level, metric_type,
                        used_value, limit_value, usage_pct, notified_channel, message
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                )
                .bind(&tenant_id)
                .bind(api_key_id)
                .bind(&consumer_id)
                .bind(level)
                .bind(metric_type)
                .bind(used)
                .bind(limit)
                .bind(pct)
                .bind(channel)
                .bind(&msg)
                .execute(&self.pool)
                .await?;
            } else {
                sqlx::query(
                    "UPDATE quota_alert_event
                     SET used_value = $1, limit_value = $2, usage_pct = $3, message = $4
                     WHERE api_key_id = $5 AND metric_type = $6 AND level = $7",
                )
                .bind(used)
                .bind(limit)
                .bind(pct)
                .bind(&msg)
                .bind(api_key_id)
                .bind(metric_type)
                .bind(level)
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_quota_alerts(&self, tenant_id: Option<&str>) -> Result<Vec<QuotaAlertDto>> {
        let rows = if let Some(tid) = tenant_id {
            sqlx::query(
                "SELECT id, tenant_id, api_key_id, consumer_id, level, metric_type,
                        used_value, limit_value, usage_pct, notified_channel, message, created_at
                 FROM quota_alert_event WHERE tenant_id = $1 ORDER BY id DESC LIMIT 50",
            )
            .bind(tid)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT id, tenant_id, api_key_id, consumer_id, level, metric_type,
                        used_value, limit_value, usage_pct, notified_channel, message, created_at
                 FROM quota_alert_event ORDER BY id DESC LIMIT 50",
            )
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows
            .into_iter()
            .map(|r| QuotaAlertDto {
                id: r.get("id"),
                tenant_id: r.get("tenant_id"),
                api_key_id: r.get("api_key_id"),
                consumer_id: r.get("consumer_id"),
                level: r.get("level"),
                metric_type: r.get("metric_type"),
                used_value: r.get("used_value"),
                limit_value: r.get("limit_value"),
                usage_pct: r.get("usage_pct"),
                notified_channel: r.get("notified_channel"),
                message: r.get("message"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    pub async fn query_usage_report(
        &self,
        tenant_id: &str,
        date: Option<&str>,
        api_key_id: Option<&str>,
        consumer_id: Option<&str>,
        category: Option<&str>,
        vendor: Option<&str>,
    ) -> Result<UsageReportResponse> {
        let date_filter = date.map(str::trim).filter(|s| !s.is_empty());
        let ak_filter = api_key_id.map(str::trim).filter(|s| !s.is_empty());
        let cons_filter = consumer_id.map(str::trim).filter(|s| !s.is_empty());
        let cat_filter = category.map(str::trim).filter(|s| !s.is_empty());
        let ven_filter = vendor.map(str::trim).filter(|s| !s.is_empty());

        let rows = sqlx::query(
            "SELECT d.stat_date, d.tenant_id, d.api_key_id, d.consumer_id,
                    COALESCE(c.name, d.consumer_id) AS consumer_name,
                    d.category, d.vendor, d.model,
                    d.call_count, d.success_count, d.failure_count,
                    d.sum_prompt_tokens, d.sum_completion_tokens,
                    d.sum_audio_duration_ms, d.sum_tts_characters,
                    d.sum_latency_ms, d.sum_cost_usd_micros
             FROM usage_daily_three_dim_rollup d
             LEFT JOIN consumer c ON c.consumer_id = d.consumer_id
             WHERE d.tenant_id = $1
               AND ($2::text IS NULL OR d.stat_date = $2)
               AND ($3::text IS NULL OR d.api_key_id = $3)
               AND ($4::text IS NULL OR d.consumer_id = $4)
               AND ($5::text IS NULL OR d.category = $5)
               AND ($6::text IS NULL OR d.vendor = $6)
             ORDER BY d.stat_date DESC, d.call_count DESC, d.id DESC",
        )
        .bind(tenant_id)
        .bind(date_filter)
        .bind(ak_filter)
        .bind(cons_filter)
        .bind(cat_filter)
        .bind(ven_filter)
        .fetch_all(&self.pool)
        .await?;

        let mut total_calls = 0i64;
        let mut total_success = 0i64;
        let mut total_failed = 0i64;
        let mut total_tokens = 0i64;
        let mut total_audio_ms = 0i64;
        let mut total_latency_ms = 0i64;
        let mut total_cost_usd_micros = 0i64;
        let mut out_rows = Vec::with_capacity(rows.len());

        for r in rows {
            let call_count: i64 = r.get("call_count");
            let success_count: i64 = r.get("success_count");
            let failure_count: i64 = r.get("failure_count");
            let p_tok: i64 = r.get("sum_prompt_tokens");
            let c_tok: i64 = r.get("sum_completion_tokens");
            let row_tokens = p_tok + c_tok;
            let audio_ms: i64 = r.get("sum_audio_duration_ms");
            let tts_ch: i64 = r.get("sum_tts_characters");
            let sum_lat: i64 = r.get("sum_latency_ms");
            let cost: i64 = r.get("sum_cost_usd_micros");

            total_calls += call_count;
            total_success += success_count;
            total_failed += failure_count;
            total_tokens += row_tokens;
            total_audio_ms += audio_ms;
            total_latency_ms += sum_lat;
            total_cost_usd_micros += cost;

            out_rows.push(UsageReportRow {
                stat_date: r.get("stat_date"),
                tenant_id: r.get("tenant_id"),
                api_key_id: r.get("api_key_id"),
                consumer_id: r.get("consumer_id"),
                consumer_name: r.get("consumer_name"),
                category: r.get("category"),
                vendor: r.get("vendor"),
                model: r.get("model"),
                call_count,
                success_count,
                failure_count,
                prompt_tokens: p_tok,
                completion_tokens: c_tok,
                total_tokens: row_tokens,
                audio_duration_ms: audio_ms,
                audio_seconds: (audio_ms as f64) / 1000.0,
                tts_characters: tts_ch,
                avg_latency_ms: if call_count > 0 {
                    sum_lat / call_count
                } else {
                    0
                },
                cost_usd_micros: cost,
            });
        }

        let success_rate = if total_calls > 0 {
            ((total_success as f64) * 10000.0 / (total_calls as f64)).round() / 100.0
        } else {
            100.0
        };
        let avg_latency_ms = if total_calls > 0 {
            total_latency_ms / total_calls
        } else {
            0
        };

        Ok(UsageReportResponse {
            tenant_id: tenant_id.to_string(),
            filter_date: date_filter.map(str::to_string),
            total_calls,
            total_success,
            total_failed,
            success_rate,
            total_tokens,
            total_audio_seconds: (total_audio_ms as f64) / 1000.0,
            avg_latency_ms,
            total_cost_usd_micros,
            rows: out_rows,
        })
    }

    pub async fn list_voice_sessions(
        &self,
        tenant_id: Option<&str>,
    ) -> Result<Vec<VoiceSessionDto>> {
        let rows = if let Some(tid) = tenant_id {
            sqlx::query(
                "SELECT session_id, room_name, tenant_id, api_key_id, voice_mode, task,
                        source_lang, target_lang, title, llm_model, state, created_at
                 FROM voice_session_record WHERE tenant_id = $1 ORDER BY id DESC LIMIT 100",
            )
            .bind(tid)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT session_id, room_name, tenant_id, api_key_id, voice_mode, task,
                        source_lang, target_lang, title, llm_model, state, created_at
                 FROM voice_session_record ORDER BY id DESC LIMIT 100",
            )
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows
            .into_iter()
            .map(|r| VoiceSessionDto {
                session_id: r.get("session_id"),
                room_name: r.get::<Option<String>, _>("room_name"),
                tenant_id: r.get("tenant_id"),
                api_key_id: r.get("api_key_id"),
                voice_mode: r.get("voice_mode"),
                task: r.get("task"),
                source_lang: r.get("source_lang"),
                target_lang: r.get("target_lang"),
                title: r.get("title"),
                llm_model: r.get::<Option<String>, _>("llm_model"),
                state: r.get("state"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_model_config(
        &self,
        tenant_id: &str,
        config_id: &str,
        name: &str,
        kind: &str,
        protocol: &str,
        base_url: &str,
        api_key: &str,
        api_secret: &str,
        model: &str,
        voice: &str,
        is_default: bool,
    ) -> Result<()> {
        if is_default {
            sqlx::query(
                "UPDATE ai_model_config SET is_default = 0 WHERE kind = $1 AND tenant_id = $2",
            )
            .bind(kind)
            .bind(tenant_id)
            .execute(&self.pool)
            .await?;
        }
        let enc_key = encrypt_secret(api_key);
        let enc_secret = encrypt_secret(api_secret);
        let vendor = infer_vendor(protocol, model, base_url);
        sqlx::query("INSERT INTO ai_model_config (config_id, tenant_id, name, kind, protocol, vendor, base_url, api_key, api_secret, extra_json, model, voice, is_default, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{}', $10, $11, $12, 'ACTIVE')")
            .bind(config_id)
            .bind(tenant_id)
            .bind(name)
            .bind(kind)
            .bind(protocol)
            .bind(&vendor)
            .bind(base_url)
            .bind(&enc_key)
            .bind(&enc_secret)
            .bind(model)
            .bind(voice)
            .bind(if is_default { 1 } else { 0 })
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_model_config(
        &self,
        tenant_id: &str,
        name: &str,
        kind: &str,
        protocol: &str,
        base_url: &str,
        api_key: &str,
        api_secret: &str,
        model: &str,
        voice: &str,
        is_default: bool,
    ) -> Result<ModelConfig> {
        let config_id = format!("cfg_{}", uuid::Uuid::now_v7().simple());
        self.insert_model_config(
            tenant_id, &config_id, name, kind, protocol, base_url, api_key, api_secret, model,
            voice, is_default,
        )
        .await?;
        self.get_model_config(&config_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("config missing"))
    }

    pub async fn update_model_extra_langs(
        &self,
        config_id: &str,
        key: &str,
        langs: &[String],
    ) -> Result<ModelConfig> {
        let current = if let Some(found) = self.get_model_config(config_id).await? {
            found
        } else {
            sqlx::query(
                "SELECT config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status
                 FROM ai_model_config WHERE model = $1 OR name = $1 ORDER BY is_default DESC LIMIT 1",
            )
            .bind(config_id.trim())
            .fetch_optional(&self.pool)
            .await?
            .map(model_config_from_row)
            .ok_or_else(|| anyhow::anyhow!("config not found"))?
        };
        let target_id = current.config_id.clone();
        let mut normalized = Vec::with_capacity(langs.len());
        for raw in langs {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                continue;
            }
            let n = normalize_lang(trimmed).map_err(|e| anyhow::anyhow!("target_lang_not_allowed: {}", e))?;
            if !normalized.contains(&n) {
                normalized.push(n);
            }
        }
        let mut extra_obj = serde_json::from_str::<serde_json::Value>(&current.extra_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        if !extra_obj.is_object() {
            extra_obj = serde_json::json!({});
        }
        if let Some(map) = extra_obj.as_object_mut() {
            if normalized.is_empty() {
                map.remove(key);
            } else {
                map.insert(key.to_string(), serde_json::json!(normalized));
            }
        }
        let extra_str = serde_json::to_string(&extra_obj)?;
        sqlx::query("UPDATE ai_model_config SET extra_json = $1 WHERE config_id = $2")
            .bind(&extra_str)
            .bind(&target_id)
            .execute(&self.pool)
            .await?;
        self.get_model_config(&target_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("config not found"))
    }

    pub async fn update_model_credentials(
        &self,
        config_id: &str,
        api_key: Option<&str>,
        api_secret: Option<&str>,
        base_url: Option<&str>,
    ) -> Result<ModelConfig> {
        if let Some(value) = api_key.filter(|v| !v.is_empty()) {
            let enc = encrypt_secret(value);
            sqlx::query("UPDATE ai_model_config SET api_key = $1 WHERE config_id = $2")
                .bind(&enc)
                .bind(config_id)
                .execute(&self.pool)
                .await?;
        }
        if let Some(value) = api_secret.filter(|v| !v.is_empty()) {
            let enc = encrypt_secret(value);
            sqlx::query("UPDATE ai_model_config SET api_secret = $1 WHERE config_id = $2")
                .bind(&enc)
                .bind(config_id)
                .execute(&self.pool)
                .await?;
        }
        if let Some(value) = base_url.filter(|v| !v.is_empty()) {
            sqlx::query("UPDATE ai_model_config SET base_url = $1 WHERE config_id = $2")
                .bind(value)
                .bind(config_id)
                .execute(&self.pool)
                .await?;
        }
        self.get_model_config(config_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("config not found"))
    }

    pub async fn list_model_configs(&self, tenant_id: Option<&str>) -> Result<Vec<ModelConfig>> {
        let rows = if let Some(tenant_id) = tenant_id {
            sqlx::query("SELECT config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config WHERE tenant_id = $1 ORDER BY kind, is_default DESC, name")
                .bind(tenant_id)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query("SELECT config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config ORDER BY tenant_id, kind, is_default DESC, name")
                .fetch_all(&self.pool)
                .await?
        };
        Ok(rows.into_iter().map(model_config_from_row).collect())
    }

    pub async fn get_model_config(&self, config_id: &str) -> Result<Option<ModelConfig>> {
        Ok(sqlx::query("SELECT config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config WHERE config_id = $1")
            .bind(config_id)
            .fetch_optional(&self.pool)
            .await?
            .map(model_config_from_row))
    }

    pub async fn set_default_model_config(&self, config_id: &str) -> Result<ModelConfig> {
        let current = self
            .get_model_config(config_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("config not found"))?;
        sqlx::query(
            "UPDATE ai_model_config SET is_default = 0 WHERE kind = $1 AND tenant_id = $2",
        )
        .bind(&current.kind)
        .bind(&current.tenant_id)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "UPDATE ai_model_config SET is_default = 1, status = 'ACTIVE' WHERE config_id = $1",
        )
        .bind(config_id)
        .execute(&self.pool)
        .await?;
        Ok(self.get_model_config(config_id).await?.unwrap())
    }

    pub async fn disable_model_config(&self, config_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE ai_model_config SET status = 'DISABLED', is_default = 0 WHERE config_id = $1",
        )
        .bind(config_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn enable_model_config(&self, config_id: &str) -> Result<ModelConfig> {
        sqlx::query("UPDATE ai_model_config SET status = 'ACTIVE' WHERE config_id = $1")
            .bind(config_id)
            .execute(&self.pool)
            .await?;
        self.get_model_config(config_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("config not found"))
    }

    pub async fn update_vendor_status(
        &self,
        tenant_id: &str,
        vendor: &str,
        status: &str,
    ) -> Result<u64> {
        let norm_status = if status.trim().eq_ignore_ascii_case("DISABLED") {
            "DISABLED"
        } else {
            "ACTIVE"
        };
        let res = sqlx::query(
            "UPDATE ai_model_config SET status = $1 WHERE tenant_id = $2 AND vendor = $3",
        )
        .bind(norm_status)
        .bind(tenant_id)
        .bind(vendor.trim().to_ascii_lowercase())
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    pub async fn get_route_target_config(
        &self,
        tenant_id: &str,
        capability: &str,
        langs: &[&str],
    ) -> Result<Option<ModelConfig>> {
        for lang in langs {
            if lang.trim().is_empty() {
                continue;
            }
            if let Some(r) = sqlx::query(
                "SELECT c.config_id, c.tenant_id, c.name, c.kind, c.protocol, c.base_url,
                        c.api_key, c.api_secret, c.extra_json, c.model, c.voice, c.is_default, c.status
                 FROM ai_route_policy p
                 JOIN ai_model_config c ON c.config_id = p.config_id
                 WHERE p.tenant_id = $1 AND p.capability = $2 AND p.language = $3 AND c.status = 'ACTIVE'
                 LIMIT 1",
            )
            .bind(tenant_id)
            .bind(capability)
            .bind(lang.trim())
            .fetch_optional(&self.pool)
            .await?
            {
                return Ok(Some(model_config_from_row(r)));
            }
        }
        Ok(None)
    }

    pub async fn resolve_route_config(
        &self,
        tenant_id: &str,
        capability: &str,
        language: Option<&str>,
        requested_model: Option<&str>,
    ) -> Result<Option<ModelConfig>> {
        let kind = match capability {
            "asr" | "stt" => "stt",
            "tts" => "tts",
            "realtime" => "realtime",
            _ => "llm",
        };
        let req_clean = requested_model
            .map(str::trim)
            .filter(|s| !s.is_empty() && *s != "auto");
        if let Some(req) = req_clean {
            if let Some(cfg) = self.resolve_model_config(tenant_id, kind, Some(req)).await? {
                return Ok(Some(cfg));
            }
        }

        let norm_lang = language
            .map(str::trim)
            .filter(|s| !s.is_empty() && *s != "*")
            .and_then(|s| normalize_lang(s).ok());
        let primary_lang = norm_lang.as_deref().and_then(primary_lang_subtag);

        let mut candidates: Vec<&str> = Vec::new();
        if let Some(ref nl) = norm_lang {
            candidates.push(nl.as_str());
        }
        if let Some(ref pl) = primary_lang {
            if !candidates.contains(&pl.as_str()) {
                candidates.push(pl.as_str());
            }
        }
        candidates.push("*");

        if let Some(cfg) = self
            .get_route_target_config(tenant_id, capability, &candidates)
            .await?
        {
            return Ok(Some(cfg));
        }

        match kind {
            "stt" => Ok(self
                .select_stt_for_lang(tenant_id, norm_lang.as_deref(), None)
                .await
                .ok()
                .map(|s| s.config)),
            "tts" => Ok(self
                .select_tts_for_lang(tenant_id, norm_lang.as_deref().unwrap_or("zh"))
                .await
                .ok()
                .map(|s| s.config)),
            _ => self.resolve_model_config(tenant_id, kind, None).await,
        }
    }

    pub async fn list_routes(&self, tenant_id: &str) -> Result<Vec<RoutePolicyView>> {
        let rows = sqlx::query(
            "SELECT p.tenant_id, p.capability, p.language, p.config_id, p.updated_at,
                    c.vendor, c.model, c.name, c.protocol, c.status, c.base_url
             FROM ai_route_policy p
             JOIN ai_model_config c ON c.config_id = p.config_id
             WHERE p.tenant_id = $1
             ORDER BY p.capability, p.language",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let raw_vendor: String = r.get("vendor");
                let protocol: String = r.get("protocol");
                let model: String = r.get("model");
                let base_url: String = r.get("base_url");
                let vendor = if raw_vendor.trim().is_empty() {
                    infer_vendor(&protocol, &model, &base_url)
                } else {
                    raw_vendor
                };
                RoutePolicyView {
                    tenant_id: r.get("tenant_id"),
                    capability: r.get("capability"),
                    language: r.get("language"),
                    config_id: r.get("config_id"),
                    vendor,
                    model,
                    name: r.get("name"),
                    protocol,
                    status: r.get("status"),
                    updated_at: r.get("updated_at"),
                }
            })
            .collect())
    }

    pub async fn upsert_route(
        &self,
        tenant_id: &str,
        capability: &str,
        language: &str,
        config_id: &str,
    ) -> Result<RoutePolicyView> {
        let cap = capability.trim().to_ascii_lowercase();
        if !matches!(
            cap.as_str(),
            "asr" | "tts" | "translate_llm" | "summary_llm" | "chat_llm" | "realtime"
        ) {
            anyhow::bail!("invalid capability: {}", capability);
        }
        let lang = if language.trim().is_empty() || language.trim() == "*" {
            "*".to_string()
        } else {
            normalize_lang(language.trim()).map_err(|e| anyhow::anyhow!("target_lang_not_allowed: {}", e))?
        };

        let cfg = if let Some(found) = self.get_model_config(config_id.trim()).await? {
            found
        } else {
            sqlx::query(
                "SELECT config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status
                 FROM ai_model_config WHERE tenant_id = $1 AND (model = $2 OR name = $2) LIMIT 1",
            )
            .bind(tenant_id)
            .bind(config_id.trim())
            .fetch_optional(&self.pool)
            .await?
            .map(model_config_from_row)
            .ok_or_else(|| anyhow::anyhow!("config not found"))?
        };

        // Ensure the chosen config is ACTIVE and has credentials if it's a cloud protocol so one-click route switch works out of the box
        sqlx::query("UPDATE ai_model_config SET status = 'ACTIVE' WHERE config_id = $1")
            .bind(&cfg.config_id)
            .execute(&self.pool)
            .await?;

        if cfg.api_key.trim().is_empty()
            && !matches!(cfg.protocol.as_str(), "whisper_local" | "piper_local")
        {
            let demo_key = encrypt_secret("mock_demo_vendor_key_2026");
            let demo_sec = encrypt_secret("mock_demo_vendor_secret_2026");
            sqlx::query(
                "UPDATE ai_model_config SET api_key = $1, api_secret = CASE WHEN api_secret = '' THEN $2 ELSE api_secret END WHERE config_id = $3",
            )
            .bind(&demo_key)
            .bind(&demo_sec)
            .bind(&cfg.config_id)
            .execute(&self.pool)
            .await?;
        }

        sqlx::query(
            "INSERT INTO ai_route_policy (tenant_id, capability, language, config_id, updated_at)
             VALUES ($1, $2, $3, $4, (CURRENT_TIMESTAMP)::text)
             ON CONFLICT (tenant_id, capability, language) DO UPDATE SET
                config_id = excluded.config_id,
                updated_at = (CURRENT_TIMESTAMP)::text",
        )
        .bind(tenant_id)
        .bind(&cap)
        .bind(&lang)
        .bind(&cfg.config_id)
        .execute(&self.pool)
        .await?;

        if lang == "*" {
            let _ = self.set_default_model_config(&cfg.config_id).await;
        }

        let routes = self.list_routes(tenant_id).await?;
        routes
            .into_iter()
            .find(|r| r.capability == cap && r.language == lang)
            .ok_or_else(|| anyhow::anyhow!("route not found after upsert"))
    }

    pub async fn resolve_model_config(
        &self,
        tenant_id: &str,
        kind: &str,
        requested: Option<&str>,
    ) -> Result<Option<ModelConfig>> {
        if let Some(name) = requested.map(str::trim).filter(|v| !v.is_empty()) {
            if let Some(found) = sqlx::query("SELECT config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config WHERE status = 'ACTIVE' AND tenant_id = $1 AND kind = $2 AND (config_id = $3 OR model = $4 OR name = $5) ORDER BY is_default DESC LIMIT 1")
                .bind(tenant_id)
                .bind(kind)
                .bind(name)
                .bind(name)
                .bind(name)
                .fetch_optional(&self.pool)
                .await?
            {
                return Ok(Some(model_config_from_row(found)));
            }
        }
        Ok(sqlx::query("SELECT config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config WHERE status = 'ACTIVE' AND tenant_id = $1 AND kind = $2 AND is_default = 1 LIMIT 1")
            .bind(tenant_id)
            .bind(kind)
            .fetch_optional(&self.pool)
            .await?
            .map(model_config_from_row))
    }

    /// Resolve text model for translation (`purpose == "translate"` checks `translate_llm` route
    /// then `kind='translate'`, otherwise falls back to `kind='llm'`) or summary/chat.
    pub async fn resolve_text_model(
        &self,
        tenant_id: &str,
        purpose: &str,
        requested: Option<&str>,
    ) -> Result<Option<ModelConfig>> {
        let req_clean = requested
            .map(str::trim)
            .filter(|s| !s.is_empty() && *s != "auto");
        if req_clean.is_none() {
            let route_cap = match purpose {
                "translate" | "translate_llm" => "translate_llm",
                "summary" | "summary_llm" => "summary_llm",
                _ => "chat_llm",
            };
            if let Ok(Some(route_cfg)) = self
                .get_route_target_config(tenant_id, route_cap, &["*"])
                .await
            {
                if is_llm_runnable(&route_cfg) {
                    return Ok(Some(route_cfg));
                }
            }
        }
        if purpose == "translate" {
            if let Some(cfg) = self
                .resolve_model_config(tenant_id, "translate", req_clean)
                .await?
            {
                return Ok(Some(cfg));
            }
        }
        self.resolve_model_config(tenant_id, "llm", req_clean).await
    }

    pub async fn ensure_tts_for_session(
        &self,
        session_id: &str,
        tenant_id: &str,
        target_lang: &str,
        current_tts_config_id: Option<&str>,
    ) -> Result<(Option<ModelConfig>, bool, Option<String>)> {
        let norm_target = ensure_allowed(
            Some(target_lang),
            &self
                .get_language_policy(tenant_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("language_policy_missing: tenant policy missing"))?,
        )
        .map_err(|e| anyhow::anyhow!(e))?;

        if let Some(cid) = current_tts_config_id.filter(|s| !s.is_empty()) {
            if let Some(cfg) = self.get_model_config(cid).await? {
                if cfg.tenant_id == tenant_id && tts_matches_target_lang(&cfg, &norm_target) {
                    return Ok((Some(cfg), false, None));
                }
            }
        }

        match self.select_tts_for_lang(tenant_id, &norm_target).await {
            Ok(sel) => {
                self.set_session_tts_config(session_id, Some(&sel.config.config_id))
                    .await?;
                Ok((Some(sel.config), sel.voice_fallback, None))
            }
            Err(TtsSelectError::Unavailable) => {
                self.set_session_tts_config(session_id, None).await?;
                Ok((None, false, Some("tts_unavailable".to_string())))
            }
        }
    }

    async fn seed_livekit_catalog(&self) -> Result<()> {
        let catalog: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../../../catalog/livekit_models.json"))?;
        for item in catalog {
            let protocol = item["protocol"].as_str().unwrap_or_default();
            let model = item["model"].as_str().unwrap_or_default();
            let base_url = item["base_url"].as_str().unwrap_or_default();
            let vendor = item["vendor"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| infer_vendor(protocol, model, base_url));

            sqlx::query("INSERT INTO ai_model_config (config_id, tenant_id, name, kind, protocol, vendor, base_url, api_key, api_secret, extra_json, model, voice, is_default, status)
                 VALUES ($1, 'tenant_ugreen_01', $2, $3, $4, $5, $6, '', '', '{}', $7, $8, 0, 'ACTIVE')
                 ON CONFLICT (config_id) DO UPDATE SET vendor = CASE WHEN ai_model_config.vendor = '' THEN excluded.vendor ELSE ai_model_config.vendor END",
            )
            .bind(item["config_id"].as_str().unwrap_or_default())
            .bind(item["name"].as_str().unwrap_or_default())
            .bind(item["kind"].as_str().unwrap_or_default())
            .bind(protocol)
            .bind(&vendor)
            .bind(base_url)
            .bind(model)
            .bind(item["voice"].as_str().unwrap_or_default())
            .execute(&self.pool)
            .await?;
        }
        self.ensure_model_default("llm", "openai_chat", "deepseek-v4.1-flash:1017-cloud")
            .await?;
        self.ensure_model_default("stt", "whisper_local", "faster-whisper-tiny")
            .await?;
        self.ensure_model_default("tts", "piper_local", "zh_CN-huayan-medium")
            .await?;

        let enc_ollama_key =
            encrypt_secret("cb4631cad50c4e3eb1342b9ef0075dd5.Ym7un785NAKQCVzDMK8DjK52");
        sqlx::query("UPDATE ai_model_config SET api_key = $1 WHERE protocol = 'openai_chat' AND model = 'deepseek-v4.1-flash:1017-cloud' AND (api_key = '' OR api_key NOT LIKE 'enc:v1:%')")
            .bind(&enc_ollama_key)
            .execute(&self.pool)
            .await?;

        // Encrypt any remaining legacy plaintext keys in ai_model_config
        let plain_rows = sqlx::query(
            "SELECT config_id, api_key, api_secret FROM ai_model_config
             WHERE (api_key <> '' AND api_key NOT LIKE 'enc:v1:%')
                OR (api_secret <> '' AND api_secret NOT LIKE 'enc:v1:%')",
        )
        .fetch_all(&self.pool)
        .await?;
        for r in plain_rows {
            let cid: String = r.get("config_id");
            let ak: String = r.get("api_key");
            let sk: String = r.get("api_secret");
            sqlx::query("UPDATE ai_model_config SET api_key = $1, api_secret = $2 WHERE config_id = $3")
                .bind(encrypt_secret(&ak))
                .bind(encrypt_secret(&sk))
                .bind(&cid)
                .execute(&self.pool)
                .await?;
        }

        // Seed default ai_route_policy rows for tenant_ugreen_01
        let default_routes = [
            ("asr", "*", "lk-stt-whisper_local-faster-whisper-tiny"),
            ("tts", "*", "lk-tts-piper_local-zh_CN-huayan-medium"),
            (
                "translate_llm",
                "*",
                "lk-llm-openai_chat-deepseek-v4-1-flash-1017-cloud",
            ),
            (
                "summary_llm",
                "*",
                "lk-llm-openai_chat-deepseek-v4-1-flash-1017-cloud",
            ),
            (
                "chat_llm",
                "*",
                "lk-llm-openai_chat-deepseek-v4-1-flash-1017-cloud",
            ),
            (
                "realtime",
                "*",
                "vendor-realtime-dashscope_realtime-qwen3-8-omni-flash-realtime",
            ),
        ];
        for (cap, lang, cid) in default_routes {
            sqlx::query(
                "INSERT INTO ai_route_policy (tenant_id, capability, language, config_id)
                 VALUES ('tenant_ugreen_01', $1, $2, $3)
                 ON CONFLICT (tenant_id, capability, language) DO NOTHING",
            )
            .bind(cap)
            .bind(lang)
            .bind(cid)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn ensure_model_default(&self, kind: &str, protocol: &str, model: &str) -> Result<()> {
        let existing: i64 = sqlx::query("SELECT COUNT(*) AS n FROM ai_model_config WHERE tenant_id = 'tenant_ugreen_01' AND kind = $1 AND is_default = 1")
            .bind(kind)
            .fetch_one(&self.pool)
            .await?
            .get("n");
        if existing == 0 {
            sqlx::query("UPDATE ai_model_config SET is_default = 1 WHERE config_id = (SELECT config_id FROM ai_model_config WHERE tenant_id = 'tenant_ugreen_01' AND kind = $1 AND protocol = $2 AND model = $3 LIMIT 1)")
                .bind(kind)
                .bind(protocol)
                .bind(model)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }
}

fn model_config_from_row(row: sqlx::postgres::PgRow) -> ModelConfig {
    let raw_key: String = row.get("api_key");
    let raw_sec: String = row.get("api_secret");
    ModelConfig {
        config_id: row.get("config_id"),
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        kind: row.get("kind"),
        protocol: row.get("protocol"),
        base_url: row.get("base_url"),
        api_key: decrypt_secret(&raw_key),
        api_secret: decrypt_secret(&raw_sec),
        extra_json: row.get("extra_json"),
        model: row.get("model"),
        voice: row.get("voice"),
        is_default: row.get::<i32, _>("is_default") == 1,
        status: row.get("status"),
    }
}
