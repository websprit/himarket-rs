use anyhow::Result;
use chrono::{DateTime, Utc};
use himarket_core::{
    CreateVoiceSessionRequest, RtcUsageBreakdown, TenantLanguagePolicy, TextLlmUsageBreakdown,
    UsageEventPayload, UsageSummaryResponse, VerifiedBusinessKey, VoiceModelUsageBreakdown,
    ensure_allowed, normalize_lang, primary_lang_subtag, sha256_hex, validate_language_policy,
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
        "openai_tts" => !cfg.api_key.trim().is_empty(),
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
        "openai_stt" | "google_stt" => !cfg.api_key.trim().is_empty(),
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
             VALUES ('tenant_ugreen_01', 'zh', '[\"zh\",\"en\",\"ja\",\"ko\"]')
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

        let row = sqlx::query("SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, rate_limit_rpm, quota_limit_usd_micros, quota_used_usd_micros
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
        if let Some(req) = requested_model.map(str::trim).filter(|s| !s.is_empty()) {
            if let Some((_, matched)) = all_configs.iter().find(|(_, c)| {
                c.config_id == req || c.model == req || c.name == req
            }) {
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

        let res = sqlx::query("INSERT INTO usage_event_ledger (
                event_id, idempotency_key, session_id, room_name, tenant_id, api_key_id,
                source, category, model, prompt_tokens, completion_tokens, cached_tokens,
                audio_input_tokens, audio_output_tokens, audio_duration_ms, tts_characters,
                rtc_duration_ms, rtc_bytes, cost_usd_micros, occurred_at_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
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

        let dt = DateTime::<Utc>::from_timestamp_millis(ev.occurred_at_ms).unwrap_or_else(Utc::now);
        let stat_hour = dt.format("%Y-%m-%d %H:00:00").to_string();

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

        sqlx::query("UPDATE voice_api_key_binding SET quota_used_usd_micros = quota_used_usd_micros + $1 WHERE api_key_id = $2",
        )
        .bind(cost_micros)
        .bind(&ev.api_key_id)
        .execute(&self.pool)
        .await?;

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
            sqlx::query("SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, quota_used_usd_micros FROM voice_api_key_binding WHERE tenant_id = $1 ORDER BY id DESC")
                .bind(tid)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query("SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, quota_used_usd_micros FROM voice_api_key_binding ORDER BY id DESC")
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
            })
            .collect())
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
        sqlx::query("INSERT INTO ai_model_config (config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '{}', $9, $10, $11, 'ACTIVE')")
            .bind(config_id)
            .bind(tenant_id)
            .bind(name)
            .bind(kind)
            .bind(protocol)
            .bind(base_url)
            .bind(api_key)
            .bind(api_secret)
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
            sqlx::query("UPDATE ai_model_config SET api_key = $1 WHERE config_id = $2")
                .bind(value)
                .bind(config_id)
                .execute(&self.pool)
                .await?;
        }
        if let Some(value) = api_secret.filter(|v| !v.is_empty()) {
            sqlx::query("UPDATE ai_model_config SET api_secret = $1 WHERE config_id = $2")
                .bind(value)
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

    /// Resolve text model for translation (`purpose == "translate"` prefers `kind='translate'` if
    /// configured, otherwise falls back to `kind='llm'`) or summary/chat (`kind='llm'`).
    pub async fn resolve_text_model(
        &self,
        tenant_id: &str,
        purpose: &str,
        requested: Option<&str>,
    ) -> Result<Option<ModelConfig>> {
        if purpose == "translate" {
            if let Some(cfg) = self
                .resolve_model_config(tenant_id, "translate", requested)
                .await?
            {
                return Ok(Some(cfg));
            }
        }
        self.resolve_model_config(tenant_id, "llm", requested).await
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
            sqlx::query("INSERT INTO ai_model_config (config_id, tenant_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status)
                 VALUES ($1, 'tenant_ugreen_01', $2, $3, $4, $5, '', '', '{}', $6, $7, 0, 'ACTIVE')
                 ON CONFLICT (config_id) DO NOTHING",
            )
            .bind(item["config_id"].as_str().unwrap_or_default())
            .bind(item["name"].as_str().unwrap_or_default())
            .bind(item["kind"].as_str().unwrap_or_default())
            .bind(item["protocol"].as_str().unwrap_or_default())
            .bind(item["base_url"].as_str().unwrap_or_default())
            .bind(item["model"].as_str().unwrap_or_default())
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
        sqlx::query("UPDATE ai_model_config SET api_key = $1 WHERE protocol = 'openai_chat' AND model = 'deepseek-v4.1-flash:1017-cloud' AND api_key = ''")
            .bind("cb4631cad50c4e3eb1342b9ef0075dd5.Ym7un785NAKQCVzDMK8DjK52")
            .execute(&self.pool)
            .await?;
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
    ModelConfig {
        config_id: row.get("config_id"),
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        kind: row.get("kind"),
        protocol: row.get("protocol"),
        base_url: row.get("base_url"),
        api_key: row.get("api_key"),
        api_secret: row.get("api_secret"),
        extra_json: row.get("extra_json"),
        model: row.get("model"),
        voice: row.get("voice"),
        is_default: row.get::<i32, _>("is_default") == 1,
        status: row.get("status"),
    }
}
