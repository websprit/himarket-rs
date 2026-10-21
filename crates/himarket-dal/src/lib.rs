use anyhow::Result;
use chrono::{DateTime, Utc};
use himarket_core::{
    CreateVoiceSessionRequest, RtcUsageBreakdown, TextLlmUsageBreakdown, UsageEventPayload,
    UsageSummaryResponse, VerifiedBusinessKey, VoiceModelUsageBreakdown, sha256_hex,
};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DalRepo {
    pool: SqlitePool,
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
pub struct VoiceSessionDto {
    pub session_id: String,
    pub room_name: String,
    pub tenant_id: String,
    pub api_key_id: String,
    pub voice_mode: String,
    pub llm_model: String,
    pub state: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionIdentity {
    pub session_id: String,
    pub room_name: String,
    pub tenant_id: String,
    pub portal_id: String,
    pub consumer_id: String,
    pub api_key_id: String,
    pub voice_mode: String,
    pub llm_model: String,
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

impl DalRepo {
    pub async fn connect_and_init(db_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(16)
            .connect(db_url)
            .await?;

        let repo = Self {
            pool,
            key_cache: Arc::new(RwLock::new(HashMap::new())),
            session_cache: Arc::new(RwLock::new(HashMap::new())),
            auth_sessions: Arc::new(RwLock::new(HashMap::new())),
        };
        repo.init_schema_and_seed().await?;
        Ok(repo)
    }

    async fn init_schema_and_seed(&self) -> Result<()> {
        let ddl = r#"
        CREATE TABLE IF NOT EXISTS administrator (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS portal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            admin_id TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS developer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            developer_id TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL,
            email TEXT,
            portal_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS consumer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            consumer_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            portal_id TEXT NOT NULL,
            developer_id TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS product (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL UNIQUE,
            type TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'PUBLISHED',
            auto_approve INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS product_subscription (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            consumer_id TEXT NOT NULL,
            developer_id TEXT NOT NULL,
            portal_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            UNIQUE(product_id, consumer_id)
        );
        CREATE TABLE IF NOT EXISTS voice_api_key_binding (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key_id TEXT NOT NULL UNIQUE,
            api_key_sha256 TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL,
            portal_id TEXT NOT NULL,
            consumer_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            allowed_modes TEXT NOT NULL DEFAULT 'realtime,cascade',
            rate_limit_rpm INTEGER NOT NULL DEFAULT 120,
            quota_limit_usd_micros INTEGER,
            quota_used_usd_micros INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS voice_session_record (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL UNIQUE,
            room_name TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL,
            portal_id TEXT NOT NULL,
            consumer_id TEXT NOT NULL,
            api_key_id TEXT NOT NULL,
            voice_mode TEXT NOT NULL,
            llm_model TEXT,
            state TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS usage_event_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            room_name TEXT,
            tenant_id TEXT NOT NULL,
            api_key_id TEXT NOT NULL,
            source TEXT NOT NULL,
            category TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            cached_tokens INTEGER,
            audio_input_tokens INTEGER,
            audio_output_tokens INTEGER,
            audio_duration_ms INTEGER,
            tts_characters INTEGER,
            rtc_duration_ms INTEGER,
            rtc_bytes INTEGER,
            cost_usd_micros INTEGER NOT NULL DEFAULT 0,
            occurred_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ai_model_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_id TEXT NOT NULL UNIQUE,
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
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS usage_hourly_rollup (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stat_hour TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            api_key_id TEXT NOT NULL,
            category TEXT NOT NULL,
            model TEXT NOT NULL,
            event_count INTEGER NOT NULL DEFAULT 0,
            sum_prompt_tokens INTEGER NOT NULL DEFAULT 0,
            sum_completion_tokens INTEGER NOT NULL DEFAULT 0,
            sum_cached_tokens INTEGER NOT NULL DEFAULT 0,
            sum_audio_input_tokens INTEGER NOT NULL DEFAULT 0,
            sum_audio_output_tokens INTEGER NOT NULL DEFAULT 0,
            sum_audio_duration_ms INTEGER NOT NULL DEFAULT 0,
            sum_tts_characters INTEGER NOT NULL DEFAULT 0,
            sum_rtc_duration_ms INTEGER NOT NULL DEFAULT 0,
            sum_rtc_bytes INTEGER NOT NULL DEFAULT 0,
            sum_cost_usd_micros INTEGER NOT NULL DEFAULT 0,
            UNIQUE(stat_hour, tenant_id, api_key_id, category, model)
        );
        "#;

        for stmt in ddl.split(';') {
            let trimmed = stmt.trim();
            if !trimmed.is_empty() {
                sqlx::query(trimmed).execute(&self.pool).await?;
            }
        }
        let _ = sqlx::query("ALTER TABLE developer ADD COLUMN password_hash TEXT")
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("ALTER TABLE ai_model_config ADD COLUMN api_secret TEXT NOT NULL DEFAULT ''")
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("ALTER TABLE ai_model_config ADD COLUMN extra_json TEXT NOT NULL DEFAULT '{}'")
            .execute(&self.pool)
            .await;
        self.seed_livekit_catalog().await?;

        // Seed default Portal, Developer (Tenant), Consumer, Product, and Business API Key for immediate Android & E2E testing
        sqlx::query(
            "INSERT OR IGNORE INTO portal (portal_id, name, description, admin_id) VALUES ('portal_default', 'HiMarket AI Voice Portal', 'Enterprise AI Voice & LLM Gateway Portal', 'admin_01')",
        )
        .execute(&self.pool)
        .await?;

        let admin_hash = sha256_hex("admin123");
        sqlx::query(
            "INSERT OR IGNORE INTO administrator (admin_id, username, password_hash) VALUES ('admin_01', 'admin', ?)",
        )
        .bind(&admin_hash)
        .execute(&self.pool)
        .await?;

        let developer_hash = sha256_hex("voice123");
        sqlx::query(
            "INSERT OR IGNORE INTO developer (developer_id, username, email, portal_id, status, password_hash) VALUES ('tenant_ugreen_01', 'ugreen_voice_dev', 'voice@ugreen.com', 'portal_default', 'ACTIVE', ?)",
        )
        .bind(&developer_hash)
        .execute(&self.pool)
        .await?;
        sqlx::query("UPDATE developer SET password_hash = ? WHERE developer_id = 'tenant_ugreen_01' AND (password_hash IS NULL OR password_hash = '')")
            .bind(&developer_hash)
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "INSERT OR IGNORE INTO consumer (consumer_id, name, description, portal_id, developer_id) VALUES ('cons_android_app', 'Android Voice Intercom App', 'SM_F9460 LiveKit Voice AI Client', 'portal_default', 'tenant_ugreen_01')",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "INSERT OR IGNORE INTO product (product_id, name, type, description, status) VALUES ('prod_voice_ai', 'LiveKit + DeepSeek Voice Agent', 'AGENT_API', 'Real-time WebRTC Voice Assistant with Faster-Whisper + DeepSeek-V4.1 + Piper/CosyVoice', 'PUBLISHED')",
        )
        .execute(&self.pool)
        .await?;

        let default_raw_key = "hm_ak_live_ugreen_2026_voice_key";
        let key_hash = sha256_hex(default_raw_key);
        sqlx::query(
            "INSERT OR IGNORE INTO voice_api_key_binding (api_key_id, api_key_sha256, tenant_id, portal_id, consumer_id, status, allowed_modes, rate_limit_rpm, quota_limit_usd_micros, quota_used_usd_micros)
             VALUES ('ak_live_ugreen_01', ?, 'tenant_ugreen_01', 'portal_default', 'cons_android_app', 'ACTIVE', 'realtime,cascade', 120, 100000000, 0)",
        )
        .bind(&key_hash)
        .execute(&self.pool)
        .await?;

        let config_count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM ai_model_config")
            .fetch_one(&self.pool)
            .await?
            .get("n");
        if config_count == 0 {
            self.insert_model_config("cfg_llm_default", "Ollama DeepSeek", "llm", "openai_chat", "https://ollama.com/v1", "cb4631cad50c4e3eb1342b9ef0075dd5.Ym7un785NAKQCVzDMK8DjK52", "", "deepseek-v4.1-flash:1017-cloud", "", true).await?;
            self.insert_model_config("cfg_stt_default", "本地 Faster-Whisper", "stt", "whisper_local", "", "", "", "faster-whisper-tiny", "/models/faster-whisper-tiny", true).await?;
            self.insert_model_config("cfg_tts_default", "本地 Piper 中文", "tts", "piper_local", "", "", "", "zh_CN-huayan-medium", "/models/piper/zh_CN-huayan-medium.onnx", true).await?;
        }

        Ok(())
    }

    pub async fn verify_business_key(&self, raw_api_key: &str) -> Result<Option<VerifiedBusinessKey>> {
        let key_hash = sha256_hex(raw_api_key.trim());
        {
            let cache = self.key_cache.read().await;
            if let Some(found) = cache.get(&key_hash) {
                return Ok(Some(found.clone()));
            }
        }

        let row = sqlx::query(
            "SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, rate_limit_rpm, quota_limit_usd_micros, quota_used_usd_micros
             FROM voice_api_key_binding WHERE api_key_sha256 = ? OR api_key_id = ?",
        )
        .bind(&key_hash)
        .bind(raw_api_key.trim())
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
        sqlx::query(
            "INSERT INTO voice_session_record (session_id, room_name, tenant_id, portal_id, consumer_id, api_key_id, voice_mode, llm_model, state)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')",
        )
        .bind(session_id)
        .bind(room_name)
        .bind(&key.tenant_id)
        .bind(&key.portal_id)
        .bind(&key.consumer_id)
        .bind(&key.api_key_id)
        .bind(&req.mode)
        .bind(&req.llm_model)
        .execute(&self.pool)
        .await?;

        let ident = SessionIdentity {
            session_id: session_id.to_string(),
            room_name: room_name.to_string(),
            tenant_id: key.tenant_id.clone(),
            portal_id: key.portal_id.clone(),
            consumer_id: key.consumer_id.clone(),
            api_key_id: key.api_key_id.clone(),
            voice_mode: req.mode.clone(),
            llm_model: req.llm_model.clone(),
        };
        let mut cache = self.session_cache.write().await;
        cache.insert(session_id.to_string(), ident.clone());
        cache.insert(room_name.to_string(), ident.clone());
        Ok(ident)
    }

    pub async fn resolve_session_identity(
        &self,
        session_or_room: &str,
    ) -> Result<Option<SessionIdentity>> {
        if session_or_room.is_empty() {
            return Ok(None);
        }
        {
            let cache = self.session_cache.read().await;
            if let Some(found) = cache.get(session_or_room) {
                return Ok(Some(found.clone()));
            }
        }
        let row = sqlx::query(
            "SELECT session_id, room_name, tenant_id, portal_id, consumer_id, api_key_id, voice_mode, llm_model
             FROM voice_session_record WHERE session_id = ? OR room_name = ? LIMIT 1",
        )
        .bind(session_or_room)
        .bind(session_or_room)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(r) = row {
            let ident = SessionIdentity {
                session_id: r.get("session_id"),
                room_name: r.get("room_name"),
                tenant_id: r.get("tenant_id"),
                portal_id: r.get("portal_id"),
                consumer_id: r.get("consumer_id"),
                api_key_id: r.get("api_key_id"),
                voice_mode: r.get("voice_mode"),
                llm_model: r.get("llm_model"),
            };
            let mut cache = self.session_cache.write().await;
            cache.insert(ident.session_id.clone(), ident.clone());
            cache.insert(ident.room_name.clone(), ident.clone());
            Ok(Some(ident))
        } else {
            Ok(None)
        }
    }

    pub async fn ingest_usage_event(&self, mut ev: UsageEventPayload) -> Result<bool> {
        // Resolve missing tenant_id / api_key_id from session_id or room_name
        if ev.tenant_id.is_empty() || ev.api_key_id.is_empty() {
            let lookup_key = if !ev.session_id.is_empty() {
                ev.session_id.as_str()
            } else {
                ev.room_name.as_deref().unwrap_or("")
            };
            if let Some(ident) = self.resolve_session_identity(lookup_key).await? {
                ev.session_id = ident.session_id;
                ev.room_name = Some(ident.room_name);
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

        let res = sqlx::query(
            "INSERT OR IGNORE INTO usage_event_ledger (
                event_id, idempotency_key, session_id, room_name, tenant_id, api_key_id,
                source, category, model, prompt_tokens, completion_tokens, cached_tokens,
                audio_input_tokens, audio_output_tokens, audio_duration_ms, tts_characters,
                rtc_duration_ms, rtc_bytes, cost_usd_micros, occurred_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            return Ok(false); // Deduplicated by idempotency_key
        }

        let dt = DateTime::<Utc>::from_timestamp_millis(ev.occurred_at_ms).unwrap_or_else(Utc::now);
        let stat_hour = dt.format("%Y-%m-%d %H:00:00").to_string();

        sqlx::query(
            "INSERT INTO usage_hourly_rollup (
                stat_hour, tenant_id, api_key_id, category, model, event_count,
                sum_prompt_tokens, sum_completion_tokens, sum_cached_tokens,
                sum_audio_input_tokens, sum_audio_output_tokens, sum_audio_duration_ms,
                sum_tts_characters, sum_rtc_duration_ms, sum_rtc_bytes, sum_cost_usd_micros
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stat_hour, tenant_id, api_key_id, category, model) DO UPDATE SET
                event_count = event_count + 1,
                sum_prompt_tokens = sum_prompt_tokens + excluded.sum_prompt_tokens,
                sum_completion_tokens = sum_completion_tokens + excluded.sum_completion_tokens,
                sum_cached_tokens = sum_cached_tokens + excluded.sum_cached_tokens,
                sum_audio_input_tokens = sum_audio_input_tokens + excluded.sum_audio_input_tokens,
                sum_audio_output_tokens = sum_audio_output_tokens + excluded.sum_audio_output_tokens,
                sum_audio_duration_ms = sum_audio_duration_ms + excluded.sum_audio_duration_ms,
                sum_tts_characters = sum_tts_characters + excluded.sum_tts_characters,
                sum_rtc_duration_ms = sum_rtc_duration_ms + excluded.sum_rtc_duration_ms,
                sum_rtc_bytes = sum_rtc_bytes + excluded.sum_rtc_bytes,
                sum_cost_usd_micros = sum_cost_usd_micros + excluded.sum_cost_usd_micros",
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

        sqlx::query(
            "UPDATE voice_api_key_binding SET quota_used_usd_micros = quota_used_usd_micros + ? WHERE api_key_id = ?",
        )
        .bind(cost_micros)
        .bind(&ev.api_key_id)
        .execute(&self.pool)
        .await?;

        // Invalidate cached key so updated quota is reflected
        self.key_cache.write().await.clear();

        Ok(true)
    }

    pub async fn query_usage_summary(
        &self,
        tenant_id: &str,
        api_key_id: Option<&str>,
    ) -> Result<UsageSummaryResponse> {
        let rows = if let Some(ak) = api_key_id {
            sqlx::query(
                "SELECT category,
                        SUM(event_count) AS cnt,
                        SUM(sum_prompt_tokens) AS p_tok,
                        SUM(sum_completion_tokens) AS c_tok,
                        SUM(sum_cached_tokens) AS cache_tok,
                        SUM(sum_audio_input_tokens) AS a_in,
                        SUM(sum_audio_output_tokens) AS a_out,
                        SUM(sum_audio_duration_ms) AS a_dur,
                        SUM(sum_tts_characters) AS tts_ch,
                        SUM(sum_rtc_duration_ms) AS rtc_dur,
                        SUM(sum_rtc_bytes) AS rtc_b,
                        SUM(sum_cost_usd_micros) AS cost
                 FROM usage_hourly_rollup
                 WHERE tenant_id = ? AND api_key_id = ?
                 GROUP BY category",
            )
            .bind(tenant_id)
            .bind(ak)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT category,
                        SUM(event_count) AS cnt,
                        SUM(sum_prompt_tokens) AS p_tok,
                        SUM(sum_completion_tokens) AS c_tok,
                        SUM(sum_cached_tokens) AS cache_tok,
                        SUM(sum_audio_input_tokens) AS a_in,
                        SUM(sum_audio_output_tokens) AS a_out,
                        SUM(sum_audio_duration_ms) AS a_dur,
                        SUM(sum_tts_characters) AS tts_ch,
                        SUM(sum_rtc_duration_ms) AS rtc_dur,
                        SUM(sum_rtc_bytes) AS rtc_b,
                        SUM(sum_cost_usd_micros) AS cost
                 FROM usage_hourly_rollup
                 WHERE tenant_id = ?
                 GROUP BY category",
            )
            .bind(tenant_id)
            .fetch_all(&self.pool)
            .await?
        };

        let mut rtc = RtcUsageBreakdown::default();
        let mut voice = VoiceModelUsageBreakdown::default();
        let mut text_llm = TextLlmUsageBreakdown::default();
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
        let rows = sqlx::query("SELECT product_id, name, type, description, status FROM product ORDER BY id")
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
            sqlx::query("SELECT consumer_id, name, portal_id, developer_id FROM consumer WHERE developer_id = ? ORDER BY id")
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

    pub async fn login(&self, role: &str, username: &str, password: &str) -> Result<Option<AuthSession>> {
        let hash = sha256_hex(password);
        let found = if role == "admin" {
            sqlx::query("SELECT admin_id, username FROM administrator WHERE username = ? AND password_hash = ?")
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
            sqlx::query("SELECT developer_id, username, portal_id FROM developer WHERE username = ? AND password_hash = ? AND status = 'ACTIVE'")
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
            self.auth_sessions.write().await.insert(session.token.clone(), session.clone());
        }
        Ok(found)
    }

    pub async fn auth_session(&self, token: &str) -> Option<AuthSession> {
        self.auth_sessions.read().await.get(token).cloned()
    }

    pub async fn create_portal(&self, name: &str, description: Option<&str>) -> Result<PortalDto> {
        let portal_id = format!("portal_{}", uuid::Uuid::now_v7().simple());
        sqlx::query("INSERT INTO portal (portal_id, name, description, admin_id) VALUES (?, ?, ?, 'admin_01')")
            .bind(&portal_id)
            .bind(name)
            .bind(description)
            .execute(&self.pool)
            .await?;
        Ok(PortalDto { portal_id, name: name.into(), description: description.map(|s| s.into()) })
    }

    pub async fn create_product(&self, name: &str, product_type: &str, description: Option<&str>) -> Result<ProductDto> {
        let product_id = format!("prod_{}", uuid::Uuid::now_v7().simple());
        sqlx::query("INSERT INTO product (product_id, name, type, description, status) VALUES (?, ?, ?, ?, 'PUBLISHED')")
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

    pub async fn create_consumer(&self, name: &str, portal_id: &str, developer_id: &str, description: Option<&str>) -> Result<ConsumerDto> {
        let consumer_id = format!("cons_{}", uuid::Uuid::now_v7().simple());
        sqlx::query("INSERT INTO consumer (consumer_id, name, description, portal_id, developer_id) VALUES (?, ?, ?, ?, ?)")
            .bind(&consumer_id)
            .bind(name)
            .bind(description)
            .bind(portal_id)
            .bind(developer_id)
            .execute(&self.pool)
            .await?;
        Ok(ConsumerDto { consumer_id, name: name.into(), portal_id: portal_id.into(), developer_id: developer_id.into() })
    }

    pub async fn create_api_key(&self, consumer_id: &str) -> Result<ApiKeyCreated> {
        let consumer = sqlx::query("SELECT portal_id, developer_id FROM consumer WHERE consumer_id = ?")
            .bind(consumer_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("consumer not found"))?;
        let raw_key = format!("hm_ak_{}", uuid::Uuid::now_v7().simple());
        let api_key_id = format!("ak_{}", &raw_key[6..18]);
        sqlx::query(
            "INSERT INTO voice_api_key_binding (api_key_id, api_key_sha256, tenant_id, portal_id, consumer_id, status, allowed_modes, rate_limit_rpm, quota_limit_usd_micros, quota_used_usd_micros)
             VALUES (?, ?, ?, ?, ?, 'ACTIVE', 'realtime,cascade', 120, 100000000, 0)",
        )
        .bind(&api_key_id)
        .bind(sha256_hex(&raw_key))
        .bind(consumer.get::<String, _>("developer_id"))
        .bind(consumer.get::<String, _>("portal_id"))
        .bind(consumer_id)
        .execute(&self.pool)
        .await?;
        self.key_cache.write().await.clear();
        Ok(ApiKeyCreated { api_key_id, raw_key })
    }

    pub async fn list_api_keys(&self, tenant_id: Option<&str>) -> Result<Vec<ApiKeyDto>> {
        let rows = if let Some(tid) = tenant_id {
            sqlx::query("SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, quota_used_usd_micros FROM voice_api_key_binding WHERE tenant_id = ? ORDER BY id DESC")
                .bind(tid).fetch_all(&self.pool).await?
        } else {
            sqlx::query("SELECT api_key_id, tenant_id, portal_id, consumer_id, status, allowed_modes, quota_used_usd_micros FROM voice_api_key_binding ORDER BY id DESC")
                .fetch_all(&self.pool).await?
        };
        Ok(rows.into_iter().map(|r| ApiKeyDto {
            api_key_id: r.get("api_key_id"),
            tenant_id: r.get("tenant_id"),
            portal_id: r.get("portal_id"),
            consumer_id: r.get("consumer_id"),
            status: r.get("status"),
            allowed_modes: r.get("allowed_modes"),
            quota_used_usd_micros: r.get("quota_used_usd_micros"),
        }).collect())
    }

    pub async fn list_voice_sessions(&self, tenant_id: Option<&str>) -> Result<Vec<VoiceSessionDto>> {
        let rows = if let Some(tid) = tenant_id {
            sqlx::query("SELECT session_id, room_name, tenant_id, api_key_id, voice_mode, llm_model, state, created_at FROM voice_session_record WHERE tenant_id = ? ORDER BY id DESC LIMIT 100")
                .bind(tid).fetch_all(&self.pool).await?
        } else {
            sqlx::query("SELECT session_id, room_name, tenant_id, api_key_id, voice_mode, llm_model, state, created_at FROM voice_session_record ORDER BY id DESC LIMIT 100")
                .fetch_all(&self.pool).await?
        };
        Ok(rows.into_iter().map(|r| VoiceSessionDto {
            session_id: r.get("session_id"),
            room_name: r.get("room_name"),
            tenant_id: r.get("tenant_id"),
            api_key_id: r.get("api_key_id"),
            voice_mode: r.get("voice_mode"),
            llm_model: r.get("llm_model"),
            state: r.get("state"),
            created_at: r.get("created_at"),
        }).collect())
    }

    async fn insert_model_config(&self, config_id: &str, name: &str, kind: &str, protocol: &str, base_url: &str, api_key: &str, api_secret: &str, model: &str, voice: &str, is_default: bool) -> Result<()> {
        if is_default {
            sqlx::query("UPDATE ai_model_config SET is_default = 0 WHERE kind = ?").bind(kind).execute(&self.pool).await?;
        }
        sqlx::query("INSERT INTO ai_model_config (config_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status) VALUES (?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?, 'ACTIVE')")
            .bind(config_id).bind(name).bind(kind).bind(protocol).bind(base_url).bind(api_key).bind(api_secret).bind(model).bind(voice).bind(if is_default { 1 } else { 0 })
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn create_model_config(&self, name: &str, kind: &str, protocol: &str, base_url: &str, api_key: &str, api_secret: &str, model: &str, voice: &str, is_default: bool) -> Result<ModelConfig> {
        let config_id = format!("cfg_{}", uuid::Uuid::now_v7().simple());
        self.insert_model_config(&config_id, name, kind, protocol, base_url, api_key, api_secret, model, voice, is_default).await?;
        self.get_model_config(&config_id).await?.ok_or_else(|| anyhow::anyhow!("config missing"))
    }

    pub async fn update_model_credentials(&self, config_id: &str, api_key: Option<&str>, api_secret: Option<&str>, base_url: Option<&str>) -> Result<ModelConfig> {
        if let Some(value) = api_key.filter(|v| !v.is_empty()) {
            sqlx::query("UPDATE ai_model_config SET api_key = ? WHERE config_id = ?").bind(value).bind(config_id).execute(&self.pool).await?;
        }
        if let Some(value) = api_secret.filter(|v| !v.is_empty()) {
            sqlx::query("UPDATE ai_model_config SET api_secret = ? WHERE config_id = ?").bind(value).bind(config_id).execute(&self.pool).await?;
        }
        if let Some(value) = base_url.filter(|v| !v.is_empty()) {
            sqlx::query("UPDATE ai_model_config SET base_url = ? WHERE config_id = ?").bind(value).bind(config_id).execute(&self.pool).await?;
        }
        self.get_model_config(config_id).await?.ok_or_else(|| anyhow::anyhow!("config not found"))
    }

    pub async fn list_model_configs(&self, kind: Option<&str>) -> Result<Vec<ModelConfig>> {
        let rows = if let Some(kind) = kind {
            sqlx::query("SELECT config_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config WHERE kind = ? ORDER BY is_default DESC, name").bind(kind).fetch_all(&self.pool).await?
        } else {
            sqlx::query("SELECT config_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config ORDER BY kind, is_default DESC, name").fetch_all(&self.pool).await?
        };
        Ok(rows.into_iter().map(model_config_from_row).collect())
    }

    pub async fn get_model_config(&self, config_id: &str) -> Result<Option<ModelConfig>> {
        Ok(sqlx::query("SELECT config_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config WHERE config_id = ?")
            .bind(config_id).fetch_optional(&self.pool).await?.map(model_config_from_row))
    }

    pub async fn set_default_model_config(&self, config_id: &str) -> Result<ModelConfig> {
        let current = self.get_model_config(config_id).await?.ok_or_else(|| anyhow::anyhow!("config not found"))?;
        sqlx::query("UPDATE ai_model_config SET is_default = 0 WHERE kind = ?").bind(&current.kind).execute(&self.pool).await?;
        sqlx::query("UPDATE ai_model_config SET is_default = 1, status = 'ACTIVE' WHERE config_id = ?").bind(config_id).execute(&self.pool).await?;
        Ok(self.get_model_config(config_id).await?.unwrap())
    }

    pub async fn disable_model_config(&self, config_id: &str) -> Result<()> {
        sqlx::query("UPDATE ai_model_config SET status = 'DISABLED', is_default = 0 WHERE config_id = ?").bind(config_id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn resolve_model_config(&self, kind: &str, requested: Option<&str>) -> Result<Option<ModelConfig>> {
        if let Some(name) = requested.map(str::trim).filter(|v| !v.is_empty()) {
            if let Some(found) = sqlx::query("SELECT config_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config WHERE status = 'ACTIVE' AND kind = ? AND (config_id = ? OR model = ? OR name = ?) ORDER BY is_default DESC LIMIT 1")
                .bind(kind).bind(name).bind(name).bind(name).fetch_optional(&self.pool).await? {
                return Ok(Some(model_config_from_row(found)));
            }
        }
        Ok(sqlx::query("SELECT config_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status FROM ai_model_config WHERE status = 'ACTIVE' AND kind = ? AND is_default = 1 LIMIT 1")
            .bind(kind).fetch_optional(&self.pool).await?.map(model_config_from_row))
    }

    async fn seed_livekit_catalog(&self) -> Result<()> {
        let catalog: Vec<serde_json::Value> = serde_json::from_str(include_str!("../../../catalog/livekit_models.json"))?;
        for item in catalog {
            sqlx::query(
                "INSERT OR IGNORE INTO ai_model_config (config_id, name, kind, protocol, base_url, api_key, api_secret, extra_json, model, voice, is_default, status)
                 VALUES (?, ?, ?, ?, ?, '', '', '{}', ?, ?, 0, 'ACTIVE')",
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
        self.ensure_model_default("llm", "openai_chat", "deepseek-v4.1-flash:1017-cloud").await?;
        self.ensure_model_default("stt", "whisper_local", "faster-whisper-tiny").await?;
        self.ensure_model_default("tts", "piper_local", "zh_CN-huayan-medium").await?;
        sqlx::query("UPDATE ai_model_config SET api_key = ? WHERE protocol = 'openai_chat' AND model = 'deepseek-v4.1-flash:1017-cloud' AND api_key = ''")
            .bind("cb4631cad50c4e3eb1342b9ef0075dd5.Ym7un785NAKQCVzDMK8DjK52")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn ensure_model_default(&self, kind: &str, protocol: &str, model: &str) -> Result<()> {
        let existing: i64 = sqlx::query("SELECT COUNT(*) AS n FROM ai_model_config WHERE kind = ? AND is_default = 1")
            .bind(kind)
            .fetch_one(&self.pool)
            .await?
            .get("n");
        if existing == 0 {
            sqlx::query("UPDATE ai_model_config SET is_default = 1 WHERE config_id = (SELECT config_id FROM ai_model_config WHERE kind = ? AND protocol = ? AND model = ? LIMIT 1)")
                .bind(kind)
                .bind(protocol)
                .bind(model)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }
}

fn model_config_from_row(row: sqlx::sqlite::SqliteRow) -> ModelConfig {
    ModelConfig {
        config_id: row.get("config_id"),
        name: row.get("name"),
        kind: row.get("kind"),
        protocol: row.get("protocol"),
        base_url: row.get("base_url"),
        api_key: row.get("api_key"),
        api_secret: row.get("api_secret"),
        extra_json: row.get("extra_json"),
        model: row.get("model"),
        voice: row.get("voice"),
        is_default: row.get::<i64, _>("is_default") == 1,
        status: row.get("status"),
    }
}
