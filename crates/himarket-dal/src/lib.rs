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

        // Seed default Portal, Developer (Tenant), Consumer, Product, and Business API Key for immediate Android & E2E testing
        sqlx::query(
            "INSERT OR IGNORE INTO portal (portal_id, name, description, admin_id) VALUES ('portal_default', 'HiMarket AI Voice Portal', 'Enterprise AI Voice & LLM Gateway Portal', 'admin_01')",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "INSERT OR IGNORE INTO developer (developer_id, username, email, portal_id, status) VALUES ('tenant_ugreen_01', 'ugreen_voice_dev', 'voice@ugreen.com', 'portal_default', 'ACTIVE')",
        )
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
}
