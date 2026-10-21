use axum::{
    Json, Router,
    http::{HeaderValue, header},
    response::Html,
    routing::get,
};
use himarket_access::{AccessState, access_router};
use himarket_core::ApiResponse;
use himarket_dal::DalRepo;
use himarket_market::market_router;
use himarket_metering::{MeteringState, metering_router};
use serde_json::Value;
use std::{env, net::SocketAddr, time::Duration};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

pub fn build_app(
    repo: DalRepo,
    livekit_api_key: String,
    livekit_api_secret: String,
    livekit_public_ws_url: String,
    livekit_internal_http_url: String,
    internal_secret: String,
) -> Router {
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("http client");

    let access_state = AccessState {
        repo: repo.clone(),
        livekit_api_key,
        livekit_api_secret,
        livekit_public_ws_url,
        livekit_internal_http_url,
        http_client,
    };

    let metering_state = MeteringState {
        repo: repo.clone(),
        internal_secret,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/healthz", get(healthz))
        .route("/", get(|| async { Html(include_str!("../../../web/index.html")) }))
        .route("/admin", get(|| async { Html(include_str!("../../../web/admin.html")) }))
        .route("/portal", get(|| async { Html(include_str!("../../../web/portal.html")) }))
        .route("/console.js", get(console_js))
        .route("/console.css", get(console_css))
        .merge(market_router(repo))
        .merge(access_router(access_state))
        .merge(metering_router(metering_state))
        .layer(cors)
}

async fn console_js() -> ([(header::HeaderName, HeaderValue); 1], &'static str) {
    ([(header::CONTENT_TYPE, HeaderValue::from_static("text/javascript"))], include_str!("../../../web/console.js"))
}

async fn console_css() -> ([(header::HeaderName, HeaderValue); 1], &'static str) {
    ([(header::CONTENT_TYPE, HeaderValue::from_static("text/css"))], include_str!("../../../web/console.css"))
}

async fn healthz() -> Json<ApiResponse<Value>> {
    ApiResponse::ok(serde_json::json!({
        "service": "himarket-rs",
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,himarket=debug".into()),
        )
        .init();

    let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let livekit_api_key = env::var("LIVEKIT_API_KEY").unwrap_or_else(|_| "devkey".to_string());
    let livekit_api_secret = env::var("LIVEKIT_API_SECRET").unwrap_or_else(|_| "secret".to_string());
    let livekit_public_ws_url =
        env::var("LIVEKIT_PUBLIC_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:7880".to_string());
    let livekit_internal_http_url = env::var("LIVEKIT_INTERNAL_HTTP_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:7880".to_string());
    let internal_secret = env::var("HIMARKET_INTERNAL_SECRET").unwrap_or_default();

    let repo = DalRepo::connect_and_init(&db_url).await?;
    let app = build_app(
        repo,
        livekit_api_key,
        livekit_api_secret,
        livekit_public_ws_url,
        livekit_internal_http_url,
        internal_secret,
    );

    let addr: SocketAddr = listen_addr.parse()?;
    info!("himarket-rs listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use himarket_core::{CreateVoiceSessionRequest, UsageEventPayload, UsageMetricsPayload};

    #[tokio::test]
    async fn test_end_to_end_session_and_three_source_metering() {
        let repo = DalRepo::connect_and_init("sqlite::memory:").await.unwrap();
        let key = repo
            .verify_business_key("hm_ak_live_ugreen_2026_voice_key")
            .await
            .unwrap()
            .expect("seeded key");
        assert_eq!(key.tenant_id, "tenant_ugreen_01");
        assert!(key.has_quota());

        let req = CreateVoiceSessionRequest {
            mode: "cascade".to_string(),
            llm_model: "deepseek-v4.1-flash:1017-cloud".to_string(),
            stt_model: Some("faster-whisper-small".to_string()),
            tts_voice: Some("piper-zh_CN-huayan-medium".to_string()),
            system_prompt: None,
        };
        let ident = repo
            .register_voice_session("sess_test_1", "lk_rm_sess_test_1", &key, &req)
            .await
            .unwrap();
        assert_eq!(ident.api_key_id, "ak_live_ugreen_01");

        // 1. Ingest LiveKit SFU RTC usage (only room_name supplied, auto-resolves tenant & key)
        let rtc_ev = UsageEventPayload {
            event_id: "ev_rtc_1".into(),
            idempotency_key: "lk_rtc:RM_1:PA_1:1".into(),
            session_id: String::new(),
            room_name: Some("lk_rm_sess_test_1".into()),
            tenant_id: String::new(),
            api_key_id: String::new(),
            source: "livekit_sfu".into(),
            category: "rtc".into(),
            model: "webrtc-audio".into(),
            occurred_at_ms: 1761000000000,
            metrics: UsageMetricsPayload {
                rtc_duration_ms: Some(12_000),
                rtc_bytes: Some(192_000),
                ..Default::default()
            },
        };
        assert!(repo.ingest_usage_event(rtc_ev.clone()).await.unwrap());
        // Duplicate idempotency_key must be deduplicated
        assert!(!repo.ingest_usage_event(rtc_ev).await.unwrap());

        // 2. Ingest Agent STT + TTS usage
        let stt_ev = UsageEventPayload {
            event_id: "ev_stt_1".into(),
            idempotency_key: "agent_stt:sess_test_1:turn1".into(),
            session_id: "sess_test_1".into(),
            room_name: Some("lk_rm_sess_test_1".into()),
            tenant_id: "tenant_ugreen_01".into(),
            api_key_id: "ak_live_ugreen_01".into(),
            source: "agent_worker".into(),
            category: "stt".into(),
            model: "faster-whisper".into(),
            occurred_at_ms: 1761000001000,
            metrics: UsageMetricsPayload {
                audio_duration_ms: Some(4_200),
                ..Default::default()
            },
        };
        assert!(repo.ingest_usage_event(stt_ev).await.unwrap());

        let tts_ev = UsageEventPayload {
            event_id: "ev_tts_1".into(),
            idempotency_key: "agent_tts:sess_test_1:turn1".into(),
            session_id: "sess_test_1".into(),
            room_name: Some("lk_rm_sess_test_1".into()),
            tenant_id: "tenant_ugreen_01".into(),
            api_key_id: "ak_live_ugreen_01".into(),
            source: "agent_worker".into(),
            category: "tts".into(),
            model: "piper-zh".into(),
            occurred_at_ms: 1761000002000,
            metrics: UsageMetricsPayload {
                tts_characters: Some(45),
                audio_duration_ms: Some(5_800),
                ..Default::default()
            },
        };
        assert!(repo.ingest_usage_event(tts_ev).await.unwrap());

        // 3. Ingest CLIProxyAPI Text LLM usage
        let llm_ev = UsageEventPayload {
            event_id: "ev_cpa_1".into(),
            idempotency_key: "cli_proxy:req_101".into(),
            session_id: "sess_test_1".into(),
            room_name: None,
            tenant_id: "tenant_ugreen_01".into(),
            api_key_id: "ak_live_ugreen_01".into(),
            source: "cli_proxy_api".into(),
            category: "text_llm".into(),
            model: "deepseek-v4.1-flash:1017-cloud".into(),
            occurred_at_ms: 1761000001500,
            metrics: UsageMetricsPayload {
                prompt_tokens: Some(120),
                completion_tokens: Some(68),
                cached_tokens: Some(32),
                ..Default::default()
            },
        };
        assert!(repo.ingest_usage_event(llm_ev).await.unwrap());

        // 4. Query aggregated summary by tenant_id & api_key_id
        let summary = repo
            .query_usage_summary("tenant_ugreen_01", Some("ak_live_ugreen_01"))
            .await
            .unwrap();
        assert_eq!(summary.total_events, 4);
        assert_eq!(summary.rtc.rtc_bytes, 192_000);
        assert_eq!(summary.voice_models.stt_audio_duration_ms, 4_200);
        assert_eq!(summary.voice_models.tts_characters, 45);
        assert_eq!(summary.text_llm.prompt_tokens, 120);
        assert_eq!(summary.text_llm.completion_tokens, 68);
    }
}
