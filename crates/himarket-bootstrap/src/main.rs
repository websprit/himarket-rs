use axum::{
    Json, Router,
    http::{HeaderValue, header},
    response::Html,
    routing::get,
};
use himarket_access::{
    AccessState, ChatClient, HttpChatClient, HttpSynthesizeClient, SynthesizeClient, access_router,
};
use himarket_core::ApiResponse;
use himarket_dal::DalRepo;
use himarket_market::market_router;
use himarket_metering::{MeteringState, metering_router};
use serde_json::Value;
use std::{env, net::SocketAddr, sync::Arc, time::Duration};
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
    let tts_url = env::var("HIMARKET_TTS_URL").unwrap_or_default();
    build_app_with_clients(
        repo,
        livekit_api_key,
        livekit_api_secret,
        livekit_public_ws_url,
        livekit_internal_http_url,
        internal_secret,
        Arc::new(HttpChatClient::new()),
        Arc::new(HttpSynthesizeClient::new(tts_url)),
        true,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_app_with_clients(
    repo: DalRepo,
    livekit_api_key: String,
    livekit_api_secret: String,
    livekit_public_ws_url: String,
    livekit_internal_http_url: String,
    internal_secret: String,
    chat_client: Arc<dyn ChatClient>,
    synth_client: Arc<dyn SynthesizeClient>,
    strip_jwt_secrets: bool,
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
        internal_secret: internal_secret.clone(),
        http_client,
        chat_client,
        synth_client,
        strip_jwt_secrets,
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
        .route(
            "/",
            get(|| async { Html(include_str!("../../../web/index.html")) }),
        )
        .route(
            "/admin",
            get(|| async { Html(include_str!("../../../web/admin.html")) }),
        )
        .route(
            "/portal",
            get(|| async { Html(include_str!("../../../web/portal.html")) }),
        )
        .route("/console.js", get(console_js))
        .route("/console.css", get(console_css))
        .merge(market_router(repo))
        .merge(access_router(access_state))
        .merge(metering_router(metering_state))
        .layer(cors)
}

async fn console_js() -> ([(header::HeaderName, HeaderValue); 1], &'static str) {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/javascript"),
        )],
        include_str!("../../../web/console.js"),
    )
}

async fn console_css() -> ([(header::HeaderName, HeaderValue); 1], &'static str) {
    (
        [(header::CONTENT_TYPE, HeaderValue::from_static("text/css"))],
        include_str!("../../../web/console.css"),
    )
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

    let db_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must be a postgres connection string");
    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let livekit_api_key = env::var("LIVEKIT_API_KEY").unwrap_or_else(|_| "devkey".to_string());
    let livekit_api_secret =
        env::var("LIVEKIT_API_SECRET").unwrap_or_else(|_| "secret".to_string());
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
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use himarket_access::{ChatCompletionOutput, ModelBinding};
    use himarket_core::{AppError, CreateVoiceSessionRequest, UsageEventPayload, UsageMetricsPayload};
    use himarket_dal::format_summary_materials;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    async fn fresh_repo() -> DalRepo {
        let url = std::env::var("DATABASE_URL")
            .expect("set DATABASE_URL to an external PostgreSQL database before running tests");
        DalRepo::connect_and_reset(&url).await.unwrap()
    }

    struct FakeChatClient {
        calls: AtomicUsize,
        delay_ms: u64,
        fail_empty: bool,
    }

    impl FakeChatClient {
        fn new(delay_ms: u64, fail_empty: bool) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                delay_ms,
                fail_empty,
            }
        }
    }

    impl ChatClient for FakeChatClient {
        fn complete<'a>(
            &'a self,
            _binding: &'a ModelBinding,
            system_prompt: &'a str,
            user_content: &'a str,
            _livekit_api_key: &'a str,
            _livekit_api_secret: &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ChatCompletionOutput, AppError>> + Send + 'a>,
        > {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
                }
                if self.fail_empty {
                    return Err(AppError::bad_gateway_code("llm_empty", "empty output"));
                }
                let content = if system_prompt.contains("只输出译文") {
                    format!("TRANSLATED({})", user_content)
                } else if system_prompt.contains("精简要点") {
                    format!("# 精简版总结标题\n\n## 核心摘要\n精简摘要: {}", user_content)
                } else {
                    format!(
                        "# 演讲探索与核心观点\n\n## 核心摘要\n总结内容: {}\n\n## 核心要点\n1. 要点一",
                        user_content
                    )
                };
                Ok(ChatCompletionOutput {
                    content,
                    prompt_tokens: Some(40),
                    completion_tokens: Some(25),
                    cached_tokens: Some(5),
                })
            })
        }
    }

    struct FakeSynthClient {
        should_fail: bool,
    }

    impl SynthesizeClient for FakeSynthClient {
        fn synthesize<'a>(
            &'a self,
            _binding: &'a ModelBinding,
            _text: &'a str,
            _internal_secret: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, AppError>> + Send + 'a>>
        {
            Box::pin(async move {
                if self.should_fail {
                    Err(AppError::bad_gateway_code("tts_failed", "piper failed"))
                } else {
                    Ok(b"RIFF\x24\x00\x00\x00WAVEfmt ".to_vec())
                }
            })
        }
    }

    #[tokio::test]
    async fn test_end_to_end_session_and_three_source_metering() {
        let repo = fresh_repo().await;
        let key = repo
            .verify_business_key("hm_ak_live_ugreen_2026_voice_key")
            .await
            .unwrap()
            .expect("seeded key");
        assert_eq!(key.tenant_id, "tenant_ugreen_01");
        assert!(key.has_quota());

        let req = CreateVoiceSessionRequest {
            mode: "cascade".to_string(),
            task: "cascade".to_string(),
            llm_model: Some("deepseek-v4.1-flash:1017-cloud".to_string()),
            stt_model: Some("faster-whisper-small".to_string()),
            tts_voice: Some("piper-zh_CN-huayan-medium".to_string()),
            system_prompt: None,
            source_lang: String::new(),
            target_lang: String::new(),
            speak: true,
        };
        let ident = repo
            .register_voice_session("sess_test_1", "lk_rm_sess_test_1", &key, &req)
            .await
            .unwrap();
        assert_eq!(ident.api_key_id, "ak_live_ugreen_01");

        // 1. Ingest LiveKit SFU RTC usage
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
        assert!(!repo.ingest_usage_event(rtc_ev).await.unwrap());

        // 2. Ingest Agent STT + TTS + translate + summary usage
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

        let tr_ev = UsageEventPayload {
            event_id: "ev_tr_1".into(),
            idempotency_key: "agent_translate:sess_test_1:turn1".into(),
            session_id: "sess_test_1".into(),
            room_name: None,
            tenant_id: "tenant_ugreen_01".into(),
            api_key_id: "ak_live_ugreen_01".into(),
            source: "agent_worker".into(),
            category: "translate".into(),
            model: "deepseek-v4.1-flash:1017-cloud".into(),
            occurred_at_ms: 1761000002500,
            metrics: UsageMetricsPayload {
                prompt_tokens: Some(50),
                completion_tokens: Some(30),
                cached_tokens: Some(0),
                ..Default::default()
            },
        };
        assert!(repo.ingest_usage_event(tr_ev.clone()).await.unwrap());
        let quota_after_tr = repo
            .verify_business_key("hm_ak_live_ugreen_2026_voice_key")
            .await
            .unwrap()
            .unwrap()
            .quota_used_usd_micros;
        assert!(!repo.ingest_usage_event(tr_ev).await.unwrap());
        let quota_after_dup = repo
            .verify_business_key("hm_ak_live_ugreen_2026_voice_key")
            .await
            .unwrap()
            .unwrap()
            .quota_used_usd_micros;
        assert_eq!(quota_after_tr, quota_after_dup);

        let sum_ev = UsageEventPayload {
            event_id: "ev_sum_1".into(),
            idempotency_key: "api_summary:sess_test_1:r1".into(),
            session_id: "sess_test_1".into(),
            room_name: None,
            tenant_id: "tenant_ugreen_01".into(),
            api_key_id: "ak_live_ugreen_01".into(),
            source: "himarket_access".into(),
            category: "summary".into(),
            model: "deepseek-v4.1-flash:1017-cloud".into(),
            occurred_at_ms: 1761000003000,
            metrics: UsageMetricsPayload {
                prompt_tokens: Some(200),
                completion_tokens: Some(90),
                cached_tokens: Some(10),
                ..Default::default()
            },
        };
        assert!(repo.ingest_usage_event(sum_ev).await.unwrap());

        let summary = repo
            .query_usage_summary("tenant_ugreen_01", Some("ak_live_ugreen_01"))
            .await
            .unwrap();
        assert_eq!(summary.total_events, 6);
        assert_eq!(summary.rtc.rtc_bytes, 192_000);
        assert_eq!(summary.voice_models.stt_audio_duration_ms, 4_200);
        assert_eq!(summary.voice_models.tts_characters, 45);
        assert_eq!(summary.text_llm.prompt_tokens, 120);
        assert_eq!(summary.translate.prompt_tokens, 50);
        assert_eq!(summary.translate.completion_tokens, 30);
        assert_eq!(summary.summary.prompt_tokens, 200);
        assert_eq!(summary.summary.completion_tokens, 90);
    }

    #[tokio::test]
    async fn model_configs_are_isolated_by_tenant() {
        let repo = fresh_repo().await;
        repo.create_model_config(
            "tenant_a",
            "A",
            "llm",
            "openai_chat",
            "https://a.example",
            "",
            "",
            "model-a",
            "",
            true,
        )
        .await
        .unwrap();
        repo.create_model_config(
            "tenant_b",
            "B",
            "llm",
            "openai_chat",
            "https://b.example",
            "",
            "",
            "model-b",
            "",
            true,
        )
        .await
        .unwrap();
        let a = repo
            .resolve_model_config("tenant_a", "llm", None)
            .await
            .unwrap()
            .unwrap();
        let b = repo
            .resolve_model_config("tenant_b", "llm", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.model, "model-a");
        assert_eq!(b.model, "model-b");
        let fallback = repo
            .resolve_model_config("tenant_a", "llm", Some("model-b"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fallback.tenant_id, "tenant_a");
        assert_eq!(fallback.model, "model-a");
    }

    #[tokio::test]
    async fn test_voice_tasks_selectors_summary_and_endpoints() {
        let repo = fresh_repo().await;

        // 1. TTS selector: catalog has active cartesia / azure_tts with empty keys; ja must fall back to Piper, never cartesia
        let ja_tts = repo
            .select_tts_for_lang("tenant_ugreen_01", "ja")
            .await
            .unwrap();
        assert!(ja_tts.voice_fallback);
        assert_eq!(ja_tts.config.protocol, "piper_local");

        // Another tenant's ja voice must not leak
        let other_ja = repo
            .create_model_config(
                "tenant_other",
                "Other JA Piper",
                "tts",
                "piper_local",
                "",
                "",
                "",
                "ja_JP-other",
                "/models/piper/ja_JP-other.onnx",
                true,
            )
            .await
            .unwrap();
        repo.update_model_extra_langs(&other_ja.config_id, "tts_languages", &["ja".into()])
            .await
            .unwrap();
        let still_fallback = repo
            .select_tts_for_lang("tenant_ugreen_01", "ja")
            .await
            .unwrap();
        assert!(still_fallback.voice_fallback);

        // Tenant's own ja Piper is selected with voice_fallback=false
        let own_ja = repo
            .create_model_config(
                "tenant_ugreen_01",
                "Ugreen JA Piper",
                "tts",
                "piper_local",
                "",
                "",
                "",
                "ja_JP-kokoro",
                "/models/piper/ja_JP-kokoro.onnx",
                false,
            )
            .await
            .unwrap();
        repo.update_model_extra_langs(&own_ja.config_id, "tts_languages", &["ja".into()])
            .await
            .unwrap();
        let matched_ja = repo
            .select_tts_for_lang("tenant_ugreen_01", "ja")
            .await
            .unwrap();
        assert!(!matched_ja.voice_fallback);
        assert_eq!(matched_ja.config.config_id, own_ja.config_id);

        // 2. STT selector: requesting "de" or explicit empty-key chirp_3 / dashscope_stt falls back to runnable Whisper
        let de_stt = repo
            .select_stt_for_lang("tenant_ugreen_01", Some("de"), None)
            .await
            .unwrap();
        assert!(de_stt.stt_fallback);
        assert_eq!(de_stt.config.protocol, "whisper_local");

        let chirp_fallback = repo
            .select_stt_for_lang("tenant_ugreen_01", Some("en"), Some("chirp_3"))
            .await
            .unwrap();
        assert!(chirp_fallback.stt_fallback);
        assert_eq!(chirp_fallback.config.protocol, "whisper_local");

        let dash_fallback = repo
            .select_stt_for_lang(
                "tenant_ugreen_01",
                Some("zh"),
                Some("vendor-stt-dashscope_stt-qwen3-asr-flash-realtime"),
            )
            .await
            .unwrap();
        assert!(dash_fallback.stt_fallback);
        assert_eq!(dash_fallback.config.protocol, "whisper_local");

        // 3. Tenant with ONLY STT (no LLM): transcribe session succeeds, cascade fails
        repo.upsert_language_policy("tenant_stt_only", "zh", &["zh".into(), "en".into()])
            .await
            .unwrap();
        repo.create_model_config(
            "tenant_stt_only",
            "STT Only Whisper",
            "stt",
            "whisper_local",
            "",
            "",
            "",
            "faster-whisper-tiny",
            "/models/faster-whisper-tiny",
            true,
        )
        .await
        .unwrap();
        let cons = repo
            .create_consumer("STT App", "portal_default", "tenant_stt_only", None)
            .await
            .unwrap();
        let stt_only_key = repo.create_api_key(&cons.consumer_id).await.unwrap();

        let chat_fake = Arc::new(FakeChatClient::new(80, false));
        let synth_fake = Arc::new(FakeSynthClient { should_fail: true });
        let app = build_app_with_clients(
            repo.clone(),
            "devkey".into(),
            "secret".into(),
            "ws://127.0.0.1:7880".into(),
            "http://127.0.0.1:9".into(),
            "hm_internal_metering_secret_2026".into(),
            chat_fake.clone(),
            synth_fake,
            true,
        );

        // Missing auth header must return 401 (no seed key fallback)
        let unauth_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/voice/sessions")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"task":"transcribe"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth_resp.status(), StatusCode::UNAUTHORIZED);

        // Transcribe on STT-only tenant succeeds
        let tr_sess_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/voice/sessions")
                    .header("Authorization", format!("Bearer {}", stt_only_key.raw_key))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"task":"transcribe","source_lang":"zh"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tr_sess_resp.status(), StatusCode::OK);

        // Cascade on STT-only tenant fails with 400 llm_unavailable
        let cas_sess_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/voice/sessions")
                    .header("Authorization", format!("Bearer {}", stt_only_key.raw_key))
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"task":"cascade"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cas_sess_resp.status(), StatusCode::BAD_REQUEST);

        // 4. text_translate sent to /api/v1/voice/sessions returns 400 use_text_endpoint
        let bad_tt = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/voice/sessions")
                    .header("Authorization", "Bearer hm_ak_live_ugreen_2026_voice_key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"task":"text_translate","target_lang":"en"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_tt.status(), StatusCode::BAD_REQUEST);
        let bad_tt_bytes = to_bytes(bad_tt.into_body(), 1024 * 64).await.unwrap();
        let bad_tt_json: serde_json::Value = serde_json::from_slice(&bad_tt_bytes).unwrap();
        assert_eq!(bad_tt_json["error"], "use_text_endpoint");

        // 5. context session has NULL room_name, no participant_token, and list_voice_sessions + resolve_session_identity work
        let ctx_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/voice/sessions")
                    .header("Authorization", "Bearer hm_ak_live_ugreen_2026_voice_key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"task":"context","target_lang":"zh"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ctx_resp.status(), StatusCode::OK);
        let ctx_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(ctx_resp.into_body(), 65536).await.unwrap()).unwrap();
        let ctx_sid = ctx_json["data"]["session_id"].as_str().unwrap().to_string();
        assert!(ctx_json["data"]["room_name"].is_null());
        assert!(ctx_json["data"]["participant_token"].is_null());

        let sessions_list = repo
            .list_voice_sessions(Some("tenant_ugreen_01"))
            .await
            .unwrap();
        assert!(sessions_list.iter().any(|s| s.session_id == ctx_sid && s.room_name.is_none()));
        assert!(repo.resolve_session_identity(&ctx_sid).await.unwrap().is_some());
        assert!(repo.resolve_session_identity("").await.unwrap().is_none());

        // 6. Concurrent append_summary_input gets seq 1 and 2; same client_input_id deduplicates & backfills translation
        let r1 = repo.clone();
        let r2 = repo.clone();
        let sid1 = ctx_sid.clone();
        let sid2 = ctx_sid.clone();
        let (a1, a2) = tokio::join!(
            async move {
                r1.append_summary_input(&sid1, "stt", Some("第一句原文"), None, None, "stt:turn1")
                    .await
                    .unwrap()
            },
            async move {
                r2.append_summary_input(&sid2, "text", Some("补充笔记"), None, None, "note-1")
                    .await
                    .unwrap()
            }
        );
        let mut seqs = vec![a1.seq, a2.seq];
        seqs.sort();
        assert_eq!(seqs, vec![1, 2]);

        // Backfill translated_text on stt:turn1 without allocating seq 3
        let backfill = repo
            .append_summary_input(
                &ctx_sid,
                "stt",
                Some("第一句原文"),
                Some("First sentence translated"),
                None,
                "stt:turn1",
            )
            .await
            .unwrap();
        assert!(backfill.replayed);
        assert_eq!(backfill.seq, a1.seq);
        let all_inputs = repo.list_summary_inputs(&ctx_sid).await.unwrap();
        assert_eq!(all_inputs.len(), 2);

        // Verify speech_translate default materials include BOTH the translation and the [note]
        let (mode, joined, count, _, _, _) =
            format_summary_materials("speech_translate", None, &all_inputs).unwrap();
        assert_eq!(mode, "translated");
        assert_eq!(count, 2);
        assert!(joined.contains("First sentence translated"));
        assert!(joined.contains("[note] 补充笔记"));

        // 7. Concurrent summary execution with same request_id calls FakeChatClient only ONCE
        let calls_before = chat_fake.calls.load(Ordering::SeqCst);
        let app1 = app.clone();
        let app2 = app.clone();
        let s_a = ctx_sid.clone();
        let s_b = ctx_sid.clone();
        let (sum_res1, sum_res2) = tokio::join!(
            async move {
                app1.oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/v1/voice/sessions/{}/summaries", s_a))
                        .header("Authorization", "Bearer hm_ak_live_ugreen_2026_voice_key")
                        .header("Content-Type", "application/json")
                        .body(Body::from(
                            r#"{"request_id":"req_dedup_1","speak":true,"target_lang":"ja","summary_style":"speech"}"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap()
            },
            async move {
                tokio::time::sleep(Duration::from_millis(15)).await;
                app2.oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/v1/voice/sessions/{}/summaries", s_b))
                        .header("Authorization", "Bearer hm_ak_live_ugreen_2026_voice_key")
                        .header("Content-Type", "application/json")
                        .body(Body::from(
                            r#"{"request_id":"req_dedup_1","speak":true,"target_lang":"ja","summary_style":"speech"}"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap()
            }
        );
        let calls_after = chat_fake.calls.load(Ordering::SeqCst);
        assert_eq!(calls_after - calls_before, 1);
        let statuses = [sum_res1.status(), sum_res2.status()];
        assert!(statuses.contains(&StatusCode::OK));

        // Check that session title was populated from `# 演讲探索与核心观点`
        let updated_sess = repo.get_session_record(&ctx_sid).await.unwrap().unwrap();
        assert_eq!(updated_sess.title, "演讲探索与核心观点");
        assert_eq!(
            updated_sess.tts_config_id.as_deref(),
            Some(own_ja.config_id.as_str())
        );

        // 8. Change target_lang from ja to en -> rebinds TTS to fallback Piper (not ja!), preserves session title on concise summary
        let lang_upd = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/internal/v1/voice/sessions/{}/target-lang", ctx_sid))
                    .header("X-HiMarket-Internal-Secret", "hm_internal_metering_secret_2026")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"target_lang":"en","resolve_tts":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lang_upd.status(), StatusCode::OK);
        let lang_upd_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(lang_upd.into_body(), 65536).await.unwrap()).unwrap();
        assert_eq!(lang_upd_json["data"]["target_lang"], "en");
        assert_eq!(lang_upd_json["data"]["voice_fallback"], true);
        assert_ne!(
            lang_upd_json["data"]["tts"]["config_id"].as_str(),
            Some(own_ja.config_id.as_str())
        );

        // Concise summary with new request_id creates a second result row and keeps original session title
        let concise_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/voice/sessions/{}/summaries", ctx_sid))
                    .header("Authorization", "Bearer hm_ak_live_ugreen_2026_voice_key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"request_id":"req_concise_2","speak":true,"summary_style":"concise"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(concise_resp.status(), StatusCode::OK);
        let concise_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(concise_resp.into_body(), 65536).await.unwrap())
                .unwrap();
        assert_eq!(concise_json["data"]["summary_style"], "concise");
        assert_eq!(concise_json["data"]["title"], "精简版总结标题");
        let sess_after_concise = repo.get_session_record(&ctx_sid).await.unwrap().unwrap();
        assert_eq!(sess_after_concise.title, "演讲探索与核心观点");

        // 9. Speak HTTP + failing synth client returns 502 tts_failed with no audio bytes
        let spk_post = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/voice/speak")
                    .header("Authorization", "Bearer hm_ak_live_ugreen_2026_voice_key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"text":"你好世界","target_lang":"zh"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(spk_post.status(), StatusCode::OK);
        let spk_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(spk_post.into_body(), 65536).await.unwrap()).unwrap();
        let spk_sid = spk_json["data"]["session_id"].as_str().unwrap();

        let seg_get = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/v1/voice/speak/{}/segments/0", spk_sid))
                    .header("Authorization", "Bearer hm_ak_live_ugreen_2026_voice_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(seg_get.status(), StatusCode::BAD_GATEWAY);
        let seg_err_bytes = to_bytes(seg_get.into_body(), 65536).await.unwrap();
        assert_eq!(&seg_err_bytes[..], br#"{"error":"tts_failed"}"#);

        // Room speak task rejects target-lang update with task_rejected
        let spk_lang_reject = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/internal/v1/voice/sessions/{}/target-lang", spk_sid))
                    .header("X-HiMarket-Internal-Secret", "hm_internal_metering_secret_2026")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"target_lang":"en"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(spk_lang_reject.status(), StatusCode::BAD_REQUEST);
    }
}
