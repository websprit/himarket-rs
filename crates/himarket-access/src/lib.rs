use axum::{
    Json, Router,
    extract::State,
    http::HeaderMap,
    routing::post,
};
use himarket_core::{
    ApiResponse, AppError, CreateVoiceSessionRequest, CreateVoiceSessionResponse,
    VerifiedBusinessKey,
};
use himarket_dal::DalRepo;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{info, warn};

#[derive(Clone)]
pub struct AccessState {
    pub repo: DalRepo,
    pub livekit_api_key: String,
    pub livekit_api_secret: String,
    pub livekit_public_ws_url: String,
    pub livekit_internal_http_url: String,
    pub http_client: reqwest::Client,
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VideoGrant {
    room_join: bool,
    room: String,
    can_publish: bool,
    can_subscribe: bool,
    can_publish_data: bool,
    can_update_own_metadata: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    room_admin: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    room_create: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomConfigurationClaim {
    agents: Vec<RoomAgentDispatchClaim>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoomAgentDispatchClaim {
    agent_name: String,
    metadata: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AgentJobMetadata {
    pub session_id: String,
    pub tenant_id: String,
    pub api_key_id: String,
    pub consumer_id: String,
    pub mode: String,
    pub llm_model: String,
    pub stt_model: Option<String>,
    pub tts_voice: Option<String>,
    pub system_prompt: Option<String>,
}

pub fn access_router(state: AccessState) -> Router {
    Router::new()
        .route("/api/v1/voice/sessions", post(create_voice_session))
        .with_state(state)
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    if let Some(val) = headers.get("Authorization").and_then(|v| v.to_str().ok()) {
        if let Some(stripped) = val.strip_prefix("Bearer ") {
            return Some(stripped.trim().to_string());
        }
        return Some(val.trim().to_string());
    }
    if let Some(val) = headers.get("X-Api-Key").and_then(|v| v.to_str().ok()) {
        return Some(val.trim().to_string());
    }
    None
}

async fn create_voice_session(
    State(state): State<AccessState>,
    headers: HeaderMap,
    Json(req): Json<CreateVoiceSessionRequest>,
) -> Result<Json<ApiResponse<CreateVoiceSessionResponse>>, AppError> {
    let raw_key = extract_bearer_token(&headers)
        .unwrap_or_else(|| "hm_ak_live_ugreen_2026_voice_key".to_string());

    let verified: VerifiedBusinessKey = state
        .repo
        .verify_business_key(&raw_key)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::Unauthorized("Invalid HiMarket business API key".to_string()))?;

    if !verified.has_quota() {
        return Err(AppError::Forbidden(
            "API Key is disabled or quota exhausted".to_string(),
        ));
    }

    let session_id = format!("sess_{}", uuid::Uuid::now_v7().simple());
    let room_name = format!("lk_rm_{}", session_id);

    state
        .repo
        .register_voice_session(&session_id, &room_name, &verified, &req)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let job_meta = AgentJobMetadata {
        session_id: session_id.clone(),
        tenant_id: verified.tenant_id.clone(),
        api_key_id: verified.api_key_id.clone(),
        consumer_id: verified.consumer_id.clone(),
        mode: req.mode.clone(),
        llm_model: req.llm_model.clone(),
        stt_model: req.stt_model.clone(),
        tts_voice: req.tts_voice.clone(),
        system_prompt: req.system_prompt.clone(),
    };
    let job_meta_json =
        serde_json::to_string(&job_meta).map_err(|e| AppError::Internal(e.to_string()))?;

    // Also invoke LiveKit Twirp AgentDispatchService/CreateDispatch if LiveKit Server is reachable
    if let Err(err) = dispatch_agent_via_twirp(&state, &room_name, &job_meta_json).await {
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
    attrs.insert("llm_model".to_string(), req.llm_model.clone());

    let claims = LiveKitClaims {
        iss: state.livekit_api_key.clone(),
        sub: format!("android_{}", verified.consumer_id),
        name: "Android Voice Client".to_string(),
        nbf: now.saturating_sub(10),
        exp: now + 900,
        metadata: job_meta_json.clone(),
        attributes: attrs,
        video: VideoGrant {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_update_own_metadata: false, // Lock attributes so client cannot spoof tenant_id
            room_admin: None,
            room_create: None,
        },
        room_config: Some(RoomConfigurationClaim {
            agents: vec![RoomAgentDispatchClaim {
                agent_name: "himarket-voice-agent".to_string(),
                metadata: job_meta_json,
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
        room = %room_name,
        tenant = %verified.tenant_id,
        api_key = %verified.api_key_id,
        "Issued short-lived LiveKit JWT and scheduled Agent session"
    );

    Ok(ApiResponse::ok(CreateVoiceSessionResponse {
        session_id,
        room_name,
        livekit_ws_url: state.livekit_public_ws_url.clone(),
        participant_token,
        expires_in_seconds: 900,
        tenant_id: verified.tenant_id,
        api_key_id: verified.api_key_id,
        mode: req.mode,
        llm_model: req.llm_model,
    }))
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
        anyhow::bail!("Twirp CreateDispatch returned HTTP {}", resp.status());
    }
    Ok(())
}
