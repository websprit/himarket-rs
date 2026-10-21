use axum::{
    Json, Router,
    extract::{Query, State},
    http::HeaderMap,
    routing::{get, post},
};
use himarket_core::{ApiResponse, AppError, UsageSummaryResponse};
use himarket_dal::{
    ApiKeyCreated, ApiKeyDto, AuthSession, ConsumerDto, DalRepo, ModelConfig, PortalDto,
    ProductDto, VoiceSessionDto,
};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct UsageQueryParams {
    pub tenant_id: Option<String>,
    pub api_key_id: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub role: String,
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct NamedRequest {
    pub name: String,
    pub description: Option<String>,
    pub product_type: Option<String>,
    pub portal_id: Option<String>,
    pub developer_id: Option<String>,
}

#[derive(Deserialize)]
pub struct ApiKeyRequest {
    pub consumer_id: String,
}

#[derive(Deserialize)]
pub struct CredentialsRequest {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub base_url: Option<String>,
}

#[derive(Deserialize)]
pub struct ModelConfigRequest {
    pub name: String,
    pub kind: String,
    pub protocol: String,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub model: String,
    pub voice: Option<String>,
    pub is_default: Option<bool>,
}

#[derive(serde::Serialize)]
pub struct ModelConfigView {
    pub config_id: String,
    pub name: String,
    pub kind: String,
    pub protocol: String,
    pub base_url: String,
    pub api_key_hint: String,
    pub has_api_secret: bool,
    pub model: String,
    pub voice: String,
    pub is_default: bool,
    pub status: String,
}

fn view_config(config: ModelConfig) -> ModelConfigView {
    let hint = if config.api_key.len() <= 4 {
        if config.api_key.is_empty() { String::new() } else { "****".into() }
    } else {
        format!("****{}", &config.api_key[config.api_key.len() - 4..])
    };
    ModelConfigView {
        config_id: config.config_id,
        name: config.name,
        kind: config.kind,
        protocol: config.protocol,
        base_url: config.base_url,
        api_key_hint: hint,
        has_api_secret: !config.api_secret.is_empty(),
        model: config.model,
        voice: config.voice,
        is_default: config.is_default,
        status: config.status,
    }
}

pub fn market_router(repo: DalRepo) -> Router {
    Router::new()
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/me", get(me))
        .route("/api/v1/portals", get(list_portals).post(create_portal))
        .route("/api/v1/products", get(list_products).post(create_product))
        .route("/api/v1/consumers", get(list_consumers).post(create_consumer))
        .route("/api/v1/api-keys", get(list_api_keys).post(create_api_key))
        .route("/api/v1/voice/sessions", get(list_voice_sessions))
        .route("/api/v1/model-configs", get(list_model_configs).post(create_model_config))
        .route("/api/v1/model-configs/{config_id}/credentials", post(update_model_credentials))
        .route("/api/v1/model-configs/{config_id}/default", post(set_default_model_config))
        .route("/api/v1/model-configs/{config_id}/disable", post(disable_model_config))
        .route("/api/v1/usage/summary", get(get_usage_summary))
        .with_state(repo)
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|v| v.trim().to_string())
}

async fn require_user(repo: &DalRepo, headers: &HeaderMap) -> Result<AuthSession, AppError> {
    let token = bearer(headers).ok_or_else(|| AppError::Unauthorized("login required".into()))?;
    repo.auth_session(&token)
        .await
        .ok_or_else(|| AppError::Unauthorized("invalid session".into()))
}

async fn login(
    State(repo): State<DalRepo>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<ApiResponse<AuthSession>>, AppError> {
    let role = if req.role == "admin" { "admin" } else { "developer" };
    let session = repo
        .login(role, req.username.trim(), &req.password)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::Unauthorized("用户名或密码错误".into()))?;
    Ok(ApiResponse::ok(session))
}

async fn me(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<AuthSession>>, AppError> {
    Ok(ApiResponse::ok(require_user(&repo, &headers).await?))
}

async fn list_portals(
    State(repo): State<DalRepo>,
) -> Result<Json<ApiResponse<Vec<PortalDto>>>, AppError> {
    Ok(ApiResponse::ok(repo.list_portals().await.map_err(|e| AppError::Internal(e.to_string()))?))
}

async fn create_portal(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<NamedRequest>,
) -> Result<Json<ApiResponse<PortalDto>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can create portals".into()));
    }
    Ok(ApiResponse::ok(
        repo.create_portal(&req.name, req.description.as_deref())
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}

async fn list_products(
    State(repo): State<DalRepo>,
) -> Result<Json<ApiResponse<Vec<ProductDto>>>, AppError> {
    Ok(ApiResponse::ok(repo.list_products().await.map_err(|e| AppError::Internal(e.to_string()))?))
}

async fn create_product(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<NamedRequest>,
) -> Result<Json<ApiResponse<ProductDto>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can publish products".into()));
    }
    Ok(ApiResponse::ok(
        repo.create_product(
            &req.name,
            req.product_type.as_deref().unwrap_or("AGENT_API"),
            req.description.as_deref(),
        )
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}

async fn list_consumers(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<ConsumerDto>>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    let tenant = if user.role == "developer" {
        Some(user.subject_id.as_str())
    } else {
        q.tenant_id.as_deref()
    };
    Ok(ApiResponse::ok(repo.list_consumers(tenant).await.map_err(|e| AppError::Internal(e.to_string()))?))
}

async fn create_consumer(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<NamedRequest>,
) -> Result<Json<ApiResponse<ConsumerDto>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    let (portal_id, developer_id) = if user.role == "developer" {
        (user.portal_id.clone().unwrap_or_else(|| "portal_default".into()), user.subject_id.clone())
    } else {
        (
            req.portal_id.clone().unwrap_or_else(|| "portal_default".into()),
            req.developer_id.clone().unwrap_or_else(|| "tenant_ugreen_01".into()),
        )
    };
    Ok(ApiResponse::ok(
        repo.create_consumer(&req.name, &portal_id, &developer_id, req.description.as_deref())
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}

async fn list_api_keys(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<ApiKeyDto>>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    let tenant = if user.role == "developer" { Some(user.subject_id.as_str()) } else { q.tenant_id.as_deref() };
    Ok(ApiResponse::ok(repo.list_api_keys(tenant).await.map_err(|e| AppError::Internal(e.to_string()))?))
}

async fn create_api_key(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<ApiKeyRequest>,
) -> Result<Json<ApiResponse<ApiKeyCreated>>, AppError> {
    require_user(&repo, &headers).await?;
    Ok(ApiResponse::ok(repo.create_api_key(&req.consumer_id).await.map_err(|e| AppError::BadRequest(e.to_string()))?))
}

async fn list_voice_sessions(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<VoiceSessionDto>>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    let tenant = if user.role == "developer" { Some(user.subject_id.as_str()) } else { q.tenant_id.as_deref() };
    Ok(ApiResponse::ok(repo.list_voice_sessions(tenant).await.map_err(|e| AppError::Internal(e.to_string()))?))
}

async fn list_model_configs(
    State(repo): State<DalRepo>,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<ModelConfigView>>>, AppError> {
    let items = repo.list_model_configs(None).await.map_err(|e| AppError::Internal(e.to_string()))?;
    let _ = q;
    Ok(ApiResponse::ok(items.into_iter().map(view_config).collect()))
}

async fn create_model_config(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<ModelConfigRequest>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can configure models".into()));
    }
    let created = repo.create_model_config(
        &req.name,
        &req.kind,
        &req.protocol,
        req.base_url.as_deref().unwrap_or(""),
        req.api_key.as_deref().unwrap_or(""),
        req.api_secret.as_deref().unwrap_or(""),
        &req.model,
        req.voice.as_deref().unwrap_or(""),
        req.is_default.unwrap_or(false),
    ).await.map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(view_config(created)))
}

async fn update_model_credentials(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    axum::extract::Path(config_id): axum::extract::Path<String>,
    Json(req): Json<CredentialsRequest>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can configure models".into()));
    }
    let updated = repo.update_model_credentials(&config_id, req.api_key.as_deref(), req.api_secret.as_deref(), req.base_url.as_deref())
        .await.map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(view_config(updated)))
}

async fn set_default_model_config(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    axum::extract::Path(config_id): axum::extract::Path<String>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can configure models".into()));
    }
    Ok(ApiResponse::ok(view_config(repo.set_default_model_config(&config_id).await.map_err(|e| AppError::BadRequest(e.to_string()))?)))
}

async fn disable_model_config(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    axum::extract::Path(config_id): axum::extract::Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can configure models".into()));
    }
    repo.disable_model_config(&config_id).await.map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(()))
}

async fn get_usage_summary(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<UsageSummaryResponse>>, AppError> {
    let user = if let Some(token) = bearer(&headers) { repo.auth_session(&token).await } else { None };
    let tenant_id = if user.as_ref().map(|u| u.role.as_str()) == Some("developer") {
        user.unwrap().subject_id
    } else {
        q.tenant_id.clone().unwrap_or_else(|| "tenant_ugreen_01".into())
    };
    Ok(ApiResponse::ok(
        repo.query_usage_summary(&tenant_id, q.api_key_id.as_deref())
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}
