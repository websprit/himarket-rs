use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post, put},
};
use himarket_core::{
    ApiResponse, AppError, QuotaAlertDto, TenantLanguagePolicy, UsageReportResponse,
    UsageSummaryResponse, infer_vendor,
};
use himarket_dal::{
    ApiKeyCreated, ApiKeyDto, AuthSession, ConsumerDto, DalRepo, ModelConfig, PortalDto,
    ProductDto, RoutePolicyView, VoiceSessionDto, parse_extra_lang_list,
};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct UsageQueryParams {
    pub tenant_id: Option<String>,
    pub kind: Option<String>,
    pub api_key_id: Option<String>,
    pub consumer_id: Option<String>,
    pub category: Option<String>,
    pub vendor: Option<String>,
    pub date: Option<String>,
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
    pub tenant_id: Option<String>,
    pub name: String,
    pub kind: String,
    pub protocol: String,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub model: String,
    pub voice: Option<String>,
    pub is_default: Option<bool>,
    pub tts_languages: Option<Vec<String>>,
    pub stt_languages: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct UpdateTtsLanguagesRequest {
    pub tts_languages: Vec<String>,
}

#[derive(Deserialize)]
pub struct UpdateSttLanguagesRequest {
    pub stt_languages: Vec<String>,
}

#[derive(Deserialize)]
pub struct UpsertTenantLanguagePolicyRequest {
    pub tenant_id: String,
    pub default_target_lang: String,
    pub allowed_langs: Vec<String>,
}

#[derive(Deserialize)]
pub struct UpsertRouteRequest {
    pub tenant_id: Option<String>,
    pub capability: String,
    #[serde(default)]
    pub language: String,
    pub config_id: String,
}

#[derive(Deserialize)]
pub struct UpdateVendorStatusRequest {
    pub tenant_id: Option<String>,
    pub status: String,
}

#[derive(Deserialize)]
pub struct UpdateApiKeyQuotaRequest {
    pub quota_limit_calls: Option<i64>,
    pub quota_limit_tokens: Option<i64>,
    pub quota_limit_usd_micros: Option<i64>,
    pub alert_threshold_pct: Option<i32>,
    pub alert_webhook_url: Option<String>,
    #[serde(default)]
    pub reset_used: bool,
}

#[derive(Deserialize)]
pub struct UpdateApiKeyStatusRequest {
    pub status: String,
}

#[derive(serde::Serialize)]
pub struct ModelConfigView {
    pub config_id: String,
    pub tenant_id: String,
    pub name: String,
    pub kind: String,
    pub protocol: String,
    pub vendor: String,
    pub base_url: String,
    pub api_key_hint: String,
    pub has_api_secret: bool,
    pub encrypted: bool,
    pub model: String,
    pub voice: String,
    pub is_default: bool,
    pub status: String,
    pub tts_languages: Vec<String>,
    pub stt_languages: Vec<String>,
}

fn view_config(config: ModelConfig) -> ModelConfigView {
    let hint = if config.api_key.len() <= 4 {
        if config.api_key.is_empty() {
            String::new()
        } else {
            "****".into()
        }
    } else {
        format!("****{}", &config.api_key[config.api_key.len() - 4..])
    };
    let tts_languages = parse_extra_lang_list(&config.extra_json, "tts_languages");
    let stt_languages = parse_extra_lang_list(&config.extra_json, "stt_languages");
    let vendor = infer_vendor(&config.protocol, &config.model, &config.base_url);
    let encrypted = !config.api_key.is_empty() || !config.api_secret.is_empty();
    ModelConfigView {
        config_id: config.config_id,
        tenant_id: config.tenant_id,
        name: config.name,
        kind: config.kind,
        protocol: config.protocol,
        vendor,
        base_url: config.base_url,
        api_key_hint: hint,
        has_api_secret: !config.api_secret.is_empty(),
        encrypted,
        model: config.model,
        voice: config.voice,
        is_default: config.is_default,
        status: config.status,
        tts_languages,
        stt_languages,
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
        .route(
            "/api/v1/api-keys/{api_key_id}/quota",
            put(update_api_key_quota),
        )
        .route(
            "/api/v1/api-keys/{api_key_id}/status",
            post(update_api_key_status),
        )
        .route("/api/v1/alerts", get(list_quota_alerts))
        .route("/api/v1/voice/sessions", get(list_voice_sessions))
        .route(
            "/api/v1/model-configs",
            get(list_model_configs).post(create_model_config),
        )
        .route(
            "/api/v1/model-configs/{config_id}/credentials",
            post(update_model_credentials),
        )
        .route(
            "/api/v1/model-configs/{config_id}/tts-languages",
            post(update_model_tts_languages),
        )
        .route(
            "/api/v1/model-configs/{config_id}/stt-languages",
            post(update_model_stt_languages),
        )
        .route(
            "/api/v1/model-configs/{config_id}/default",
            post(set_default_model_config),
        )
        .route(
            "/api/v1/model-configs/{config_id}/disable",
            post(disable_model_config),
        )
        .route(
            "/api/v1/model-configs/{config_id}/enable",
            post(enable_model_config),
        )
        .route(
            "/api/v1/vendors/{vendor}/status",
            post(update_vendor_status),
        )
        .route("/api/v1/routes", get(list_routes).put(put_route))
        .route(
            "/api/v1/tenant-language-policy",
            get(get_tenant_language_policy).put(put_tenant_language_policy),
        )
        .route("/api/v1/usage/summary", get(get_usage_summary))
        .route("/api/v1/usage/report", get(get_usage_report))
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
    let role = if req.role == "admin" {
        "admin"
    } else {
        "developer"
    };
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
    Ok(ApiResponse::ok(
        repo.list_portals()
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
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
    Ok(ApiResponse::ok(
        repo.list_products()
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}

async fn create_product(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<NamedRequest>,
) -> Result<Json<ApiResponse<ProductDto>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can publish products".into(),
        ));
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
    Ok(ApiResponse::ok(
        repo.list_consumers(tenant)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}

async fn create_consumer(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<NamedRequest>,
) -> Result<Json<ApiResponse<ConsumerDto>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "business_line_readonly: 业务线账号为只读权限，仅管理员可创建业务线应用".into(),
        ));
    }
    let portal_id = req
        .portal_id
        .clone()
        .unwrap_or_else(|| "portal_default".into());
    let developer_id = req
        .developer_id
        .clone()
        .unwrap_or_else(|| "tenant_ugreen_01".into());
    Ok(ApiResponse::ok(
        repo.create_consumer(
            &req.name,
            &portal_id,
            &developer_id,
            req.description.as_deref(),
        )
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
    let tenant = if user.role == "developer" {
        Some(user.subject_id.as_str())
    } else {
        q.tenant_id.as_deref()
    };
    Ok(ApiResponse::ok(
        repo.list_api_keys(tenant)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}

async fn create_api_key(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<ApiKeyRequest>,
) -> Result<Json<ApiResponse<ApiKeyCreated>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "business_line_readonly: 业务线账号为只读权限，仅管理员可签发 API Key".into(),
        ));
    }
    Ok(ApiResponse::ok(
        repo.create_api_key(&req.consumer_id)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?,
    ))
}

async fn update_api_key_quota(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(api_key_id): Path<String>,
    Json(req): Json<UpdateApiKeyQuotaRequest>,
) -> Result<Json<ApiResponse<ApiKeyDto>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can update quota".into()));
    }
    let updated = repo
        .update_api_key_quota(
            &api_key_id,
            req.quota_limit_calls,
            req.quota_limit_tokens,
            req.quota_limit_usd_micros,
            req.alert_threshold_pct,
            req.alert_webhook_url.as_deref(),
            req.reset_used,
        )
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(updated))
}

async fn update_api_key_status(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(api_key_id): Path<String>,
    Json(req): Json<UpdateApiKeyStatusRequest>,
) -> Result<Json<ApiResponse<ApiKeyDto>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can update status".into()));
    }
    let updated = repo
        .update_api_key_status(&api_key_id, &req.status)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(updated))
}

async fn list_quota_alerts(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<QuotaAlertDto>>>, AppError> {
    let user = if let Some(token) = bearer(&headers) {
        repo.auth_session(&token).await
    } else {
        None
    };
    let tenant = if user.as_ref().map(|u| u.role.as_str()) == Some("developer") {
        Some(user.unwrap().subject_id)
    } else {
        q.tenant_id.clone()
    };
    let alerts = repo
        .list_quota_alerts(tenant.as_deref())
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(ApiResponse::ok(alerts))
}

async fn list_voice_sessions(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<VoiceSessionDto>>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    let tenant = if user.role == "developer" {
        Some(user.subject_id.as_str())
    } else {
        q.tenant_id.as_deref()
    };
    Ok(ApiResponse::ok(
        repo.list_voice_sessions(tenant)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}

async fn list_model_configs(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<ModelConfigView>>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    let tenant = if user.role == "developer" {
        Some(user.subject_id.as_str())
    } else {
        q.tenant_id.as_deref()
    };
    let items = repo
        .list_model_configs(tenant)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(ApiResponse::ok(items.into_iter().map(view_config).collect()))
}

async fn create_model_config(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<ModelConfigRequest>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can configure models".into(),
        ));
    }
    let mut created = repo
        .create_model_config(
            req.tenant_id.as_deref().unwrap_or("tenant_ugreen_01"),
            &req.name,
            &req.kind,
            &req.protocol,
            req.base_url.as_deref().unwrap_or(""),
            req.api_key.as_deref().unwrap_or(""),
            req.api_secret.as_deref().unwrap_or(""),
            &req.model,
            req.voice.as_deref().unwrap_or(""),
            req.is_default.unwrap_or(false),
        )
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;

    if let Some(tts_langs) = &req.tts_languages {
        if !tts_langs.is_empty() {
            created = repo
                .update_model_extra_langs(&created.config_id, "tts_languages", tts_langs)
                .await
                .map_err(|e| AppError::BadRequest(e.to_string()))?;
        }
    }
    if let Some(stt_langs) = &req.stt_languages {
        if !stt_langs.is_empty() {
            created = repo
                .update_model_extra_langs(&created.config_id, "stt_languages", stt_langs)
                .await
                .map_err(|e| AppError::BadRequest(e.to_string()))?;
        }
    }

    Ok(ApiResponse::ok(view_config(created)))
}

async fn update_model_credentials(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(config_id): Path<String>,
    Json(req): Json<CredentialsRequest>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can configure models".into(),
        ));
    }
    let updated = repo
        .update_model_credentials(
            &config_id,
            req.api_key.as_deref(),
            req.api_secret.as_deref(),
            req.base_url.as_deref(),
        )
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(view_config(updated)))
}

async fn update_model_tts_languages(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(config_id): Path<String>,
    Json(req): Json<UpdateTtsLanguagesRequest>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can configure models".into(),
        ));
    }
    let updated = repo
        .update_model_extra_langs(&config_id, "tts_languages", &req.tts_languages)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(view_config(updated)))
}

async fn update_model_stt_languages(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(config_id): Path<String>,
    Json(req): Json<UpdateSttLanguagesRequest>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can configure models".into(),
        ));
    }
    let updated = repo
        .update_model_extra_langs(&config_id, "stt_languages", &req.stt_languages)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(view_config(updated)))
}

async fn set_default_model_config(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(config_id): Path<String>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can configure models".into(),
        ));
    }
    Ok(ApiResponse::ok(view_config(
        repo.set_default_model_config(&config_id)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?,
    )))
}

async fn disable_model_config(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(config_id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can configure models".into(),
        ));
    }
    repo.disable_model_config(&config_id)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(()))
}

async fn enable_model_config(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(config_id): Path<String>,
) -> Result<Json<ApiResponse<ModelConfigView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can configure models".into(),
        ));
    }
    let updated = repo
        .enable_model_config(&config_id)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(view_config(updated)))
}

async fn update_vendor_status(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Path(vendor): Path<String>,
    Json(req): Json<UpdateVendorStatusRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can update vendors".into()));
    }
    let tenant_id = req
        .tenant_id
        .as_deref()
        .unwrap_or("tenant_ugreen_01");
    let count = repo
        .update_vendor_status(tenant_id, &vendor, &req.status)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(serde_json::json!({
        "vendor": vendor,
        "status": req.status,
        "updated_count": count
    })))
}

async fn list_routes(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<RoutePolicyView>>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    let tenant_id = if user.role == "developer" {
        user.subject_id
    } else {
        q.tenant_id.unwrap_or_else(|| "tenant_ugreen_01".into())
    };
    let routes = repo
        .list_routes(&tenant_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(ApiResponse::ok(routes))
}

async fn put_route(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<UpsertRouteRequest>,
) -> Result<Json<ApiResponse<RoutePolicyView>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden("only admin can update routes".into()));
    }
    let tenant_id = req.tenant_id.as_deref().unwrap_or("tenant_ugreen_01");
    let updated = repo
        .upsert_route(tenant_id, &req.capability, &req.language, &req.config_id)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(updated))
}

async fn get_tenant_language_policy(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<TenantLanguagePolicy>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    let tenant_id = if user.role == "developer" {
        user.subject_id
    } else {
        q.tenant_id.unwrap_or_else(|| "tenant_ugreen_01".into())
    };
    let policy = repo
        .get_language_policy(&tenant_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| {
            AppError::bad_request_code(
                "language_policy_missing",
                format!("tenant {} has no language policy", tenant_id),
            )
        })?;
    Ok(ApiResponse::ok(policy))
}

async fn put_tenant_language_policy(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Json(req): Json<UpsertTenantLanguagePolicyRequest>,
) -> Result<Json<ApiResponse<TenantLanguagePolicy>>, AppError> {
    let user = require_user(&repo, &headers).await?;
    if user.role != "admin" {
        return Err(AppError::Forbidden(
            "only admin can update tenant language policy".into(),
        ));
    }
    let updated = repo
        .upsert_language_policy(&req.tenant_id, &req.default_target_lang, &req.allowed_langs)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(ApiResponse::ok(updated))
}

async fn get_usage_summary(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<UsageSummaryResponse>>, AppError> {
    let user = if let Some(token) = bearer(&headers) {
        repo.auth_session(&token).await
    } else {
        None
    };
    let tenant_id = if user.as_ref().map(|u| u.role.as_str()) == Some("developer") {
        user.unwrap().subject_id
    } else {
        q.tenant_id
            .clone()
            .unwrap_or_else(|| "tenant_ugreen_01".into())
    };
    Ok(ApiResponse::ok(
        repo.query_usage_summary(&tenant_id, q.api_key_id.as_deref())
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?,
    ))
}

async fn get_usage_report(
    State(repo): State<DalRepo>,
    headers: HeaderMap,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<UsageReportResponse>>, AppError> {
    let user = if let Some(token) = bearer(&headers) {
        repo.auth_session(&token).await
    } else {
        None
    };
    let tenant_id = if user.as_ref().map(|u| u.role.as_str()) == Some("developer") {
        user.unwrap().subject_id
    } else {
        q.tenant_id
            .clone()
            .unwrap_or_else(|| "tenant_ugreen_01".into())
    };
    let report = repo
        .query_usage_report(
            &tenant_id,
            q.date.as_deref(),
            q.api_key_id.as_deref(),
            q.consumer_id.as_deref(),
            q.category.as_deref(),
            q.vendor.as_deref(),
        )
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(ApiResponse::ok(report))
}
