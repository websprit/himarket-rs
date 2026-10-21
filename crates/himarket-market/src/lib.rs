use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use himarket_core::{ApiResponse, AppError, UsageSummaryResponse};
use himarket_dal::{ConsumerDto, DalRepo, PortalDto, ProductDto};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct UsageQueryParams {
    pub tenant_id: Option<String>,
    pub api_key_id: Option<String>,
}

pub fn market_router(repo: DalRepo) -> Router {
    Router::new()
        .route("/api/v1/portals", get(list_portals))
        .route("/api/v1/products", get(list_products))
        .route("/api/v1/consumers", get(list_consumers))
        .route("/api/v1/usage/summary", get(get_usage_summary))
        .with_state(repo)
}

async fn list_portals(
    State(repo): State<DalRepo>,
) -> Result<Json<ApiResponse<Vec<PortalDto>>>, AppError> {
    let items = repo
        .list_portals()
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(ApiResponse::ok(items))
}

async fn list_products(
    State(repo): State<DalRepo>,
) -> Result<Json<ApiResponse<Vec<ProductDto>>>, AppError> {
    let items = repo
        .list_products()
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(ApiResponse::ok(items))
}

async fn list_consumers(
    State(repo): State<DalRepo>,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<Vec<ConsumerDto>>>, AppError> {
    let items = repo
        .list_consumers(q.tenant_id.as_deref())
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(ApiResponse::ok(items))
}

async fn get_usage_summary(
    State(repo): State<DalRepo>,
    Query(q): Query<UsageQueryParams>,
) -> Result<Json<ApiResponse<UsageSummaryResponse>>, AppError> {
    let tenant_id = q.tenant_id.as_deref().unwrap_or("tenant_ugreen_01");
    let summary = repo
        .query_usage_summary(tenant_id, q.api_key_id.as_deref())
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(ApiResponse::ok(summary))
}
