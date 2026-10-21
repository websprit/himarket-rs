use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use himarket_core::{ApiResponse, AppError, UsageEventPayload};
use himarket_dal::DalRepo;
use serde::Serialize;
use tracing::info;

#[derive(Clone)]
pub struct MeteringState {
    pub repo: DalRepo,
    pub internal_secret: String,
}

#[derive(Serialize)]
pub struct IngestAck {
    pub accepted: bool,
    pub deduplicated: bool,
    pub idempotency_key: String,
}

pub fn metering_router(state: MeteringState) -> Router {
    Router::new()
        .route("/internal/v1/metering/events", post(ingest_single_event))
        .route("/internal/v1/metering/batch", post(ingest_batch_events))
        .with_state(state)
}

fn verify_secret(headers: &HeaderMap, expected: &str) -> Result<(), AppError> {
    if expected.is_empty() {
        return Ok(());
    }
    let got = headers
        .get("X-HiMarket-Internal-Secret")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if got != expected {
        return Err(AppError::Unauthorized(
            "Invalid X-HiMarket-Internal-Secret".to_string(),
        ));
    }
    Ok(())
}

async fn ingest_single_event(
    State(state): State<MeteringState>,
    headers: HeaderMap,
    Json(ev): Json<UsageEventPayload>,
) -> Result<(StatusCode, Json<ApiResponse<IngestAck>>), AppError> {
    verify_secret(&headers, &state.internal_secret)?;
    let key = ev.idempotency_key.clone();
    let src = ev.source.clone();
    let cat = ev.category.clone();
    let inserted = state
        .repo
        .ingest_usage_event(ev)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    info!(
        source = %src,
        category = %cat,
        idempotency_key = %key,
        inserted = inserted,
        "Metering event processed"
    );

    Ok((
        StatusCode::ACCEPTED,
        ApiResponse::ok(IngestAck {
            accepted: true,
            deduplicated: !inserted,
            idempotency_key: key,
        }),
    ))
}

async fn ingest_batch_events(
    State(state): State<MeteringState>,
    headers: HeaderMap,
    Json(events): Json<Vec<UsageEventPayload>>,
) -> Result<(StatusCode, Json<ApiResponse<usize>>), AppError> {
    verify_secret(&headers, &state.internal_secret)?;
    let mut inserted_count = 0usize;
    for ev in events {
        if state
            .repo
            .ingest_usage_event(ev)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?
        {
            inserted_count += 1;
        }
    }
    Ok((StatusCode::ACCEPTED, ApiResponse::ok(inserted_count)))
}
