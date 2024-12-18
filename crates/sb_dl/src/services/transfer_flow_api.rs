use {
    crate::transfer_flow::{create_ordered_transfer_for_block, types::OrderedTransfers},
    anyhow::Context,
    axum::{
        http::StatusCode,
        response::IntoResponse,
        routing::post,
        Json, Router,
    },
    chrono::prelude::*,
    serde::{Deserialize, Serialize},
    solana_transaction_status::UiConfirmedBlock,
    tower_http::{
        trace::{DefaultOnResponse, TraceLayer},
        LatencyUnit,
    },
};

pub async fn serve_api(listen_url: &str) -> anyhow::Result<()> {
    let router = new_router()?;
    let listener = tokio::net::TcpListener::bind(listen_url).await?;
    axum::serve(listener, router)
        .await
        .with_context(|| "api failed")
}

pub fn new_router() -> anyhow::Result<Router> {
    let app = Router::new()
        .route(
            "/orderedTransfers",
            post(ordered_transfers_for_block),
        )
        .layer(
            TraceLayer::new_for_http().on_response(
                DefaultOnResponse::new()
                    .level(tracing::Level::INFO)
                    .latency_unit(LatencyUnit::Millis),
            ),
        );
    return Ok(app);
}

async fn ordered_transfers_for_block(
    Json(payload): Json<OrderedTransfersRequest>,
) -> impl IntoResponse {
    let block: UiConfirmedBlock = match serde_json::from_value(payload.block) {
        Ok(block) => block,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(Error {
                    msg: format!("failed to deserialize block {err:#?}"),
                }),
            )
                .into_response()
        }
    };
    let time = if let Some(block_time) = block.block_time {
        DateTime::from_timestamp(block_time, 0)
    } else {
        None
    };
    let block_number = block.block_height;
    match create_ordered_transfer_for_block(block) {
        Ok(ordered_transfers) => {
            return (
                StatusCode::OK,
                Json(OrderedTransfersResponse {
                    transfers: ordered_transfers,
                    time,
                    slot: None,
                }),
            )
                .into_response()
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(Error {
                    msg: format!("failed to extract transfers(block={block_number:?}) {err:#?}"),
                }),
            )
                .into_response()
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrderedTransfersResponse {
    pub transfers: Vec<OrderedTransfers>,
    /// the block_time field in UiConfirmedBlock has an Option<_> type
    /// so its possible older values do not contain that field
    /// in which case we will set this time to None
    pub time: Option<DateTime<Utc>>,
    /// this will not be used by the transfer flow api
    /// but will be used by the transfer parser for elastic search
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<i64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Error {
    pub msg: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrderedTransfersRequest {
    pub block: serde_json::Value,
}
