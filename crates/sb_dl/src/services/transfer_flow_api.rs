use {
    crate::transfer_flow::{create_ordered_transfer_for_block, types::OrderedTransfers}, anyhow::Context, axum::{
        extract::{Extension, Path},
        http::StatusCode,
        response::IntoResponse,
        routing::get,
        Json, Router,
    }, chrono::prelude::*, db::{client::BlockFilter, models::BlockTableChoice, new_connection_pool}, diesel::{
        prelude::*, r2d2::{ConnectionManager, Pool, PooledConnection}
    }, serde::{Deserialize, Serialize}, solana_transaction_status::UiConfirmedBlock, std::sync::Arc
};
#[derive(Clone)]
pub struct State {
    db_pool: Pool<ConnectionManager<PgConnection>>,
}

pub async fn serve_api(listen_url: &str, db_url: &str) -> anyhow::Result<()> {
    let router = new_router(db_url)?;
    let listener = tokio::net::TcpListener::bind(listen_url).await?;
    axum::serve(listener, router)
        .await
        .with_context(|| "api failed")
}

pub fn new_router(db_url: &str) -> anyhow::Result<Router> {
    let db_pool = new_connection_pool(db_url, 10)?;
    let app = Router::new()
        .route(
            "/orderedTransfers/:blockNumber/:tableNumber",
            get(ordered_transfers_for_block),
        )
        .layer(Extension(Arc::new(State { db_pool })));
    return Ok(app);
}

async fn ordered_transfers_for_block(
    Path((number, table_number)): Path<(i64, i64)>,
    Extension(state): Extension<Arc<State>>,
) -> impl IntoResponse {
    let block_table_choice: BlockTableChoice = match TryFrom::try_from(table_number as u8) {
        Ok(choice) => choice,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(Error {
                    msg: "invalid table choice, must be one of [1, 2]".to_string()
                }),
            ).into_response()
        }
    };
    match state.db_pool.get() {
        Ok(mut db_conn) => {

                    let client = db::client::Client {};
                    match client.select_block(&mut db_conn, BlockFilter::Number(number), block_table_choice) {
                        Ok(mut blocks) => {
                            if blocks.is_empty() {
                                return (
                                    StatusCode::NOT_FOUND,
                                    Json(Error {
                                        msg: format!("block({number}) not found"),
                                    }),
                                )
                                    .into_response();
                            } else {
                                let block: UiConfirmedBlock = match serde_json::from_value(
                                    std::mem::take(&mut blocks[0].data),
                                ) {
                                    Ok(block) => block,
                                    Err(err) => {
                                        return (
                                            StatusCode::INTERNAL_SERVER_ERROR,
                                            Json(Error {
                                                msg: format!(
                                                    "failed to deserialize block {err:#?}"
                                                ),
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
                                match create_ordered_transfer_for_block(block) {
                                    Ok(ordered_transfers) => {
                                        return (
                                            StatusCode::OK,
                                            Json(OrderedTransfersResponse {
                                                transfers: ordered_transfers,
                                                slot: blocks[0].slot,
                                                time
                                            })
                                        ).into_response()
                                    }
                                    Err(err) => return (
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        Json(Error {
                                            msg: format!("failed to extract transfers(block={number}) {err:#?}")
                                        })
                                    ).into_response()
                                }
                            }
                        }
                        Err(err) => {
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(Error {
                                    msg: err.to_string(),
                                }),
                            )
                                .into_response()
                        }
                    }

        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(Error {
                    msg: err.to_string(),
                }),
            )
                .into_response()
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrderedTransfersResponse {
    pub transfers: Vec<OrderedTransfers>,
    /// eventually remove Option<_> when historical  data has been corrected
    pub slot: Option<i64>,
    /// the block_time field in UiConfirmedBlock has an Option<_> type
    /// so its possible older values do not contain that field
    /// in which case we will set this time to None
    pub time: Option<DateTime<Utc>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Error {
    pub msg: String,
}
