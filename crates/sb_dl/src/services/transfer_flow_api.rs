use {
    crate::transfer_flow::{create_ordered_transfer_for_block, types::OrderedTransfers},
    anyhow::Context,
    axum::{
        extract::{Extension, Path},
        http::StatusCode,
        response::IntoResponse,
        routing::get,
        Json, Router,
    },
    db::{client::BlockFilter, new_connection_pool},
    deadpool_diesel::{Manager, Pool},
    diesel::prelude::*,
    serde::{Deserialize, Serialize},
    solana_transaction_status::UiConfirmedBlock,
    std::sync::Arc,
};
#[derive(Clone)]
pub struct State {
    db_pool: Pool<Manager<PgConnection>>,
}

pub async fn serve_api(listen_url: &str, db_url: &str) -> anyhow::Result<()> {
    let router = new_router(db_url)?;
    let listener = tokio::net::TcpListener::bind(listen_url).await?;
    axum::serve(listener, router)
        .await
        .with_context(|| "api failed")
}

pub fn new_router(db_url: &str) -> anyhow::Result<Router> {
    let db_pool = new_connection_pool(db_url)?;
    let app = Router::new()
        .route(
            "/orderedTransfers/:blockNumber",
            get(ordered_transfers_for_block),
        )
        .layer(Extension(Arc::new(State { db_pool })));
    return Ok(app);
}

async fn ordered_transfers_for_block(
    Path(number): Path<i64>,
    Extension(state): Extension<Arc<State>>,
) -> impl IntoResponse {
    match state.db_pool.get().await {
        Ok(pool) => {
            match pool.lock() {
                Ok(mut db_conn) => {
                    let client = db::client::Client {};
                    match client.select_block(&mut db_conn, BlockFilter::Slot(number)) {
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

                                match create_ordered_transfer_for_block(block) {
                                    Ok(ordered_transfers) => {
                                        return (
                                            StatusCode::OK,
                                            Json(OrderedTransfersResponse {
                                                transfers: ordered_transfers
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
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Error {
    pub msg: String,
}
