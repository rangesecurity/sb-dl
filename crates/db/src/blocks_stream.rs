//! provides support for real-time postgres notifications on block changes

use {
    crate::new_tokio_postgres_connection, anyhow::{Context, Result}, futures::{
        channel::mpsc, future, join, pin_mut, stream, try_join, FutureExt, SinkExt, StreamExt, TryStreamExt
    }, std::sync::Arc, tokio_postgres::{AsyncMessage, Client}
};

#[derive(Clone)]
pub struct BlocksStreamClient {}

impl BlocksStreamClient {
    pub async fn new(db_url: &str) -> anyhow::Result<mpsc::UnboundedReceiver<AsyncMessage>> {
        let (client, mut connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls).await?;
        let (tx, rx) = mpsc::unbounded();
        let stream =
            stream::poll_fn(move |cx| connection.poll_message(cx)).map_err(|e| panic!("{}", e));
        let connection = stream.forward(tx).map(|r| r.unwrap());
        tokio::spawn(connection);
    
        client
            .batch_execute(
                "LISTEN block_changes;",
            )
            .await
            .unwrap();
    
        drop(client);
        Ok(rx)
    }
}
