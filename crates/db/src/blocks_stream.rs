//! provides support for real-time postgres notifications on block changes

use {anyhow::Result, futures::{
        channel::mpsc, stream, FutureExt, StreamExt, TryStreamExt
    }, std::sync::Arc, tokio_postgres::{AsyncMessage, Client}
};

#[derive(Clone)]
pub struct BlocksStreamClient {
    #[allow(unused)]
    client: Arc<Client>,
}

impl BlocksStreamClient {
    pub async fn new(db_url: &str) -> Result<(Self, mpsc::UnboundedReceiver<AsyncMessage>)> {
        let (client, mut connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls).await?;
        let (tx, rx) = mpsc::unbounded();
        let stream =
            stream::poll_fn(move |cx| connection.poll_message(cx)).map_err(|e| panic!("{}", e));
        let connection = stream.forward(tx).map(|r| r.unwrap());
        tokio::spawn(async move {
            connection.await
        });
    
        client
            .batch_execute(
                "LISTEN block_changes; LISTEN block2_changes;",
            )
            .await
            .unwrap();
    
        Ok((Self {
            client: Arc::new(client)
        }, rx))
    }
}
