use tokio::signal::unix::Signal;

pub mod config;
pub mod db;
pub mod services;
pub mod transfer_graph;
pub mod utils;

pub async fn handle_exit(
    mut sig_quit: Signal,
    mut sig_int: Signal,
    mut sig_term: Signal,
    finished_rx: tokio::sync::oneshot::Receiver<Option<String>>,
) -> anyhow::Result<()> {
    // handle exit routines
    tokio::select! {
        _ = sig_quit.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        _ = sig_int.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        _ = sig_term.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        msg = finished_rx => {
            match msg {
                // service encountered error
                Ok(Some(msg)) => return Err(anyhow::anyhow!(msg)),
                // service finished without error
                Ok(None) => return Ok(()),
                // underlying channel had an error
                Err(err) => return Err(anyhow::anyhow!(err))
            }
        }
    }
}