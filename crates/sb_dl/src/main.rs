pub mod cli;
pub mod commands;

use {
    anyhow::{anyhow, Result},
    clap::{value_parser, Arg, ArgMatches, Command, Parser},
    cli::{Commands, ServicesCommands},
    sb_dl::utils::init_log,
};

#[tokio::main]
async fn main() -> Result<()> {
    let app = cli::Cli::parse();

    // only preserve logs file the single most recent execution of the service
    if let Ok(exists) = tokio::fs::try_exists(&app.log_file).await {
        if exists {
            if let Err(err) =
                tokio::fs::rename(&app.log_file, format!("{}.old", app.log_file)).await
            {
                log::error!("failed to rotate log file {err:#?}");
            }
        }
    }
    init_log(&app.log_level, &app.log_file);
    match &app.command {
        Commands::Services { command } => match command {
            ServicesCommands::BigtableDownloader { .. } => {
                commands::services::downloaders::bigtable_downloader(command.clone(), &app.config)
                    .await
            }
            ServicesCommands::Backfiller { .. } => {
                commands::services::downloaders::backfiller(command.clone(), &app.config).await
            }
            ServicesCommands::GeyserStream { .. } => {
                commands::services::downloaders::geyser_stream(command.clone(), &app.config).await
            }
            ServicesCommands::IndexIdls => {
                commands::services::idl_indexer::index_idls(&app.config).await
            }
            ServicesCommands::IndexPrograms => {
                commands::services::program_indexer::index_programs(&app.config).await
            }
            ServicesCommands::TransferFlowApi { .. } => {
                commands::services::transfer_api::transfer_flow_api(command.clone(), &app.config)
                    .await
            }
            ServicesCommands::RepairGaps { .. } => {
                commands::services::repair_gaps::repair_gaps(command.clone(), &app.config).await
            }
            ServicesCommands::TransferParser { .. } => {
                commands::services::transfer_parser::transfer_parser(command.clone(), &app.config)
                    .await
            }
            ServicesCommands::SquadsIndexer { .. } => {
                commands::services::squads_indexer::index_multisigs(command.clone(), &app.config)
                    .await
            }
            ServicesCommands::ImportFailedBlocks { .. } => {
                commands::services::downloaders::import_failed_blocks(command.clone(), &app.config)
                    .await
            }
        },
        Commands::NewConfig => commands::config::new_config(&app.config).await,
        Commands::ManualIdlImport { input, program_id } => {
            commands::services::idl_indexer::manual_idl_import(input, program_id, &app.config).await
        }
        Commands::CreateTransferGraphForTx {
            slot_number,
            tx_hash,
        } => {
            commands::transfer_graph::create_transfer_graph_for_tx(
                *slot_number,
                tx_hash,
                &app.config,
            )
            .await
        }
        Commands::CreateOrderedTransfersForBlock { slot_number } => {
            commands::transfer_graph::create_ordered_transfers_for_entire_block(
                *slot_number,
                &app.config,
            )
            .await
        }
        Commands::FindGapEnd {
            gap_start,
        } => commands::db::find_gap_end(*gap_start, &app.config).await,
    }
}
