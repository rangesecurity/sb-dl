pub mod commands;

use {
    anyhow::{anyhow, Result},
    clap::{value_parser, Arg, ArgMatches, Command},
    sb_dl::utils::init_log,
};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Command::new("sb_dl")
        .about("solana block downloader")
        .arg(
            Arg::new("log-level")
                .long("log-level")
                .help("log verbosity to use")
                .default_value("info"),
        )
        .arg(
            Arg::new("log-file")
                .long("log-file")
                .help("optionally output logs to this file")
                .default_value(""),
        )
        .arg(
            Arg::new("config")
                .long("config")
                .default_value("config.yaml"),
        )
        .subcommands(vec![
            Command::new("services")
                .about("service management commands")
                .subcommands(vec![
                    Command::new("bigtable-downloader")
                        .about("download historical block data using bigtable")
                        .arg(
                            Arg::new("start")
                                .long("start")
                                .value_parser(value_parser!(u64))
                                .required(false),
                        )
                        .arg(
                            Arg::new("limit")
                                .long("limit")
                                .help("max number of slots to index")
                                .value_parser(value_parser!(u64))
                                .required(false),
                        )
                        .arg(no_minimization_flag())
                        .arg(failed_blocks_flag()),
                    Command::new("backfiller")
                        .about("block backfiller to covers gaps missed by geyser")
                        .arg(no_minimization_flag())
                        .arg(failed_blocks_flag()),
                    Command::new("geyser-stream")
                        .about("stream blocks in real-time using geyser")
                        .arg(no_minimization_flag())
                        .arg(failed_blocks_flag()),
                    Command::new("index-idls").about("index anchor idl accounts"),
                    Command::new("index-programs").about("index deployed programs"),
                    Command::new("transfer-flow-api")
                        .about("starts api used to returned transfer flow data")
                        .arg(
                            Arg::new("listen-url")
                                .long("listen-url")
                                .help("url to expose the api on"),
                        ),
                    Command::new("index-spl-token-mints")
                    .about("index spl-token mint accounts"),
                    Command::new("index-spl-token2022-mints")
                    .about("index spl-token mint accounts"),
                    Command::new("index-token-metadata")
                    .about("indexes mpl token metadata accounts")
                ]),
            Command::new("import-failed-blocks").arg(failed_blocks_flag()),
            Command::new("new-config"),
            Command::new("manual-idl-import")
                .about("manually import an idl into the database")
                .long_about("useful for programs that publish anchor idls offchain")
                .arg(
                    Arg::new("input")
                        .long("input")
                        .help("file containing the idl"),
                )
                .arg(
                    Arg::new("program-id")
                        .long("program-id")
                        .help("program to associate this idl with"),
                ),
            Command::new("fill-missing-slots")
                .about("fill missing slot information")
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .help("number of blocks to fill at once")
                        .value_parser(clap::value_parser!(i64)),
                ),
            Command::new("create-transfer-graph-for-tx")
                .about("generate transfer graph for a single tx")
                .arg(
                    Arg::new("slot-number")
                        .long("slot-number")
                        .help("slot number to fetch tx from")
                        .value_parser(clap::value_parser!(i64)),
                )
                .arg(
                    Arg::new("tx-hash")
                        .long("tx-hash")
                        .help("tx to generate graph for"),
                ),
            Command::new("create-ordered-transfers-for-block")
                .about("generates ordered transfers for an entire block")
                .arg(
                    Arg::new("slot-number")
                        .long("slot-number")
                        .help("slot number to fetch tx from")
                        .value_parser(clap::value_parser!(i64)),
                ),
        ])
        .get_matches();

    let config_path = matches.get_one::<String>("config").unwrap();
    let log_level = matches.get_one::<String>("log-level").unwrap();
    let log_file = matches.get_one::<String>("log-file").unwrap();
    // only preserve logs file the single most recent execution of the service
    if let Ok(exists) = tokio::fs::try_exists(log_file).await {
        if exists {
            if let Err(err) = tokio::fs::rename(log_file, format!("{log_file}.old")).await {
                log::error!("failed to rotate log file {err:#?}");
            }
        }
    }
    init_log(log_level, log_file);

    process_matches(&matches, config_path).await
}

async fn process_matches(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    match matches.subcommand() {
        Some(("import-failed-blocks", ifb)) => {
            commands::services::downloaders::import_failed_blocks(ifb, config_path).await
        }
        Some(("new-config", _)) => commands::config::new_config(config_path).await,
        Some(("manual-idl-import", mii)) => {
            commands::services::idl_indexer::manual_idl_import(mii, config_path).await
        }
        Some(("fill-missing-slots", fms)) => {
            commands::db::fill_missing_slots(fms, config_path).await
        }
        Some(("create-transfer-graph", ctg)) => {
            commands::transfer_graph::create_transfer_graph_for_tx(ctg, config_path).await
        }
        Some(("create-ordered-transfers-for-block", cotfb)) => {
            commands::transfer_graph::create_ordered_transfers_for_entire_block(cotfb, config_path)
                .await
        }
        Some(("services", s)) => match s.subcommand() {
            Some(("bigtable-downloader", bd)) => {
                commands::services::downloaders::bigtable_downloader(bd, config_path).await
            }
            Some(("geyser-stream", gs)) => {
                commands::services::downloaders::geyser_stream(gs, config_path).await
            }
            Some(("backfiller", bf)) => {
                commands::services::downloaders::backfiller(bf, config_path).await
            }
            Some(("index-idls", _)) => {
                commands::services::idl_indexer::index_idls(config_path).await
            }
            Some(("index-programs", _)) => {
                commands::services::program_indexer::index_programs(config_path).await
            }
            Some(("transfer-flow-api", tfa)) => {
                commands::services::transfer_api::transfer_flow_api(tfa, config_path).await
            }
            Some(("index-spl-token-mints", _)) => commands::services::mint_indexer::index_spl_token_mints(config_path).await,
            Some(("index-spl-token2022-mints", _)) => commands::services::mint_indexer::index_spl_token2022_mints(config_path).await,
            Some(("index-token-metadata", _)) => commands::services::metadata_indexer::index_metadata_accounts(config_path).await,
            _ => Err(anyhow!("invalid subcommand")),
        },
        _ => Err(anyhow!("invalid subcommand")),
    }
}

fn no_minimization_flag() -> Arg {
    Arg::new("no-minimization")
        .long("no-minimization")
        .help("if present, disable block minimization")
        .action(clap::ArgAction::SetTrue)
        .default_value("false")
        .required(false)
}

fn failed_blocks_flag() -> Arg {
    Arg::new("failed-blocks")
        .long("failed-blocks")
        .help("directory to store failed blocks in")
        .default_value("failed_blocks")
        .required(false)
}
