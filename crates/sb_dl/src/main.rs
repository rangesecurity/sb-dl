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
            Command::new("download")
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
                .arg(
                    Arg::new("no-minimization")
                        .long("no-minimization")
                        .help("if present, disable block minimization")
                        .action(clap::ArgAction::SetTrue)
                        .default_value("false")
                        .required(false),
                )
                .arg(
                    Arg::new("failed-blocks")
                    .long("failed-blocks")
                    .help("directory to store failed blocks in")
                    .default_value("failed_blocks")
                    .required(false)
                ),
                Command::new("import-failed-blocks")
                .arg(
                    Arg::new("failed-blocks")
                    .long("failed-blocks")
                    .help("directory to store failed blocks in")
                    .default_value("failed_blocks")
                    .required(false)
                ),
            Command::new("new-config"),
            Command::new("geyser-stream")
            .arg(
                Arg::new("no-minimization")
                    .long("no-minimization")
                    .help("if present, disable block minimization")
                    .action(clap::ArgAction::SetTrue)
                    .default_value("false")
                    .required(false),
            )
            .arg(
                Arg::new("failed-blocks")
                .long("failed-blocks")
                .help("directory to store failed blocks in")
                .default_value("failed_blocks")
                .required(false)
            ),
            Command::new("backfiller")
            .about("block backfiller to covers gaps missed by geyser")
            .arg(
                Arg::new("no-minimization")
                    .long("no-minimization")
                    .help("if present, disable block minimization")
                    .action(clap::ArgAction::SetTrue)
                    .default_value("false")
                    .required(false),
            )
            .arg(
                Arg::new("failed-blocks")
                .long("failed-blocks")
                .help("directory to store failed blocks in")
                .default_value("failed_blocks")
                .required(false)
            ),
            Command::new("index-idls"),
            Command::new("index-programs"),
            Command::new("manual-idl-import")
            .about("manually import an idl into the database")
            .long_about("useful for programs that publish anchor idls offchain")
            .arg(
                Arg::new("input")
                .long("input")
                .help("file containing the idl")
            )
            .arg(
                Arg::new("program-id")
                .long("program-id")
                .help("program to associate this idl with")
            ),
            Command::new("fill-missing-slots")
            .about("fill missing slot information")
            .arg(
                Arg::new("limit")
                .long("limit")
                .help("number of blocks to fill at once")
                .value_parser(clap::value_parser!(i64))
            )
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
        Some(("download", dl)) => commands::download::start(dl, config_path).await,
        Some(("import-failed-blocks", ifb)) => commands::download::import_failed_blocks(ifb, config_path).await,
        Some(("new-config", _)) => commands::config::new_config(config_path).await,
        Some(("geyser-stream", gs)) => commands::download::stream_geyser_blocks(gs, config_path).await,
        Some(("backfiller", bf)) => commands::download::recent_backfill(bf, config_path).await,
        Some(("index-idls", _)) => commands::idl_indexer::index_idls(config_path).await,
        Some(("index-programs", _)) => commands::program_indexer::index_programs(config_path).await,
        Some(("manual-idl-import", mii)) => commands::idl_indexer::manual_idl_import(mii, config_path).await,
        Some(("fill-missing-slots", fms)) => commands::db::fill_missing_slots(fms, config_path).await,
        _ => Err(anyhow!("invalid subcommand")),
    }
}
