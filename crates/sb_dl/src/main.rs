pub mod commands;

use {
    anyhow::{anyhow, Result},
    clap::{value_parser, Arg, ArgMatches, Command},
    sb_dl::utils::init_log,
};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Command::new("sb_dl")
        .about("solana bigtable downloader")
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
                    .required(false)
                ),
            Command::new("new-config"),
        ])
        .get_matches();

    let config_path = matches.get_one::<String>("config").unwrap();
    let log_level = matches.get_one::<String>("log-level").unwrap();
    let log_file = matches.get_one::<String>("log-file").unwrap();

    init_log(log_level, log_file);

    process_matches(&matches, config_path).await
}

async fn process_matches(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    match matches.subcommand() {
        Some(("download", dl)) => commands::download::start(dl, config_path).await,
        Some(("new-config", _)) => commands::config::new_config(config_path).await,
        _ => Err(anyhow!("invalid subcommand")),
    }
}
