pub mod config_commands;
pub mod download_command;

use {
    anyhow::{anyhow, Context, Result}, clap::{value_parser, Arg, ArgMatches, Command}, sb_dl::logger::{init_log, LogOpts},
};

#[tokio::main]
async fn main() -> Result<()> {
    init_log(LogOpts {
        level: "info".to_string(),
        file: "".to_string(),
        console: false
    });
    let matches = Command::new("sb_dl")
    .about("solana bigtable downloader")
    .arg(
        Arg::new("config")
        .long("config")
        .default_value("config.yaml")
    )
    .subcommands(vec![
        Command::new("download")
        .arg(
            Arg::new("start")
            .long("start")
            .value_parser(value_parser!(u64))
            .required(false)
        )
        .arg(
            Arg::new("limit")
            .long("limit")
            .help("max number of slots to index")
            .value_parser(value_parser!(u64))
            .required(false)
        ),
        Command::new("new-config"),
    ]).get_matches();
    let config_path = matches.get_one::<String>("config").unwrap();
    process_matches(&matches, config_path).await
}

async fn process_matches(
    matches: &ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    match matches.subcommand() {
        Some(("download", dl)) => {
            download_command::start(dl, config_path).await
        }
        Some(("new-config", _)) => {
            config_commands::new_config(config_path).await
        }
        _ => Err(anyhow!("invalid subcommand"))
    }
}