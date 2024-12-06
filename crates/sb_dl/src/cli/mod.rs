use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "sb_dl", about = "solana block downloader")]
pub struct Cli {
    #[arg(long, default_value = "info", help = "log verbosity to use")]
    pub log_level: String,

    #[arg(long, default_value = "", help = "optionally output logs to this file")]
    pub log_file: String,

    #[arg(long, default_value = "config.yaml")]
    pub config: String,

    // Global flags
    #[arg(long, global = true, default_value = "false")]
    pub no_minimization: bool,

    #[arg(long, global = true, default_value = "failed_blocks")]
    pub failed_blocks_dir: String,

    #[arg(long, global = true, default_value = "4")]
    pub threads: u32,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(about = "service management commands")]
    Services {
        #[command(subcommand)]
        command: ServicesCommands,
    },

    #[command(about = "initialize a new config file")]
    NewConfig,

    #[command(
        about = "manually import an idl into the database",
        long_about = "useful for programs that publish anchor idls offchain"
    )]
    ManualIdlImport {
        #[arg(long, help = "file containing the idl")]
        input: String,

        #[arg(long, help = "program to associate this idl with")]
        program_id: String,
    },

    #[command(about = "generate transfer graph for a single tx")]
    CreateTransferGraphForTx {
        #[arg(long, help = "slot number to fetch tx from")]
        slot_number: i64,

        #[arg(long, help = "tx to generate graph for")]
        tx_hash: String,
    },

    #[command(about = "generates ordered transfers for an entire block")]
    CreateOrderedTransfersForBlock {
        #[arg(long, help = "slot number to fetch tx from")]
        slot_number: i64,
    },
    #[command(about = "find the ending block for a gap")]
    FindGapEnd {
        #[arg(long, help = "starting number to assume a gap for")]
        gap_start: i64,
    },
}

#[derive(Subcommand, Clone)]
pub enum ServicesCommands {
    #[command(about = "download historical block data using bigtable")]
    BigtableDownloader {
        #[arg(long)]
        start: Option<u64>,

        #[arg(long, help = "max number of slots to index")]
        limit: Option<u64>,

        #[arg(from_global)]
        no_minimization: bool,

        #[arg(from_global)]
        failed_blocks_dir: String,

        #[arg(from_global)]
        threads: u32,
    },

    #[command(about = "block backfiller to covers gaps missed by geyser")]
    Backfiller {
        #[arg(from_global)]
        no_minimization: bool,

        #[arg(from_global)]
        failed_blocks_dir: String,

        #[arg(from_global)]
        threads: u32,
    },

    #[command(about = "stream blocks in real-time using geyser")]
    GeyserStream {
        #[arg(from_global)]
        no_minimization: bool,

        #[arg(from_global)]
        failed_blocks_dir: String,

        #[arg(from_global)]
        threads: u32,
    },

    #[command(about = "index anchor idl accounts")]
    IndexIdls,

    #[command(about = "index deployed programs")]
    IndexPrograms,

    #[command(about = "starts api used to returned transfer flow data")]
    TransferFlowApi {
        #[arg(long, help = "url to expose the api on")]
        listen_url: String,
    },

    #[command(about = "used to repair gaps in block coverage")]
    RepairGaps {
        #[arg(long)]
        starting_number: i64,

        #[arg(from_global)]
        failed_blocks_dir: String,

        #[arg(from_global)]
        threads: u32,
    },

    #[command(about = "transfer parsing service to push decoded transfers into elasticsearch")]
    TransferParser {
        #[arg(long, help = "starting block")]
        start: i64,

        #[arg(long, help = "ending block")]
        end: i64,

        #[arg(from_global)]
        block_table_choice: String,

        #[arg(
            long,
            help = "if present, use remote database",
            default_value = "false"
        )]
        use_remotedb: bool,
    },

    #[command(about = "index squads multisigs")]
    SquadsIndexer {
        #[arg(
            long,
            help = "duration in seconds to fetch data",
            default_value = "300"
        )]
        frequency: u64,
    },

    #[command(about = "import failed blocks")]
    ImportFailedBlocks {
        #[arg(from_global)]
        failed_blocks_dir: String,
    },
}
