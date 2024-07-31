# sb-dl

**S**olana **B**lock **D**own**L**oader is a tool for persisting historical solana blocks, and streaming real-time blocks using geyster, persisting to postgres in the "Ui Encoding" format. By default blocks are minimized via removal of rewards and vote/consensus related information however this can be disabled.

# Usage

## Requirements

* Rust
* Postgresql
* Dependencies: `make, build-essential, libpq-dev, pkg-config, protobuf-compiler`

## Running

**Prepare Config**

You can init the configuration file with the following:

```shell
$> sb_dl new-config
```

Afterwards populate the db_url, and big table configuration as needed.

**Starting The Bitable Downloader**

There is no need to manually run database migrations, as this is done during the startup process for the downloader. To start use the following command:

```shell
$> sb_dl download --start <starting_block> --limit <max_blocks_to_index> [--no-minimization] --failed-blocks <failed_blocks_dir>
```

* `<starting_block>` is the block to begin indexing from
* `<max_blocks_to_index>` is the max number of blocks to index
* `--no-minimization` can be used to persist full block data which includes vote transactions
* `<failed_blocks_dir>` local filesystem directory to persist blocks which failed to be inserted into postgres

Blocks are downloaded sequentially beginning at `<starting_block>`.

**Starting Geyser Stream**

```shell
$> sb_dl geyser-stream --failed-blocks <failed_blocks_dir>
```

* `<failed_blocks_dir>` local filesystem directory to persist blocks which failed to be inserted into postgres

# Notes

## storage-bigtable

The `crates/storage-bigtable` folder is cloned from <https://github.com/solana-labs/solana> at commit `27eff8408b7223bb3c4ab70523f8a8dca3ca6645`. This is due to issues with cargo dependency resolution not allowing the crate to be imported from outside the `solana` repository, as well as the need to change function visibility modifiers.


## idls

The `idls` directory contains various IDLs that are not available on-chain. The naming format must be adhered to in order to facilitate bulk manual idl import. The file name of the idl needs to contains the program id, followed by `_X.json` where `X` can be any value.