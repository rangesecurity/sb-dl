# sb-dl

**S**olana **B**igtable **D**own**L**oader is a library, and cli for downloading historical transaction data from a compatible big table deployments, and persisting this data into Postgres in the "Ui Encoding" format. By default blocks are minimized via removal of rewards and vote/consensus related information however this can be disabled.


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

**Starting The Downloader**

There is no need to manually run database migrations, as this is done during the startup process for the downloader. To start use the following command:

```shell
$> sb_dl download --start <starting_block> --limit <max_blocks_to_index> [--no-minimization]
```

* `<starting_block>` is the block to begin indexing from
* `<max_blocks_to_index>` is the max number of blocks to index
* `--no-minimization` can be used to persist full block data which includes vote transactions

Blocks are downloaded sequentially beginning at `<starting_block>`.


# Notes

## storage-bigtable

The `crates/storage-bigtable` folder is cloned from https://github.com/solana-labs/solana at commit `27eff8408b7223bb3c4ab70523f8a8dca3ca6645`. This is due to issues with cargo dependency resolution not allowing the crate to be imported from outside the `solana` repository, as well as the need to change function visibility modifiers.