# Datassert

### By Skye Lane Goetz

Datassert is a high-performance CLI for building a DuckDB-backed assertion store from NCATS Translator BABEL export files, with a focus on fast local builds and simple command-driven workflows.

## Quick Start

```bash
# Install CLI from GitHub
go install github.com/SkyeAv/datassert@latest

# Verify install
datassert --help
```

## Build Command

```bash
# Build a Datassert database (downloads BABEL data automatically)
datassert build
```

The build command automatically downloads BABEL exports from RENCI (`https://stars.renci.org/var/babel_outputs`), processes them, and produces sharded DuckDB databases.

### Flags

| Flag | Required | Default | Description |
| --- | --- | --- | --- |
| `--skip-downloads` / `-s` | No | `false` | Skip the BABEL download phase (use previously downloaded files) |
| `--use-existing-parquets` / `-p` | No | `false` | Use existing Parquet files to rebuild DuckDB databases |

### Data Pipeline

1. **Download** -- BABEL class and synonym files are downloaded from RENCI and split into LZ4-compressed NDJSON chunks under `./datassert/downloads/`.
2. **Lookup** -- Class files (`*.ndjson.lz4`) are read to build an in-memory equivalent-identifier lookup.
3. **Parquet Staging** -- Synonym files are processed with the lookup, quality-controlled, and written as sharded Parquet files to `./datassert/parquets/`.
4. **DuckDB Generation** -- Parquet files are loaded into 12 sharded DuckDB databases under `./datassert/data/`.

### Output Artifacts

- 12 sharded DuckDB databases are written to `./datassert/data/{0..11}.duckdb`.
- Each shard contains `SOURCES`, `CATEGORIES`, `CURIES`, and `SYNONYMS` tables, deduplicated, sorted, and indexed for query performance.
- Staging Parquet files are written to `./datassert/parquets/{0..11}/`.

### Examples

```bash
# Full build (download, process, and generate databases)
datassert build

# Skip downloads if BABEL files were already fetched
datassert build --skip-downloads

# Rebuild DuckDB databases from existing Parquet files
datassert build --use-existing-parquets
```

### Runtime Behavior

- Displays progress bars for download, class lookup, synonym processing, and DuckDB build phases.
- Uses 90% of available CPUs for concurrent processing.
- Downloads are retried up to 3 times on failure with a 10-second backoff.
- All working files are stored under `./datassert/`.

## Maintainer

Skye Lane Goetz
