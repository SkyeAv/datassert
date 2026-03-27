# Datassert

### By Skye Lane Goetz

Datassert is a high-performance CLI for building a DuckDB-backed assertion store from Babel export files, with a focus on fast local builds and simple command-driven workflows.

## Quick Start

```bash
# Install CLI from GitHub
go install github.com/SkyeAv/datassert@latest

# Verify install
datassert --help
```

## Build Command

```bash
# Build a Datassert database from Babel exports
datassert build --babel-dir /path/to/babel
```

### Flags

| Flag | Required | Default | Description |
| --- | --- | --- | --- |
| `--babel-dir` | Yes | N/A | Directory containing Babel `*Class.ndjson.zst` and `*Synonyms.ndjson.zst` files |
| `--db-path` | No | `./.datassert` | Base output path for sharded DuckDB databases |
| `--batch-size` | No | `50000` | Number of records per Parquet batch |
| `--buffer-size` | No | `2048` | Channel buffer size for synonym file processing |
| `--class-cpu-fraction` | No | `2` | Divisor of `NumCPU()` for class file goroutines |
| `--synonym-cpu-fraction` | No | `4` | Divisor of `NumCPU()` for synonym file goroutines |

### Input Expectations

- `--babel-dir` is scanned for files matching `*Class.ndjson.zst` and `*Synonyms.ndjson.zst`.
- File matching is non-recursive (top-level of the provided directory).

### Output Artifacts

- Staging Parquet files are written to `./.parquet-store/`.
- 16 sharded DuckDB databases are written to `<db-path>-shard{0..15}.duckdb`.
- Each shard contains `SOURCES`, `CATEGORIES`, `CURIES`, and `SYNONYMS` tables, sorted and indexed for query performance.

### Examples

```bash
# Use defaults for db path and batch size
datassert build --babel-dir ./babel-exports

# Write databases to a custom base path (produces ./data/mydb-shard{0..15}.duckdb)
datassert build --babel-dir ./babel-exports --db-path ./data/mydb

# Tune Parquet batch size and concurrency
datassert build --babel-dir ./babel-exports --batch-size 100000 --class-cpu-fraction 1
```

### Runtime Behavior

- Displays progress bars for class, synonym, and DuckDB build phases.
- Uses CPU-based concurrency with configurable fractions (`NumCPU()/class-cpu-fraction` and `NumCPU()/synonym-cpu-fraction`).

## Maintainer

Skye Lane Goetz
