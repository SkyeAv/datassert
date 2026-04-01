# Agent Development Guide

Guidelines for agentic coding agents working on the Datassert codebase — a high-performance Go CLI that processes Babel export files into sharded DuckDB databases.

## Build & Run

```bash
go build -o datassert                              # Compile binary
go run main.go                                     # Run without compiling
go run main.go build --babel-dir <dir>             # Build pipeline (default --db-dir: ".")
```

The `build` command also accepts: `--db-dir`, `--batch-size` (default 100000), `--buffer-size` (default 2048), `--class-cpu-fraction` (default 2), `--synonym-cpu-fraction` (default 4).

## Testing

```bash
go test ./...                      # All tests
go test -v ./...                   # Verbose
go test -v ./cmd/                  # Specific package
go test -run TestFoo ./cmd/        # Single test by name
go test -cover ./...               # With coverage
```

No test files exist yet. When adding tests, place `*_test.go` files alongside the code they test (same package).

## Linting & Formatting

```bash
go fmt ./...                       # Format all
go vet ./...                       # Static analysis
```

## Project Structure

```
Datassert/
├── main.go           # Entry point → cmd.Execute()
├── go.mod            # Module: github.com/SkyeAv/datassert (Go 1.25.7)
├── cmd/
│   ├── root.go       # Root cobra command ("datassert")
│   └── build.go      # build subcommand + all pipeline logic (~700 lines)
```

All application logic lives in `cmd/build.go`. The `cmd` package uses `package cmd`.

## Code Style

### Imports

Standard library (alphabetical), blank line, third-party (alphabetical). Each import on its own line:

```go
import (
	"database/sql"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/bytedance/sonic"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/spf13/cobra"
)
```

### Naming

| Element | Convention | Example |
|---------|-----------|---------|
| Types/Structs | PascalCase | `ClassLookup`, `CurieCounter` |
| Interfaces | PascalCase | `ParquetTable` |
| Functions | PascalCase (exported), camelCase (unexported) | `WriteParquet`, `checkError` |
| Variables | camelCase | `batchSize`, `babelDir` |
| Constants | camelCase or PascalCase | `nShards`, `badPrefixes` |

### Error Handling

No `error` return values — use `checkError(code, err)` / `throwError(code, err)` with uint8 site codes, which call `log.Fatalf`. Do not introduce `error` return patterns.

```go
checkError(3, err)  // code identifies the call site
```

### Struct Tags

- **JSON**: `snake_case` field names (`json:"equivalent_identifiers"`)
- **Parquet**: `ALL_CAPS` with optional tags (`parquet:"TAXON_ID,optional"`)

### Concurrency

**Sharded structures** — use `[nShards]` (16 shards) with per-shard mutexes and `_pad [40]byte` to avoid false sharing:

```go
shards [nShards]struct {
    mu   sync.Mutex
    m    map[uint64]uint32
    _pad [40]byte
}
```

**Bounded parallelism** — prefer `errgroup.Group` with `g.SetLimit(n)` over raw `sync.WaitGroup`.

**Producer-consumer** — buffered channels with a goroutine that decodes into the channel and closes on EOF.

**Lock-free maps** — `sync.Map` for read-heavy concurrent maps (e.g., `CategoryMap`).

**Shared counters** — `atomic.Uint32`.

### Resource Management

Always `defer Close()` immediately after acquisition:

```go
f := yieldReader(fileName)
defer f.Close()
zr := yieldDecoder(f)
defer zr.Close()
```

### JSON Decoding

Use `sonic.ConfigDefault.NewDecoder` for streaming:

```go
decoder := sonic.ConfigDefault.NewDecoder(reader)
for {
    var record RecordType
    if err := decoder.Decode(&record); err == io.EOF { break }
    checkError(code, err)
}
```

### Generics

Constrain with the `ParquetTable` interface union:

```go
type ParquetTable interface {
    CuriesTable | SynonymsTable | CategoriesTable | SourcesTable
}
```

### Cobra CLI

Package-level command var, `init()` for registration and flags, `MarkFlagRequired()`.

### Hashing & Sharding

All sharding uses `xxhash.Sum64` modulo `nShards` (16). Use `hashAndShard` helper.

### DuckDB

- Open: `sql.Open("duckdb", path)` with blank import `_ "github.com/duckdb/duckdb-go/v2"`
- Load: `CREATE TABLE AS SELECT * FROM read_parquet('glob')`
- Configure: `SET temp_directory`, `SET preserve_insertion_order = false`
- After loading: `CREATE INDEX`, `VACUUM ANALYZE`

## Key Dependencies

| Dependency | Purpose |
|-----------|---------|
| `github.com/spf13/cobra` | CLI framework |
| `github.com/duckdb/duckdb-go/v2` | DuckDB driver (blank import) |
| `github.com/parquet-go/parquet-go` | Parquet file I/O |
| `github.com/bytedance/sonic` | Fast JSON streaming parser |
| `github.com/klauspost/compress/zstd` | Zstandard decompression |
| `github.com/cespare/xxhash/v2` | XXHash for sharding |
| `github.com/gosuri/uiprogress` | Progress bars |
| `golang.org/x/sync` | errgroup for bounded concurrency |
