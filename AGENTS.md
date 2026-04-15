# AGENTS.md — Datassert

Guidance for agentic coding agents working in this repository.

## Project Overview

Datassert is a Go CLI that builds a sharded DuckDB-backed assertion store from NCATS Translator BABEL export files. It uses Cobra for CLI commands, sonic for JSON, parquet-go for Parquet I/O, and DuckDB for the final database output.

- **Module:** `github.com/SkyeAv/datassert`
- **Go version:** 1.25.7
- **Structure:** `main.go` (entrypoint) → `cmd/` (Cobra commands and all business logic)

## Build / Run / Test Commands

```bash
# Build the CLI binary
go build -o datassert .

# Install globally
go install github.com/SkyeAv/datassert@latest

# Run the CLI
go run . build
go run . build --skip-downloads
go run . build --use-existing-parquets

# Run all tests
go test ./...

# Run tests for a single package
go test ./cmd

# Run a single test by name
go test ./cmd -run TestFunctionName

# Run a single test with verbose output
go test ./cmd -run TestFunctionName -v

# Vet (static analysis)
go vet ./...

# Tidy dependencies
go mod tidy
```

No test files exist yet. When adding tests, follow standard Go conventions: create `*_test.go` files in the same package, use `testing.T`, and name test functions `TestXxx`.

No linter config (golangci-lint, etc.) is present. Run `go vet` before committing.

## Code Style Guidelines

### Imports

- Group imports into two blocks separated by a blank line: standard library first, then third-party packages.
- Use `_` imports for side-effect drivers (e.g., `_ "github.com/duckdb/duckdb-go/v2"`).
- No project-internal imports (single-module, flat structure).

```go
import (
    "fmt"
    "os"
    "sync"

    "github.com/spf13/cobra"
    "golang.org/x/sync/errgroup"
)
```

### Formatting

- Use `gofmt` / `goimports` formatting (tabs for indentation).
- Run `go vet` before committing to catch common mistakes.

### Naming Conventions

- **Packages:** lowercase, single word (`cmd`).
- **Functions:** camelCase; unexported (lowercase first letter) for package-private. Exported only when needed by external callers.
- **Types/Structs:** PascalCase for exported, camelCase for unexported.
- **Constants:** camelCase (not SCREAMING_SNAKE_CASE). Examples: `shards`, `maxRecords`, `version`, `renci`.
- **Variables:** camelCase. Explicit `var` declarations are used at file scope: `var x []string = []string{}`.
- **Struct fields:** PascalCase (exported) with struct tags.

### Type Declarations

- Use explicit type on `var` declarations at file scope: `var hardcodedSources []sourcesSchema = []sourcesSchema{...}`.
- Use `:=` short declarations inside functions.
- Struct tags use double quotes: `json:"curie"`, `parquet:"CURIE_ID"`, `parquet:"TAXON_ID,optional"`.

### Error Handling

- Use `log.Fatal(err)` for unrecoverable errors in CLI/business logic.
- Return `error` from functions that can fail non-fatally (e.g., `downloadAndSplit`, `nextOpenChunk`).
- No custom error types or error wrapping currently.
- Always check errors; never silently ignore them except in cleanup paths.

### Concurrency Patterns

- Use `errgroup.Group` with `g.SetLimit(n)` for bounded parallelism.
- Use `sync.Mutex` / `sync.RWMutex` for fine-grained locking on shared maps.
- Use `atomic.Uint32` for lock-free counters.
- Use `xsync.MapOf` for concurrent-safe maps.
- Shard-locked data structures include padding to avoid false sharing (see `curieCounter`, `lookup`).
- CPU limit is calculated as `runtime.NumCPU() * 9 / 10` (90% of available cores).

### Functions and Logic

- Keep functions focused and flat. No deep nesting.
- Recursive helpers are acceptable (e.g., `clean()` recursively strips quotes/whitespace).
- Helper functions use descriptive names: `tokenQC`, `qcMultipleTokens`, `applyNLPLevel`.
- Use `slices.Sort` + `slices.Compact` for deduplication.

### Constants and Configuration

- Directory paths and version strings are `const` at file scope.
- SQL configuration (DuckDB settings) and index DDL are `var` slices at file scope.
- Shard count is a typed constant: `const shards uint = 10`.

### Comments

- Sparse; prefer self-documenting code and names.
- Use `//` inline comments only for non-obvious behavior (e.g., buffer size rationale).
- No doc comments on most functions; add them for exported or complex functions.

## Architecture Notes

- **Pipeline stages:** Download → In-Memory Lookup → Parquet Staging → DuckDB Generation.
- **Sharding:** All data is sharded by xxhash of key strings into 10 shards (`getShard`).
- **CLI:** Cobra with `cmd/root.go` (root command) and `cmd/build.go` (build subcommand with flags).
- **Data flow:** BABEL NDJSON → LZ4-compressed chunks → Parquet files → DuckDB databases.
- **Output:** `./datassert/data/{0..9}.duckdb` with `SOURCES`, `CATEGORIES`, `CURIES`, `SYNONYMS` tables.

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `spf13/cobra` | CLI framework |
| `bytedance/sonic` | Fast JSON decoding |
| `duckdb/duckdb-go/v2` | DuckDB driver |
| `parquet-go/parquet-go` | Parquet file I/O |
| `pierrec/lz4/v4` | LZ4 compression/decompression |
| `cespare/xxhash/v2` | Fast hashing for sharding |
| `gosuri/uiprogress` | Progress bars |
| `puzpuzpuz/xsync/v3` | Concurrent maps |
| `golang.org/x/sync` | `errgroup` for bounded concurrency |
