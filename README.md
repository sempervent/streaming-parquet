# maw

A high-performance Rust CLI for streaming and concatenating CSV and Parquet files.

## Features

- **Streaming Processing**: Memory-bounded processing of large files
- **Schema Unification**: Automatic type widening and column alignment
- **Parallel Processing**: Concurrent file reading with configurable concurrency
- **Multiple Formats**: Support for CSV and Parquet input/output
- **Resumable Operations**: State tracking for long-running operations
- **Progress Tracking**: Real-time progress bars and throughput metrics
- **Rolling Outputs**: Split output by size or row count

## Installation

### From Source

```bash
git clone https://github.com/sempervent/streaming-parquet
cd streaming-parquet
cargo build --release
```

### Using just (recommended)

```bash
just build
```

## Usage

### Basic Usage

```bash
# Concatenate CSV files
maw file1.csv file2.csv -o output.csv

# Convert CSV to Parquet
maw data/*.csv -o output.parquet --out-format parquet

# Process with compression
maw data/ -o output.parquet --compression zstd --zstd-level 3
```

### Advanced Usage

```bash
# Rolling outputs by size
maw data/ -o output-%04d.parquet --roll-by-bytes 1073741824

# Schema customization
maw data/ -o output.csv --columns a,b,c --rename old=new

# Resumable processing
maw data/ -o output.parquet --state state.json --resume
```

### Plan Mode

```bash
# See what would be processed
maw data/ --plan
```

## Performance Targets

- CSV→CSV: ≥ 150 MB/s aggregate
- CSV→Parquet (zstd level 3): ≥ 80 MB/s  
- Parquet→Parquet (same codec): ≥ 200 MB/s

## Architecture

- **Runtime**: `tokio` for async I/O + `rayon` for CPU work
- **Arrow/Parquet**: `arrow2` + `parquet2` for high-performance data processing
- **Streaming**: Bounded memory usage via chunked reading
- **Concurrency**: Parallel readers with single writer for ordering

## Development

### Setup

```bash
just dev-setup
```

### Testing

```bash
just test
```

### Benchmarking

```bash
just bench
```

### Linting

```bash
just lint
```

## Docker

```bash
# Build image
just docker-build

# Run with Docker
just docker-run --help
```

## License

MIT
