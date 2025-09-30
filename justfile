set shell := ["bash", "-cu"]

# Build the project
build:
    cargo build --release

# Run with arguments
run *ARGS:
    cargo run --release -- {{ARGS}}

# Run tests
test:
    cargo test --all

# Run benchmarks
bench:
    cargo bench || true

# Lint the code
lint:
    cargo clippy -- -D warnings

# Format the code
fmt:
    cargo fmt

# Generate shell completions
completions:
    mkdir -p dist
    cargo run --release -- completions bash > dist/maw.bash
    cargo run --release -- completions zsh  > dist/_maw
    cargo run --release -- completions fish > dist/maw.fish

# Build Docker image
docker-build:
    docker build -t maw:latest .

# Run with Docker
docker-run *ARGS:
    docker run --rm -v "$PWD:/work" -w /work maw:latest {{ARGS}}

# Clean build artifacts
clean:
    cargo clean

# Install completions (requires sudo)
install-completions:
    sudo cp dist/maw.bash /etc/bash_completion.d/
    sudo cp dist/_maw /usr/local/share/zsh/site-functions/
    sudo cp dist/maw.fish /usr/local/share/fish/completions/

# Development setup
dev-setup:
    rustup component add clippy rustfmt
    cargo install cargo-criterion

# Check everything
check: lint test
    @echo "All checks passed!"

# Release build with optimizations
release:
    cargo build --release --target x86_64-unknown-linux-musl
    strip target/x86_64-unknown-linux-musl/release/maw
