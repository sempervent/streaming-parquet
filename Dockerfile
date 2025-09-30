# Multi-stage build for maw
FROM rust:1.80-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    musl-dev \
    zstd-dev \
    openssl-dev \
    pkgconfig

# Set working directory
WORKDIR /app

# Copy Cargo files
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY benches ./benches
COPY tests ./tests

# Build with optimizations
RUN cargo build --release --target x86_64-unknown-linux-musl

# Runtime stage
FROM alpine:3.20

# Install runtime dependencies
RUN apk add --no-cache \
    libgcc \
    zstd-libs

# Create app user
RUN addgroup -g 1000 maw && \
    adduser -D -s /bin/sh -u 1000 -G maw maw

# Copy binary from builder
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/maw /usr/local/bin/maw

# Set permissions
RUN chmod +x /usr/local/bin/maw

# Switch to non-root user
USER maw

# Set entrypoint
ENTRYPOINT ["maw"]

# Default command
CMD ["--help"]
