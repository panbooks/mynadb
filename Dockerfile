# Build stage
FROM rust:1.75-bookworm AS builder

WORKDIR /app

# Copy dependency files first for better caching
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./

# Create dummy src files for dependency compilation
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn lib() {}" > src/lib.rs

# Build dependencies only
RUN cargo build --release && rm -rf src

# Copy actual source code
COPY src ./src
COPY benches ./benches

# Touch files to invalidate cache and rebuild
RUN touch src/main.rs src/lib.rs

# Build the actual application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r mynadb && useradd -r -g mynadb mynadb

# Create directories
RUN mkdir -p /data/mynadb/logs /etc/mynadb && \
    chown -R mynadb:mynadb /data/mynadb /etc/mynadb

# Copy the binary
COPY --from=builder /app/target/release/mynadb /usr/local/bin/mynadb

# Switch to non-root user
USER mynadb

# Expose ports
EXPOSE 7000 7001 7002

# Set working directory
WORKDIR /data/mynadb

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:7000/health || exit 1

# Default command
ENTRYPOINT ["/usr/local/bin/mynadb"]
CMD ["-c", "/etc/mynadb/config.toml"]
