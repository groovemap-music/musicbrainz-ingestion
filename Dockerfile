# Build stage
FROM rust:1.98-slim@sha256:17d1ba895198f9934c6314ec5346a0d5115372f3243390c3d731e242f35c2f27 AS builder

# Install build dependencies
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

COPY Cargo.lock ./
COPY Cargo.toml ./
COPY benches ./benches

# Create dummy main to cache dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release --locked && \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build the application
RUN touch src/main.rs && \
    cargo build --release --locked

# Runtime stage
FROM debian:13-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

# Build arguments for configurable UID/GID (must match the compose `user:` override)
ARG BUILD_DATE
ARG BUILD_VERSION=0.1.0
ARG VCS_REF
ARG UID=1000
ARG GID=1000

LABEL org.opencontainers.image.title="catalog-ingestion" \
      org.opencontainers.image.description="GrooveMap Discogs and MusicBrainz catalog ingestion and event publishing" \
      org.opencontainers.image.authors="Robert Wlodarczyk <robert@simplicityguy.com>" \
      org.opencontainers.image.url="https://groovemap.music" \
      org.opencontainers.image.documentation="https://github.com/groovemap-music/catalog-ingestion/blob/main/README.md" \
      org.opencontainers.image.source="https://github.com/groovemap-music/catalog-ingestion" \
      org.opencontainers.image.vendor="GrooveMap" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.base.name="docker.io/library/debian:13-slim"

# Install runtime dependencies
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3t64 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user with a fixed UID/GID so file ownership matches the
# runtime user regardless of any `useradd -r` system-UID auto-allocation.
RUN groupadd -r -g ${GID} extractor && useradd -r -l -u ${UID} -g extractor extractor

# Create necessary directories
RUN mkdir -p /discogs-data /musicbrainz-data /logs && \
    chown -R extractor:extractor /discogs-data /musicbrainz-data /logs

# Copy binary from builder
COPY --from=builder /app/target/release/extractor /usr/local/bin/extractor

# Switch to non-root user
# UID/GID build arguments resolve to numeric IDs; DL3066 cannot infer their values.
# hadolint ignore=DL3066
USER ${UID}:${GID}

# Set environment variables
ENV LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD ["curl", "-f", "http://localhost:8000/health"]

# Expose health port
EXPOSE 8000

# Run the application
ENTRYPOINT ["extractor"]
