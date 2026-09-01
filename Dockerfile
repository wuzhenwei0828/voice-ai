# syntax=docker/dockerfile:1

FROM rust:1.97-bookworm AS builder

WORKDIR /workspace

# Keep manifests in their own layers so Docker can reuse them when the source changes.
COPY Cargo.toml Cargo.lock ./
COPY crates/voice-proto/Cargo.toml crates/voice-proto/Cargo.toml
COPY crates/voice_server/Cargo.toml crates/voice_server/Cargo.toml
COPY crates/ws_payload_helper/Cargo.toml crates/ws_payload_helper/Cargo.toml

# Cargo needs a source tree for workspace dependency resolution before building.
COPY crates/voice-proto/src crates/voice-proto/src
COPY crates/voice_server/src crates/voice_server/src
COPY crates/voice_server/static crates/voice_server/static
COPY crates/ws_payload_helper/src crates/ws_payload_helper/src

RUN cargo build --release --locked --package voice_server --bin voice_server

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin voice

WORKDIR /app

COPY --from=builder /workspace/target/release/voice_server /usr/local/bin/voice_server
COPY --from=builder /workspace/crates/voice_server/static /app/static
COPY --from=builder /workspace/crates/voice_server/src/config/config.yaml.template /app/config/config.yaml.template

RUN mkdir -p /app/config /app/logs \
    && chown -R voice:voice /app

ENV VOICE_CONFIG=/app/config/config.yaml \
    VOICE_WEB_STATIC_DIR=/app/static \
    VOICE_LOG_FILE=/app/logs/voice-server.log \
    HTTP_PORT=8081

USER voice

EXPOSE ${HTTP_PORT}

ENTRYPOINT ["/usr/local/bin/voice_server"]
