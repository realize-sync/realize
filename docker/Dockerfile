# This docker container runs the daemon
#
# Build it with:
#  docker build -t realize-daemon -f docker/Dockerfile $PWD

# --- 1. Build stage
FROM rust:1.87.0-alpine3.21 AS builder
RUN apk add gcc make musl-dev
WORKDIR /usr/src/app

# --- 1.1 copy source
COPY ./Cargo.lock Cargo.lock
COPY ./Cargo.toml Cargo.toml
COPY ./crate crate

# --- 1.2 build application
RUN cargo build -p realize-daemon --release

# --- 2. Runtime stage
FROM alpine:3.21 AS runtime

# --- 2.1 Configuration
ENV ADDRESS=0.0.0.0:9771
ENV METRICS_ADDRESS=0.0.0.0:9772

# Server private key
ENV PRIVKEY=/config/private.key

# Config file
ENV CONFIG=/config/config.yaml

# Configure logging
ENV RUST_LOG=warn,realize_lib::transport::tcp=info,realize_lib::security=debug,realize_lib::server=info,realize_daemon

# --- 2.2 entry point
COPY ./docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# --- 2.3 environment
WORKDIR /app

# --- 2.4 binary
COPY --from=builder /usr/src/app/target/release/realize-daemon /usr/local/bin/realized

# --- 2.5 run
EXPOSE 9771
EXPOSE 9772
ENTRYPOINT [ "/entrypoint.sh" ]