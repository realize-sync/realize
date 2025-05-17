# This docker container runs the daemon
#
# Build it with:
#  docker build -t realize-daemon -f docker/Dockerfile $PWD

# --- 1. Build stage
FROM rust:1.87.0-alpine3.21 AS builder
RUN apk add gcc make musl-dev
WORKDIR /usr/src/app

# --- 1.1 fetch dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

# --- 1.2 build application
RUN rm src/main.rs
COPY ./src ./src
RUN cargo build --release --bin realized

# --- 2. Runtime stage
FROM alpine:3.21 AS runtime
#RUN apk add --no-cache ca-certificates openssl

# --- 2.1 Configuration
ENV PORT=9771
ENV UID=500
ENV GID=500
ENV ADDRESS=0.0.0.0:9771

# Server private key
ENV PRIVKEY=/config/private.key

# Config file
ENV CONFIG=/config/config.yaml

# --- 2.2 entry point
COPY ./docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# --- 2.3 environment
WORKDIR /app
RUN addgroup \
    --gid "$GID" \
    "realized" \
&&  adduser \
    --disabled-password \
    --gecos "" \
    --home "$(pwd)" \
    --ingroup "realized" \
    --no-create-home \
    --uid "$UID" \
    realized \
&& chown "$GID:$UID" .

# --- 2.4 binary
COPY --from=builder /usr/src/app/target/release/realized /usr/local/bin/realized

# --- 2.5 run
EXPOSE $PORT
USER realized
ENTRYPOINT [ "/entrypoint.sh" ]