# This docker container runs the daemon
#
# Build it with:
#  docker build -t realize-daemon -f docker/Dockerfile $PWD

# Use a Rust base image with Cargo installed
FROM rust:1.86.0-alpine3.20 AS builder
RUN apk add gcc make musl-dev

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Create an empty src directory to trick Cargo into thinking it's a valid Rust project
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build the dependencies without the actual source code to cache dependencies separately
RUN cargo build --release

# Now copy the source code
RUN rm src/main.rs
COPY ./src ./src

# Build the application
RUN cargo build --release --bin realized

# --- Runtime stage ---
FROM alpine:3.20
RUN apk add --no-cache ca-certificates openssl

COPY ./docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Env setup
WORKDIR /app
ENV UID=500
ENV GID=500

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

EXPOSE 9771
ENV ADDRESS=0.0.0.0:9771

# Server private key
ENV PRIVKEY=/config/private.key

# Server configuration
ENV CONFIG=/config/config.yaml

# Copy the realized binary from the builder
COPY --from=builder /usr/src/app/target/release/realized /usr/local/bin/realized

USER realized
ENTRYPOINT [ "/entrypoint.sh" ]