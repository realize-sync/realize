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
WORKDIR /app
# Install runtime dependencies (openssl, ca-certificates, etc.)
RUN apk add --no-cache ca-certificates openssl

# Copy the realized binary from the builder
COPY --from=builder /usr/src/app/target/release/realized /usr/local/bin/realized

# Copy any required runtime files (config, resources, etc.)

ENTRYPOINT ["/usr/local/bin/realized"]
CMD ["--help"] 