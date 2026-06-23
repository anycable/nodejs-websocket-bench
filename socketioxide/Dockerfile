# Build the socketioxide bench server.
#
# Same shape as backend/Dockerfile: builder stage produces a binary,
# runtime stage is a thin slim image. Build the image from this directory:
#
#   docker build -t socketioxide-server .
#
# Or via railway.toml deploy from this directory (--path-as-root).

# socketioxide 0.18.4 / engineioxide 0.17.5 set MSRV 1.94. Use the latest
# stable 1.x so the image clears it (built locally on 1.95).
FROM rust:1-slim-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
# Copy the lock too so the image builds the exact dependency versions we
# resolved and tested locally (--locked fails the build if they drift).
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/socketioxide-bench-server /usr/local/bin/socketioxide-bench-server
EXPOSE 3000
CMD ["socketioxide-bench-server"]
