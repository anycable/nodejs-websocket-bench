# Build the socketioxide bench server.
#
# Same shape as backend/Dockerfile: builder stage produces a static binary,
# runtime stage is a thin slim image. Build the image from the repo root:
#
#   docker build -f socketioxide/Dockerfile -t socketioxide-server .
#
# Or via railway.toml deploy from this directory.

FROM rust:1.83-slim AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml ./
COPY src/ ./src/
RUN cargo build --release \
    && strip target/release/socketioxide-bench-server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/socketioxide-bench-server /usr/local/bin/socketioxide-bench-server
EXPOSE 3000
CMD ["socketioxide-bench-server"]
