# Stage 1: build the AnyCable benchi stress_publications binary from
# Vladimir's dev/benchi branch on anycable/anycable. This gives the
# bench-runner an in-process AnyCable throughput probe (embedded server,
# loopback httptest.Server) for apples-to-apples comparison with the
# Socket.io / uWS in-process emit() / publish() tests.
FROM golang:1.26-trixie AS benchi
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch dev/benchi https://github.com/anycable/anycable.git /src/anycable
WORKDIR /src/anycable/etc/benchi
RUN CGO_ENABLED=0 go build -o /out/stress_publications ./scenarios/stress_publications

# Stage 2: build the Node/TS bench-runner
FROM node:22-trixie-slim AS builder
WORKDIR /app
COPY backend/package.json backend/package-lock.json ./
RUN npm ci
COPY backend/tsconfig.json ./
COPY backend/src/ ./src/
RUN npx tsc

# Stage 3: runtime image — Node service + the Go binary
FROM node:22-trixie-slim
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./
COPY --from=benchi /out/stress_publications /usr/local/bin/stress_publications
EXPOSE 3000
# SERVICE_ENTRY selects which compiled entry point to run.
# Default = socketio/server. Set to bench-runner/server for the bench runner.
CMD ["sh", "-c", "node dist/${SERVICE_ENTRY:-socketio/server}.js"]
