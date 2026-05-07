FROM node:22-trixie-slim AS builder
WORKDIR /app
COPY backend/package.json backend/package-lock.json ./
RUN npm ci
COPY backend/tsconfig.json ./
COPY backend/src/ ./src/
RUN npx tsc

FROM node:22-trixie-slim
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./
EXPOSE 3000
# SERVICE_ENTRY selects which compiled entry point to run.
# Default = socketio/server. Set to bench-runner/server for the bench runner.
CMD ["sh", "-c", "node dist/${SERVICE_ENTRY:-socketio/server}.js"]
