# Pulls the AnyCable Pro binary from the private GitHub release at
# anycable/anycable-go-pro-releases. Railway needs a GITHUB_TOKEN service
# variable (PAT or fine-grained token with read access to that repo) — it's
# wired in as a build ARG below. The image is otherwise a thin debian base
# so we can curl + run the binary at startup.

FROM debian:trixie-slim AS downloader
ARG GITHUB_TOKEN
ARG PRO_VERSION=v1.6.14
ARG ASSET_NAME=anycable-go-linux-amd64
RUN apt-get update \
  && apt-get install -y --no-install-recommends curl jq ca-certificates \
  && rm -rf /var/lib/apt/lists/*
RUN test -n "$GITHUB_TOKEN" || (echo "GITHUB_TOKEN build arg is required" && exit 1) && \
    ASSET_ID=$(curl -sf -H "Authorization: Bearer $GITHUB_TOKEN" \
      -H "Accept: application/vnd.github+json" \
      "https://api.github.com/repos/anycable/anycable-go-pro-releases/releases/tags/${PRO_VERSION}" \
      | jq -r ".assets[] | select(.name == \"${ASSET_NAME}\") | .id") && \
    test -n "$ASSET_ID" && test "$ASSET_ID" != "null" || (echo "Asset ${ASSET_NAME} not found at ${PRO_VERSION}" && exit 1) && \
    curl -sfL -H "Authorization: Bearer $GITHUB_TOKEN" \
      -H "Accept: application/octet-stream" \
      -o /tmp/anycable-go-pro \
      "https://api.github.com/repos/anycable/anycable-go-pro-releases/releases/assets/${ASSET_ID}" && \
    chmod +x /tmp/anycable-go-pro && \
    /tmp/anycable-go-pro --version

FROM debian:trixie-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=downloader /tmp/anycable-go-pro /usr/local/bin/anycable-go-pro
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/anycable-go-pro"]
