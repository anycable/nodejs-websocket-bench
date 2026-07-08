# syntax=docker/dockerfile:1
# Single image for the three Rails cable benchmark modes; bin/bench-entrypoint
# picks the adapter and process from BENCH_MODE. Mirrors the anycable-pro /
# socketioxide per-service pattern (railway.toml + --path-as-root cable-bench/).
ARG RUBY_VERSION=3.4.4
FROM ruby:$RUBY_VERSION-slim AS base
WORKDIR /app
ENV RAILS_ENV=production \
    BUNDLE_DEPLOYMENT=1 \
    BUNDLE_WITHOUT=development:test \
    BUNDLE_PATH=/usr/local/bundle \
    RAILS_LOG_TO_STDOUT=1 \
    RAILS_SERVE_STATIC_FILES=1 \
    SECRET_KEY_BASE_DUMMY=1
RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y curl && \
    rm -rf /var/lib/apt/lists/*

FROM base AS build
RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y build-essential git pkg-config libyaml-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/*
COPY Gemfile Gemfile.lock ./
RUN bundle install && rm -rf "${BUNDLE_PATH}"/ruby/*/cache
COPY . .
RUN ./bin/rails assets:precompile

FROM base
COPY --from=build /usr/local/bundle /usr/local/bundle
COPY --from=build /app /app
RUN useradd rails --create-home --shell /bin/bash && \
    mkdir -p storage tmp/pids log && \
    chown -R rails:rails db log storage tmp
USER rails
EXPOSE 3000
ENTRYPOINT ["./bin/falcon-entrypoint"]
