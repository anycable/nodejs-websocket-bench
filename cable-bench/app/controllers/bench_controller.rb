# The harness publishes through Rails so every adapter shares one publish
# path: POST /_bench/broadcast {stream, data} -> ActionCable.server.broadcast.
# For AnyCable this is patched to publish through the broker (extended
# protocol, reliable streams); for Action Cable / Solid Cable it goes through
# the Redis / DB adapter. Same code, different transport.
class BenchController < ActionController::Base
  # The load generator posts JSON without a CSRF token.
  skip_forgery_protection

  before_action :authorize, only: :broadcast

  # GET /health — liveness probe (open, no auth).
  def health
    render json: { status: "ok", mode: ENV.fetch("CABLE_ADAPTER", "solid_cable") }
  end

  # POST /_bench/broadcast {stream, data}
  # `data` arrives as a JSON string (the harness pre-serializes it); broadcast
  # the parsed object so subscribers receive { seq, sentAt, text }.
  def broadcast
    payload =
      begin
        JSON.parse(params.require(:data))
      rescue JSON::ParserError, TypeError
        params[:data]
      end
    ActionCable.server.broadcast(params.require(:stream), payload)
    head :ok
  end

  private

  # Optional bearer gate, matching the bench-runner's BENCH_RUNNER_TOKEN. If
  # the env var is unset (local dev), the endpoint is open.
  def authorize
    expected = ENV["BENCH_RUNNER_TOKEN"]
    return if expected.blank?
    provided = request.headers["Authorization"].to_s.delete_prefix("Bearer ")
    head :unauthorized unless ActiveSupport::SecurityUtils.secure_compare(provided, expected)
  end
end
