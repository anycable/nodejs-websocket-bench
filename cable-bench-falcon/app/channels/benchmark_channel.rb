# The one channel the harness subscribes to. A client subscribes with
# `{ channel: "BenchmarkChannel", stream_name: "<name>" }` and receives every
# message broadcast to that stream. This is identical Action Cable code for
# all three adapters (Solid Cable, classic Action Cable / Redis, AnyCable) —
# what differs is the transport underneath, not the app.
class BenchmarkChannel < ApplicationCable::Channel
  def subscribed
    stream_from params[:stream_name]
  end

  # Client-to-client fan-out used by the optional whispers test. Mirrors the
  # Node socket.io server's whisper handler so the harness can reuse its driver.
  def whisper(data)
    ActionCable.server.broadcast(params[:stream_name], data)
  end
end
