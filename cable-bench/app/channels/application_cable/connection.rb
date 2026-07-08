module ApplicationCable
  # The benchmark connects anonymous clients (the load generator opens raw
  # WebSockets and subscribes to BenchmarkChannel). No app-level auth is
  # performed, so every adapter is measured on the same handshake cost.
  class Connection < ActionCable::Connection::Base
  end
end
