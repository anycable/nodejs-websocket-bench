# actioncable-next supports "fastlane" broadcasts: a stream's payload is
# JSON-encoded once per channel identifier instead of once per subscriber,
# which roughly halves broadcast latency on large fan-outs. This is an
# actioncable-next optimization (stock Action Cable has no equivalent), so we
# enable it on the Async::Cable/Falcon target to measure that path.
# https://github.com/anycable/actioncable-next#actioncableserverconfigfastlane_broadcasts_enabled--true
Rails.application.config.after_initialize do
  ActionCable.server.config.fastlane_broadcasts_enabled = true
end
