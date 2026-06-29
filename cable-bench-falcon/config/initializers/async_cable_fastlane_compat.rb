# Fastlane broadcasts (config/initializers/action_cable.rb) make
# actioncable-next encode each broadcast once per channel identifier and send
# it through Connection#raw_transmit -> Socket#raw_transmit, bypassing the
# per-subscriber coder. ActionCable::Server::Socket implements #raw_transmit,
# but the Falcon WS adapter Async::Cable::Socket (async-cable 0.3.1) only
# implements #transmit (which JSON-encodes via its coder) and has no
# #raw_transmit. So every fastlane broadcast raises NoMethodError inside the
# stream callback and the client receives nothing (0% delivery).
#
# Add the missing method: push the already-encoded frame straight onto the
# outbound queue, skipping the coder, exactly as
# ActionCable::Server::Socket#raw_transmit sends a pre-encoded message as-is.
# Without this shim, actioncable-next fastlane + async-cable cannot deliver.
if defined?(Async::Cable::Socket) && !Async::Cable::Socket.method_defined?(:raw_transmit)
  Async::Cable::Socket.class_eval do
    def raw_transmit(data)
      @output.push(data)
    end
  end
end
