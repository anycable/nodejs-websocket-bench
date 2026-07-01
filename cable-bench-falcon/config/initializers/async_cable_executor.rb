# Install async-cable's fiber-based executor on this Falcon target.
#
# Action Cable dispatches pub/sub callback invocations and periodic timers
# through `ActionCable::Server::Base#executor`. Stock Rails (and the pinned
# async-cable @27181dff1) back this with a Concurrent::ThreadPoolExecutor, so
# under Falcon's fiber reactor every broadcast dispatch bounces through an OS
# thread before it reaches the socket. That thread hop is the prime suspect for
# the broadcast-latency gap vs Puma.
#
# Samuel Williams added `Async::Cable::Executor` (a fiber-based replacement) in
# async-cable commit dddef54c, but that same commit bumped the gemspec to
# `actioncable >= 8.2.0.alpha` (edge Rails only), and this app is Rails 8.1.3 +
# actioncable-next. The Executor code itself is self-contained (it only needs
# `async`) and async-cable does not auto-wire it anyway, so we vendor it here
# verbatim and install it by overriding the server's #executor. Vendored from
# https://github.com/socketry/async-cable/blob/dddef54c/lib/async/cable/executor.rb
require "async"

module Async
  module Cable
    # Fiber-based replacement for `ActionCable::Server::ThreadedExecutor`.
    # Tasks posted from inside a reactor run on the caller's reactor (no thread
    # hop); tasks posted from outside, and recurring timers, run on a dedicated
    # reactor thread owned by the executor.
    class Executor
      def initialize
        @mutex = ::Thread::Mutex.new
        @inbox = nil
        @thread = nil
      end

      def post(task = nil, &block)
        block ||= task

        if current = ::Async::Task.current?
          current.async { block.call }
        else
          inbox.push(proc { block.call })
        end

        return self
      end

      def timer(interval, &block)
        timer = Timer.new

        if current = ::Async::Task.current?
          timer.task = current.async do |inner|
            run_timer(inner, interval, block)
          end

          return timer
        end

        inbox = timer.inbox = self.inbox
        begin
          operation = proc do |task|
            timer.task = task.async do |inner|
              run_timer(inner, interval, block)
            end
          end

          inbox.push(operation)
        rescue ::ClosedQueueError
          # Executor is shutting down; match best-effort post-during-shutdown.
        end

        return timer
      end

      def shutdown
        @mutex.synchronize do
          return unless @thread
          @inbox.close
          @thread.join
          @thread = nil
          @inbox = nil
        end
      end

      class Timer
        attr_writer :inbox

        def initialize
          @inbox = nil
          @mutex = ::Thread::Mutex.new
          @task = nil
        end

        def task=(task)
          @mutex.synchronize { @task = task }
        end

        def shutdown
          task = nil

          @mutex.synchronize do
            task = @task
            @task = nil
          end
          return unless task

          if inbox = @inbox
            begin
              inbox.push(proc { task.stop })
            rescue ::ClosedQueueError
              # Executor already shut down; timer stopped with its reactor.
            end
          else
            task.stop
          end
        end
      end

      private

      def inbox
        @inbox || @mutex.synchronize { @inbox ||= start_thread }
      end

      def run_timer(task, interval, block)
        loop do
          task.sleep(interval)
          block.call
        end
      end

      def start_thread
        inbox = ::Thread::Queue.new

        @thread = ::Thread.new do
          ::Thread.current.name = "async-cable executor"

          Sync do |task|
            while operation = inbox.pop
              operation.call(task)
            end
          end
        end

        return inbox
      end
    end
  end
end

# Override the Action Cable server's executor to use the fiber executor instead
# of the thread-pool ThreadedExecutor. Lazy + mutex-guarded, mirroring
# actioncable-next's own ActionCable::Server::Base#executor.
require "action_cable"
module AsyncCableFiberExecutor
  # Reuses ActionCable::Server::Base's own @mutex/@executor ivars (set in its
  # #initialize), matching actioncable-next's lazy #executor. If a future Rails
  # bump stops initializing @mutex, this needs a guard.
  def executor
    @executor || @mutex.synchronize { @executor ||= Async::Cable::Executor.new }
  end
end
ActionCable::Server::Base.prepend(AsyncCableFiberExecutor)
