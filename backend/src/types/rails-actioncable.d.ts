// Minimal ambient declaration for the official Rails Action Cable JS client.
// We only use createConsumer(...) and adapters.WebSocket in the bench runner.
declare module "@rails/actioncable" {
  export const adapters: { WebSocket: unknown; logger: unknown };
  export interface Subscription {
    unsubscribe(): void;
  }
  export interface Subscriptions {
    create(
      params: string | Record<string, unknown>,
      mixin?: Record<string, unknown>,
    ): Subscription;
  }
  export interface Consumer {
    subscriptions: Subscriptions;
    connect(): void;
    disconnect(): void;
  }
  export function createConsumer(url?: string): Consumer;
}
