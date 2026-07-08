// Run the official @rails/actioncable client under Node: inject a WebSocket
// implementation and stub the browser globals its ConnectionMonitor touches
// (addEventListener/removeEventListener for online/offline + visibility events,
// document.visibilityState). Import this module before creating any consumer.
import WebSocket from "ws";
import * as ActionCable from "@rails/actioncable";

{
  const g = globalThis as unknown as Record<string, unknown>;
  if (typeof g.addEventListener !== "function") g.addEventListener = () => {};
  if (typeof g.removeEventListener !== "function")
    g.removeEventListener = () => {};
  if (typeof g.document === "undefined") {
    g.document = {
      visibilityState: "visible",
      addEventListener: () => {},
      removeEventListener: () => {},
    };
  }
}
(ActionCable.adapters as { WebSocket: unknown }).WebSocket =
  WebSocket as unknown;

export { ActionCable };
