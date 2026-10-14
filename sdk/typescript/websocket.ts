import { createRequire } from "node:module";
import { dirname, join } from "node:path";

// Bun's built-in "ws" shim ignores maxPayload and fragmentation options.
// Resolve the installed parser explicitly so Bun and Node enforce the same limit.
const require = createRequire(import.meta.url);
const library: typeof import("ws") = require(join(dirname(require.resolve("ws/package.json")), "index.js"));
export const WebSocket = library.WebSocket;
export const WebSocketServer = library.WebSocketServer;
