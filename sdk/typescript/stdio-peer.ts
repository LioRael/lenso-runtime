import { RuntimeHost } from "@lenso/runtime-sdk";

const value = { type: "integer" } as const;
const host = await RuntimeHost.connect({
  namespace: "test", service: "stdio", instance: "one",
  transport: { type: "stdio" },
  contract: { version: "1", methods: { echo: { mode: "unary", input: value, output: value, error: value } } },
  handlers: { echo: (input) => input },
});
try {
  const target = { namespace: "test", service: "other", contractVersion: "1" };
  const a = host.call(target, "echo", "first");
  const b = host.call(target, "echo", "second");
  const outputs = await Promise.all([a, b]);
  if (outputs[0] !== "é" || outputs[1] !== 2) throw new Error("correlation lost");
  await host.status();
  throw new Error("disconnect did not fail pending status");
} catch (reason) {
  if ((reason as any).code !== (process.argv[2] === "oversized" ? "protocol_error" : "connection_lost")) throw reason;
  try {
    await host.status();
    throw new Error("disconnected host accepted work");
  } catch (closed) {
    if ((closed as any).code !== "connection_lost") throw closed;
  }
} finally {
  host.close();
}
