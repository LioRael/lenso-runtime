import { test, expect } from "bun:test";
import { createConnection } from "node:net";
import { resolve } from "node:path";
import { LocalInsights, root, until } from "./local";

const python = process.env.PYTHON ?? "python3";
const pythonPath = resolve(process.env.LENSO_PYTHON_SDK_ROOT ?? resolve(root, "sdk/python"), "src");
const exited = (app: LocalInsights) => app.children.every(child => child.process.exitCode !== null || child.process.signalCode !== null);
async function portClosed(port: number) {
  return new Promise<boolean>(done => {
    const socket = createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => { socket.destroy(); done(false); });
    socket.once("error", () => { socket.destroy(); done(true); });
  });
}

test("ready checks exact capabilities; concurrent close waits for all children and releases port", async () => {
  const app = new LocalInsights(python, pythonPath);
  try {
    await app.start();
    expect(app.children.length).toBe(3);
    await Promise.all([app.close(), app.close()]);
    expect(exited(app)).toBe(true);
    expect(app.children.map(child => child.process.exitCode)).toEqual([0, 0, 0]);
    expect(await portClosed(app.port)).toBe(true);
  } finally { await app.close(); }
}, 10000);

test("Python startup import failure includes log tail and cleans up kernel and Rust", async () => {
  const app = new LocalInsights(python, resolve(root, "target/missing-python-sdk"));
  try {
    await expect(app.start()).rejects.toThrow("ModuleNotFoundError");
    expect(exited(app)).toBe(true);
    expect(await portClosed(app.port)).toBe(true);
    expect(app.cleaner.tail).toContain("lenso_runtime");
  } finally { await app.close(); }
}, 10000);

test("interruption during startup cannot spawn an orphan service", async () => {
  const app = new LocalInsights(python, pythonPath);
  const starting = app.start();
  void starting.catch(() => {});
  try {
    await until("kernel spawned", () => app.children.length > 0);
    await app.close();
    await expect(starting).rejects.toThrow();
    expect(exited(app)).toBe(true);
  } finally { await app.close(); }
}, 10000);
