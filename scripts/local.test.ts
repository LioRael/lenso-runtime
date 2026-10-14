import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { candidateFiles, recordBinary, verifyBinary } from "./local";

async function nativeFixture(root: string) {
  const native = resolve(root, "native");
  await mkdir(resolve(native, "crates/core/src"), { recursive: true });
  await mkdir(resolve(native, "examples/batch"), { recursive: true });
  for (const [path, text] of Object.entries({
    "Cargo.toml": "[workspace]\n",
    "Cargo.lock": "version = 4\n",
    "crates/core/src/lib.rs": '#![doc = include_str!("../README.md")]\n',
    "crates/core/README.md": "Compiled documentation\n",
  })) await writeFile(resolve(native, path), text);
  return native;
}

test("candidate inventories include SDK configuration and compiled native documentation", async () => {
  const runtime = resolve(import.meta.dir, "..");
  await mkdir(resolve(runtime, "target"), { recursive: true });
  const fixture = await mkdtemp(resolve(runtime, "target/inventory-test-"));
  try {
    expect(typeof (await candidateFiles())["tsconfig.json"]).toBe("string");
    const native = await nativeFixture(fixture);
    expect(typeof (await candidateFiles(native, true))["crates/core/README.md"]).toBe("string");
  } finally {
    await rm(fixture, { recursive: true, force: true });
  }
});

test("binary proofs reject changed bytes, source inventories and the wrong build profile", async () => {
  const runtime = resolve(import.meta.dir, "..");
  await mkdir(resolve(runtime, "target"), { recursive: true });
  const fixture = await mkdtemp(resolve(runtime, "target/binary-proof-test-"));
  const binary = resolve(fixture, "binary");
  try {
    await writeFile(binary, "owned fixture binary");
    await recordBinary(binary, 12, undefined, "debug");
    expect(await verifyBinary(binary, undefined, "debug")).toBe(12);
    const original = await Bun.file(`${binary}.source.json`).text();
    expect(original).not.toContain(fixture);
    await expect(verifyBinary(binary, undefined, "release")).rejects.toThrow("Binary candidate mismatch");
    await writeFile(binary, "changed fixture binary");
    await expect(verifyBinary(binary)).rejects.toThrow("Binary candidate mismatch");
    await writeFile(binary, "owned fixture binary");
    const proof = JSON.parse(original);
    proof.files["Cargo.toml"] = "0".repeat(64);
    await writeFile(`${binary}.source.json`, JSON.stringify(proof));
    await expect(verifyBinary(binary)).rejects.toThrow("Binary candidate mismatch");
    await expect(recordBinary(binary, 12, undefined, "unknown")).rejects.toThrow("Unknown binary build profile");
  } finally {
    await rm(fixture, { recursive: true, force: true });
  }
});

test("native proofs reject changed include_str inputs rather than accepting an old application", async () => {
  const runtime = resolve(import.meta.dir, "..");
  await mkdir(resolve(runtime, "target"), { recursive: true });
  const fixture = await mkdtemp(resolve(runtime, "target/native-proof-test-"));
  const binary = resolve(fixture, "binary");
  try {
    const native = await nativeFixture(fixture);
    await writeFile(binary, "owned fixture binary");
    await recordBinary(binary, 12);
    const proof = await Bun.file(`${binary}.source.json`).json();
    proof.native_files = await candidateFiles(native, true);
    await writeFile(`${binary}.source.json`, JSON.stringify(proof));
    expect(await verifyBinary(binary, native, "debug")).toBe(12);
    await writeFile(resolve(native, "crates/core/README.md"), "Changed compiled documentation\n");
    await expect(verifyBinary(binary, native, "debug")).rejects.toThrow("Native application source mismatch");
  } finally {
    await rm(fixture, { recursive: true, force: true });
  }
});
