import { mkdir, writeFile } from "node:fs/promises";
import { generate } from "../sdk/typescript/generate";
export { generate, pascal, rustIdentifier, methodIdentifier } from "../sdk/typescript/generate";

if (import.meta.main) {
  const [source, directory, name = "logs"] = process.argv.slice(2);
  if (!source || !directory)
    throw new Error("usage: bun tools/generate.ts <contract.json> <output-dir> [client-name]");
  const output = generate(await Bun.file(source).json(), name);
  await mkdir(directory, { recursive: true });
  await writeFile(`${directory}/${name}.ts`, output.typescript);
  await writeFile(`${directory}/${name}.rs`, output.rust);
}
