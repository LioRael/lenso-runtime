# Offline v1 contract check

Compare a caller-reviewed contract with a proposed contract before generation or
local service replacement:

```sh
bun tools/contract-check.ts before.json after.json
bun tools/contract-check.ts --json --name logs before.json after.json
# A runnable self-check using the existing example:
bun tools/contract-check.ts examples/contracts/logs.json examples/contracts/logs.json
```

The command reads two JSON files and writes its report to stdout. It does not
modify contracts, write generated files, connect to a kernel, register a service,
promote an instance, or change the wire protocol. It imports the existing SDK
validator and calls the existing generator in memory. No SDK build, Rust build,
package install, or network access is needed to run this source command with Bun.

`--name` selects the same client name passed to `tools/generate.ts`. Its default
is `logs`, matching that generator's default. Use the actual name for each
generation target because generated type collisions depend on it. Use `--` before
filenames starting with a dash. `--help` prints usage.

## What the result means

`strictMatch` is `match`, `mismatch`, or `unknown`. For two supported, validated
definitions, it compares ordinary JSON using the existing v1 structural rule:
object key order is ignored, array order is retained, and numbers use binary64
semantics. It does not canonicalize schemas or introduce compatibility rules.

Consequently, adding an optional input field, widening a bound, reordering an
`enum` or `required` array, or swapping nullable branches still yields `mismatch`.
Those changes may have different migration effects, but v1 does not negotiate
them. Diagnostics cover method additions/removals, mode/version changes, nested
fields, required lists, nullable branches, types, enums, numeric bounds, string
length bounds and array item/count constraints. Inputs, outputs (including stream
items) and business errors are all compared. Changes are reported before → after.

The contract's `version` is an opaque, nonempty string, not a semver range. Here
“v1” names the restricted protocol/schema format; it does not require the business
contract to use the string `"1"`. Runtime selects versions exactly. Registration
and promotion keep their existing strict rules; this CLI grants no exception.

`migrationSafety` is always `unknown`, including when the definitions match.
This tool does not prove input/output language inclusion, handler behavior,
business semantics, authorization, deployment safety, or rolling-upgrade safety.
There is deliberately no “compatible”, “safe”, or inferred semantic-version
recommendation. Strict equality is only one fact needed by the existing runtime;
the CLI does not evaluate live instances, queued/in-flight calls, readiness,
connection state, frame-envelope size or operational policy.

Each document also has separate results:

| Field | Values | Meaning |
| --- | --- | --- |
| `validation` | `valid`, `invalid`, `unknown` | Existing TypeScript SDK validation, or an analyzer boundary |
| `generator` | `accepted`, `rejected`, `unknown` | Current generator completed in memory, rejected at a located path, or could not be assessed |

Generator acceptance means the existing generator returned source strings. It
does **not** mean that both compilers accepted them or that a target application
can build. This check does not add generation support, fix generator behavior or
replace target-language compilation. Identifier exclusions, empty underscore
name segments and generated type-name collisions have path diagnostics; a
collision also includes `relatedPath`. The locator follows the current generator
and is exercised against it in tests. An unrecognized generator failure remains
`unknown` at the document root, preserving the original error message. Only the
first validation or generator failure per document is reported; correct it and
rerun to expose later failures. If validation fails, generation is not attempted.

## Machine-readable output and exit status

`--json` emits one JSON object with `formatVersion: 1`. Normal reports contain
`clientName`, `strictMatch`, `migrationSafety`, `before`, `after`, `diagnostics`
and an explanatory `note`. A changed string bound, for example, produces:

```json
{
  "source": "comparison",
  "category": "change",
  "code": "constraint.changed",
  "path": "$[\"methods\"][\"run\"][\"input\"][\"maxLength\"]",
  "pointer": "/methods/run/input/maxLength",
  "message": "Contract structure changed; strict matching differs. Migration impact is unknown.",
  "before": 10,
  "after": 20
}
```

Paths use JSON bracket notation with escaped property names, with an RFC 6901
JSON Pointer alongside each path. The root is `$` / an empty pointer. A missing
`before` or `after` member means absence, distinct from an explicit JSON `null`.
Presence changes use the enclosing `required` path. Human reports truncate long
values; JSON reports retain the complete changed values. Reports therefore
include contract constants/enum members and should be handled like those inputs.

| Exit | Meaning |
| --- | --- |
| `0` | Both definitions validate, strictly match, and the generator accepts both |
| `1` | A strict mismatch or a located generator rejection |
| `2` | Invalid contract, unknown analysis, malformed input, I/O error or bad CLI usage |

Unknown/invalid takes precedence over mismatch. With `--json`, file and usage
errors also produce a single object on stdout: `formatVersion`, unknown
`strictMatch`/`migrationSafety`, and a root-path diagnostic with source `before`,
`after` or `options`. It omits document check fields because those checks did not
complete. Human file/usage errors go to stderr. `--help` prints text and exits `0`.

## Bounds and known limits

Only regular UTF-8 JSON files up to 1 MiB are accepted. This analyzer file limit
is distinct from the runtime's 256 KiB framed-message limit; passing this tool
does not certify registration-envelope size. Structural validation enforces the
existing schema-depth bound. Analyzer traversal also stops at 256 JSON path
segments or 100,000 values and reports `unknown`. Non-finite numbers, unpaired
Unicode surrogates and non-JSON library inputs also produce unknown analysis.
File decoding/parsing failures yield exit `2`. JSON parsing uses the host's
standard parser, including binary64 number rounding and last-value treatment of
duplicate object keys; source-token identity is not compared.

Unsupported schema keywords (`$ref`, `format`, arbitrary unions/maps, etc.) and
optional nullable fields fail the existing validator and are never treated as a
compatible extension. No constraint-solving heuristics are used. Missing evidence
stays `unknown`; neither a match nor generator acceptance is a safety guarantee.

## Library use and local verification

```ts
import { checkContracts, exitCode, formatHuman } from "./tools/contract-check/index";

const report = checkContracts(beforeJson, afterJson, { clientName: "logs" });
console.log(formatHuman(report));
process.exitCode = exitCode(report);
```

Pass ordinary JSON data (for example the result of `JSON.parse`); inputs are not
mutated. The module is separate from the shipped SDK and kernel. Run its focused
tests explicitly, since the existing root test script selects only SDK tests:

```sh
bun test tools/contract-check.test.ts
# With workspace development dependencies installed:
bun run build:sdk
bun run typecheck
```
