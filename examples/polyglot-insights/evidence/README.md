# Recorded runs

These are actual process log tails and JSON evidence copied after
successful execution on 2026-10-15 UTC. Machine-specific checkout prefixes
are replaced with `<repo>` for publication; results and hashes are unchanged.

- `fault-suite/`: run `insights-39fd8e14-642d-4377-a175-51de60eefbf0`,
  `bun scripts/polyglot-insights.ts --verify`, 12 checks passed.
- `normal-run/`: run `insights-51333aec-690f-42b1-a382-c69636a4cd38`,
  `bun scripts/polyglot-insights.ts`, normal zero-exit cleanup.

Both use the integrated Python SDK from this checkout, with no external SDK path
override. Each `evidence.json` has source and binary SHA-256 hashes, runtime
versions, actual progress/results and final process exit states. Fault-suite
SIGKILL exits are deliberate process-loss injections. All owned processes exited.
