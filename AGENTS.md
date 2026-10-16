# Working on Lenso Runtime

- Start with [README](README.md) for entrypoints, [PROTOCOL](PROTOCOL.md) for wire
  invariants, [validation](.agents/skills/lenso-runtime/references/validation.md)
  for scoped proof, and [distribution](docs/distribution.md) for native assets.
- Preserve explicit exports, caller-held schema checks, readiness and scoped
  identity/generation checks. A validated connection identity can be reused;
  each invocation must still enforce its own contract and target authorization.
- Keep setup out of dispatch: no per-call schema preparation, unrelated process
  startup, migrations or queue initialization. Measure real I/O, serialization,
  round trips and latency with existing harnesses before making speed claims.
- Test protocol compatibility, deadlines, cancellation, bounded credit/framing,
  process cleanup and fail-closed behavior. Prefer shared schema fixtures and real
  cross-language round trips over duplicate assertions or global monkey patches.
- Start with the smallest affected check. Build the SDK once; generator fixtures
  and workspace checks share Cargo artifacts. Coordinate heavy sibling-repository
  builds and limit local Cargo jobs when sharing the machine. Docs-only edits need
  links/frontmatter/diff checks, not native builds or benchmarks.
- Preserve all five native release targets and their smoke/checksum gates. Local
  build, CI, merge, published assets and actual consumer acceptance are separate
  evidence; report exact source/toolchain/target and unrun boundaries.
- Use owned fixtures/processes and explicit sibling checkout paths. Do not change
  another task's checkout, user browser session, global toolchain or permissions.
  Use the built-in browser or isolated headless browser if browser proof is needed.
- Landing follows [.agents/skills/land](.agents/skills/land/SKILL.md) only when
  authorized. Publication and deployment require their own requested scope.
