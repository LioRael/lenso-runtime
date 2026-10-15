# Runtime skill evidence

Date: 2026-10-15 UTC. Local documentation/skill delivery only. No core/SDK/generator source or root lockfiles changed; no global skill installation, push, merge, publication or deployment. Skill entrypoints use progressive references and passed skill-creator quick_validate; relative links resolve.

Source baseline: 765db7a. Skill: `.agents/skills/lenso-runtime`; trigger: explicit cross-language/process SDK/kernel/contract/transport work, not native-only plugin calls.

Actual checks using Bun 1.4.3:

- `bun install --frozen-lockfile`: passed.
- `bun run build:sdk`: passed.
- `bun run typecheck`: passed, including existing generated TS and multitransport examples.
- skill-creator quick_validate and relative reference checks: passed.

The transport guidance was checked against README, PROTOCOL, SDK exports, batch and multitransport sources, and the existing transport-smoke script. TCP, persistent stdio, WebSocket, cancellation and backpressure are documented as existing source capabilities. This documentation-only task did not rerun kernel/network smokes or benchmarks; commands are routed to the existing owners rather than copied into a new harness.

The native Rust adapter pins an older SDK and exposes no transport selector; the skill does not invent one. New TS SDK dependencies can require consumer root-lock integration before a candidate frozen install succeeds. Source-first authoring and Go Runtime delivery remain separately status-gated, not inferred from files in an active task.
