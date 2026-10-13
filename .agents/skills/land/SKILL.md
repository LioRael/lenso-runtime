---
name: land
description: Verify and land requested Lenso Runtime changes to the remote destination branch, honoring the repository's current checks and merge policy.
user-invocable: true
disable-model-invocation: true
metadata:
  delta-action: land
---

# Land Runtime changes

Complete an explicit landing request for this repository, normally onto remote
`main`. Creating or editing this skill supplies no landing authorization. Keep
npm/crates publication, binary distribution, tags, releases and deployment
separate; each requires its own explicit authorization.

## 1. Pin the source and destination

Use the attached Runtime checkout, leaving the user's primary checkout and
other repositories untouched. Read applicable `AGENTS.md`/`AGENT.md`,
contribution policies, [README](../../../README.md), [protocol](../../../PROTOCOL.md),
manifests and current workflow definitions.

Inspect status, branches, recent commits, fetch URLs and push URLs. Identify the
requested diff and preserve unrelated staged and unstaged work. The expected
repository is `LioRael/lenso-runtime`; confirm the configured publishing remote
rather than assuming `origin`. The `local` backlink is not a publishing remote.
Ask when the source, destination or publishing repository is ambiguous.

Fetch the destination and record its full base SHA. Query current branch
protection, applicable rulesets, required checks, reviews, signatures and allowed
merge methods. An unavailable policy query is a blocker. Read destination
instructions when they differ from the source. A missing destination requires
explicit authorization to create it.

Commit only named in-scope files on a recoverable source branch. Preserve
authorship and configured signing; stop on unavailable signing authentication.
Use `GIT_EDITOR=true` for commands that may open an editor. Do not stash or
discard unrelated work to make the checkout clean.

For a retry, explicitly import any requested parent-thread fixes before pinning
the source again. A message saying a fix exists does not update this checkout.

**Done:** source commit, repository URL, remote, destination, base SHA and
direct-push or PR route are recorded; publication of source is authorized.

## 2. Form one clean candidate

Create a uniquely named local integration branch from the fetched destination.
Integrate the pinned source without modifying the source branch or advancing
the destination. Respect linear-history rules when choosing integration.
Resolve conflicts only when combined intent is clear, preserving unrelated
destination changes; otherwise stop for a decision.

Review the complete candidate diff, including generated output, executable
scripts and workflows before running them with upstream credentials. Verify
that the change preserves local trusted-process boundaries, explicit exports,
caller-held schema checks, readiness validation and no replay/retry semantics
where those contracts are affected. Record review findings and resolutions.

If the requested change is already present, confirm remote contents and report
the no-op without claiming a new landing.

**Done:** a pinned, reviewed, clean candidate contains the requested source and
current destination work, with unrelated changes excluded.

## 3. Verify that exact revision

Inspect `package.json`, `sdk/typescript/package.json`, `Cargo.toml`, both
lockfiles and any toolchain/version files. Use their declared tools and existing
tasks; record `bun --version`, `cargo --version` and `rustc --version`.
Do not change global tool configuration or silently substitute a toolchain.

Establish reproducibility with `bun install --frozen-lockfile`; record lockfile
hashes before and after verification. If dependency manifests changed,
synchronize only the necessary lockfile changes on the candidate, review their
resolutions, commit them and repeat frozen installation in a clean source copy.
Run `cargo fetch --locked` before the existing build/test entrypoints and retain
locked Cargo checks. Unexplained resolution changes block landing.

For kernel, protocol, SDK, generator or example behavior changes, the existing
standalone verification is:

```sh
bun run build
bun run test
bun run typecheck
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
bun run smoke
```

`build` includes contract generation and native binaries; `test` includes Rust
and TS SDK tests. Confirm generated artifacts reproduce and both lockfiles stay
unchanged. Inspect affected tests and add only concrete missing regressions.
Use `cargo fmt --all --check` for changed Rust source. Documentation/skill-only
changes need frontmatter, local-link, Markdown and diff validation, not unrelated
runtime builds. Performance evidence is required only for requested performance
changes or claims; use the existing `perf` entry rather than inventing benchmarks.

For affected TS application integration, inspect the existing
`scripts/verify-lenso.ts` and the consuming repository's preparation/build
instructions, then run `LENSO_ROOT=<prepared-checkout> bun run verify:lenso`.
Use an explicitly selected attached/prepared checkout, never a cached
machine-specific path. Editing or landing consumer repositories requires
separate scope. Local SDK candidate preparation is development evidence, not a
substitute for any required immutable dependency or CI policy.

Run only owned processes, ports and disposable fixtures, and confirm cleanup.
Missing required tools, infrastructure or failed checks block promotion; skips
and local results cannot stand in for required remote evidence.

Run `git diff --check`, inspect generated/external changes and confirm a clean
tracked checkout. Commit intended changes and repeat affected verification
before publishing. Record the final tested SHA and limitations.

**Done:** applicable local checks pass on the unchanged candidate, with
reproducible dependencies, reviewed output and owned-resource cleanup.

## 4. Publish, satisfy policy and land

Recheck destination protection before promotion. Publish only the final
candidate to a unique ref if remote checks or the repository's PR route require
it. Match candidate naming to the actual workflow triggers. Verify required CI
by full SHA, ref, workflow, event, run attempt and successful job conclusions;
do not copy Console's `ci/quality` convention or invent a Runtime check name.
Required reviews and signatures must also be satisfied on that revision.

Fetch the destination immediately before landing. If it advanced, reintegrate,
review and verify a new candidate; retain the previous one as recovery state.
For an allowed direct route, normally fast-forward with an explicit refspec:
`git push <remote> <verified-sha>:refs/heads/<destination>`.
For a required PR route, merge through the hosting service using an allowed
method and a pinned expected PR head. Never force-update a shared branch,
bypass protection or treat scheduled auto-merge as completed landing.

Read back the remote ref and hosting merge record. For a direct push, confirm
the destination equals or contains the verified candidate. For squash/rebase,
verify the resulting landed diff and record the actual result SHA. A pending
PR or candidate push alone is not landing.

Retain candidate refs and local recovery branches by default. Any cleanup must
follow repository policy and explicit scope; do not delete unrelated refs or
worktrees. Report repository URL, destination, source/base/candidate/result
SHAs, PR link when applicable, exact checks, limitations and retained refs.
Distinguish pre-merge evidence from post-merge destination CI; disclose any
observed failure, and complete a post-merge wait only when required.

**Done:** remote/content confirmation proves the requested change reached its
destination with every prerequisite satisfied, or landing has stopped with a
specific blocker and recoverable source/candidate references.

In a Delta Land action, follow the attached landing-outcome instructions and
end with one structured result only after successful landing or a terminal
failure. Skill creation, preparation and already-landed no-ops are not outcomes.
