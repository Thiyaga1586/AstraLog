# AstraLog P8-E Persistence Frontier Architecture Re-Freeze

Date: 2026-10-06

Status: **RE-FREEZE CANDIDATE — IMPLEMENTATION DISABLED UNTIL EXACT-HEAD PASS NOTE**

## Entering authority

P8-D semantic authority:

    b421f2d6b4ed3a0c5b22195f973d9cef24366a78

P8-E blocker audit:

    4353c9d79ec6630389612bfd353c5e0f9c22c059

The blocker proved that true same-partition group commit was unreachable while P3 allowed one
outstanding persistence token and P4 completed that token only after durability.

## Accepted resolution

ADR-018 accepts Option A.

PERSISTENCE_FRONTIER_V2.md is the normative additive contract. It supersedes only RAFT_V1 section
8.1 single-outstanding-token scheduling. It preserves the existing completion contract and every
durability and acknowledgement invariant.

The critical new distinction is:

- staged logical append frontier: ordered, bounded and non-durable;
- durable frontier: monotonic and authoritative.

Only append-only compatible tokens may coexist. Hard-state, commit, truncate, snapshot/install,
discard/rebootstrap and durability-dependent transitions remain barriers.

## No implementation in this checkpoint

This re-freeze is documentation-only.

It does not:

- change RaftCore;
- change P4 storage execution;
- change Storage V1;
- change wire/controller formats;
- check any P8-E action;
- authorize P8-F.

P8-074 through P8-090 remain open until their implementation proofs exist.

## Implementation authorization gate

P8-E implementation is authorized only after the exact docs-only re-freeze commit proves:

1. only architecture and status documentation changed;
2. P8-001 through P8-073 remain checked and P8-074 through P8-090 remain open;
3. all eight P8 exit invariants remain open;
4. no crate, Cargo, wire, protocol or storage-golden byte changed;
5. format, Clippy, doc tests and diff hygiene pass;
6. the exact commit carries a Git note naming PERSISTENCE_FRONTIER_V2.md as the P8-E authority.

Once that note exists, P8-E may implement P8-074 through P8-090 under V2. P8-F remains disabled.

## Pre-commit qualification evidence

The docs-only re-freeze candidate passed:

- changed-path audit: README plus seven docs files only;
- crate/Cargo delta from P8-D semantic authority: none;
- protocol/storage golden delta from P8-D semantic authority: none;
- P8 checklist IDs: exactly 160 unique IDs P8-001 through P8-160;
- checked actions: exactly 73;
- P8-E open actions: exactly 17;
- P8 exit invariants open: exactly 8;
- cargo fmt --all -- --check: PASS;
- workspace Clippy with -D warnings: PASS;
- workspace doc tests: PASS;
- git diff --check: PASS.
