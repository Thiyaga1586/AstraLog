# AstraLog P8-E Architecture Blocker — Persistence Group Commit

Date: 2026-10-06

Status: **BLOCKER RESOLVED BY ACCEPTED ADR-018 / V2 RE-FREEZE CANDIDATE — NO P8-E ACTION IS CLOSED**

Entering semantic authority:

    b421f2d6b4ed3a0c5b22195f973d9cef24366a78

That exact P8-D commit carries the required PASS Git note. P8-001 through P8-073 are closed.
P8-074 through P8-090 remain unchecked.

## Blocker

The frozen P8-E contract requires a per-partition persistence staging queue that can allow multiple
original P3 persistence tokens to share one physical durability sync.

The already-frozen P3/P4 contracts make that unreachable without an architecture change:

1. P3 permits **at most one persistence batch outstanding per Raft group**.
2. While that token is outstanding, consensus-driving persistence cannot stage a second token.
3. P4 returns PersistenceCompleted(token) only after every durability operation required by that
   batch has succeeded.
4. Therefore token N+1 for the same partition cannot exist until token N is already durably
   complete.
5. Once token N is complete, its physical sync has already happened, so there is no later
   same-partition token with which that sync can be amortized.

This is a contract contradiction, not a missing queue implementation.

## Why P8-E must not fake completion

Reporting PersistenceCompleted after merely staging bytes, before the durability operation, would
violate frozen correctness:

- P3's single explicit durability frontier;
- P4 synchronous PersistenceBatch completion contract;
- I-02 quorum-ack durability;
- I-09 durable follower acknowledgement;
- I-30 no success on ambiguous durable error.

It would also allow Raft to emit responses/actions derived from durability that does not yet exist.

## Why a local queue alone does not solve it

A bounded per-partition queue can hold the current token, but under the frozen P3 frontier its
maximum reachable depth is one. A maximum-delay timer would only delay that single token's fsync;
it would not create group commit or fsync amortization.

Synthetic tests that inject multiple PersistenceBatch values directly into a helper would not prove
the production runtime can ever reach that state, so they are insufficient for P8-081/P8-089 and
must not be used to claim closure.

## Safe resolution options

### Option A — Supersede the P3 persistence frontier

Version the Raft persistence contract so one group may expose a bounded ordered queue of persistence
tokens while preserving one monotonic durability frontier.

Required work includes:

- bounded pending persistence token/byte counts in RaftCore;
- strict token ordering and prefix-only completion;
- no consensus action may observe a token as durable before its completion;
- barriers for hard-state, commit, truncation, snapshot/install and rebootstrap;
- updated deterministic/formal P3 persistence models;
- crash/ambiguous-sync campaigns;
- full P3 through P8-D requalification.

This is the most direct way to preserve the intended **per-partition** P8-E group-commit model, but it
is a real frozen-architecture change and requires the superseding ADR procedure.

### Option B — Re-scope P8-E to cross-partition filesystem group sync

Keep one token outstanding per Raft group, but coordinate one token from many partitions and prove a
single filesystem-level sync makes all participating file writes durable before completing any of
them.

This avoids widening one partition's P3 token frontier, but it is not the currently frozen
per-partition group-commit design. It also requires a portable, explicitly specified filesystem
durability primitive and failure model. The current std::fs per-file sync path does not provide this
abstraction.

### Option C — Remove/defer group commit from P8

Keep P3/P4 unchanged and formally remove P8-E group commit from the P8 closure requirements.

This is the lowest correctness risk but changes the frozen P8 scope/checklist and therefore also
requires architecture-freeze revision before P8-F can be entered.

## Recommended resolution

If true per-partition group commit remains a P8 requirement, **Option A** is the coherent design.
It makes the necessary durability pipeline explicit rather than hiding it below a contract that
forbids multiple outstanding tokens.

The proposed superseding ADR is documented separately in:

    docs/ADR_P8_E_PERSISTENCE_FRONTIER.md

ADR-018 is now accepted as the selected resolution. Implementation still has no authority until the
docs-only V2 re-freeze commit is exact-HEAD qualified and carries its freeze PASS note.

## Current mechanical boundary

- P8-001..P8-073: closed.
- P8-074..P8-090: open.
- P8-F and later: disabled.
- no P8-E production Rust/Cargo/dependency change has been made.
- P8-D semantic authority remains b421f2d plus its exact-HEAD PASS note.

P8-E implementation may resume only after PERSISTENCE_FRONTIER_V2.md is committed in the docs-only
re-freeze and that exact commit carries its required PASS note.
