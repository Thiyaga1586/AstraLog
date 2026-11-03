# AstraLog Correctness Invariants

These are the properties implementation and tests must preserve. A feature is not correct merely because its API works.

## I-01 — Single committed history per partition

Two correct nodes must never expose different committed records for the same partition log position.

## I-02 — Quorum-ack durability

Once a producer receives success for `acks=quorum`, that record must remain in the committed history despite failure of any minority of the partition's voting replicas.

## I-03 — Uncommitted suffix may disappear

Records acknowledged only by `acks=leader` may be truncated after leadership change. This is an explicit API property, not a bug.

## I-04 — Committed-only default reads

Normal consumers never receive entries beyond the partition's committed high watermark.

## I-05 — Leader fencing

A node from an older Raft term, broker incarnation, producer epoch, group epoch, or queue lease epoch cannot successfully perform an operation requiring the newer authority.

## I-06 — Raft election safety

At most one leader can be elected for a Raft term.

## I-07 — Raft log matching

If two logs contain an entry with the same Raft index and term, all preceding entries match.

## I-08 — Committed prefix preservation

A committed entry is never replaced by a different entry at the same Raft index.

## I-09 — Durable follower acknowledgement

A follower cannot acknowledge a replication entry as persisted until the required local durability operation has completed successfully.

## I-10 — CRC corruption is never silent

A checksum mismatch always produces a visible corruption/recovery path. Committed corruption is never hidden by truncation.

## I-11 — Indexes are derived state

Deleting every `.index`, `.timeindex`, and `.raftindex` file must not destroy authoritative event data. They are rebuildable from log segments.

## I-12 — Immutable closed segments

Normal append traffic never modifies a rolled immutable segment. Compaction replaces complete segment generations using copy-on-write.

## I-13 — Stable stream offsets

Retention/compaction never renumbers an existing record offset.

## I-14 — Partition ordering

For a producer whose accepted records target one partition, the committed stream order respects the protocol's accepted sequence rules.

## I-15 — Idempotent retry

An exact supported retry of a previously accepted producer sequence range must not create a second committed logical copy of that sequence range.

## I-16 — Producer fencing

A producer epoch older than the current accepted epoch for that producer ID cannot append records.

## I-17 — No silent sequence gaps

The broker rejects out-of-order producer sequence gaps rather than guessing intent.

## I-18 — Group ownership uniqueness

Within the authoritative consumer-group state, a partition is active on at most one member at a time.

## I-19 — Cooperative handoff

A partition being moved is either still active on the old member, pending revocation, or active on the new member; it does not become simultaneously active on both under the coordinator state machine.

## I-20 — Stale offset commits are fenced

A consumer using an old group/member epoch cannot overwrite offsets after the group has moved on.

## I-21 — Queue lease fencing

An acknowledgement carrying an expired/superseded receipt lease cannot complete the message for the current lease.

## I-22 — Queue delivery is at-least-once

The system may redeliver after uncertain failures but must not claim exactly-once worker execution.

## I-23 — DLQ handoff favors duplication over loss

If DLQ transfer outcome is uncertain, retry using deterministic source identity. Duplicate DLQ records are permitted under ambiguity; silent loss is not. Never mark the source complete merely because a DLQ write might have happened.

## I-24 — Membership changes do not lower safety

An empty/stale replica is not promoted to voter until it satisfies the catch-up rule and the membership transition is committed safely.

## I-25 — Controller truth is linearizable

Cluster membership, topic definitions, replica assignments, feature levels, and broker incarnation fencing have one controller-Raft committed order.

## I-26 — Bounded resource invariant

Every queue, buffer, lease table, retry structure, in-flight request window, decompression allocation, and background work queue has a configured or derived upper bound.

## I-27 — Control-plane starvation is forbidden

Data-plane overload cannot indefinitely prevent heartbeats, Raft progress, drain control, or health-critical control messages from being scheduled.

## I-28 — Version compatibility is explicit

A node must not emit a wire/disk feature version that the active cluster feature level has not enabled.

## I-29 — Benchmark provenance

Every published benchmark figure must be traceable to raw output, a git revision, configuration, and machine/environment metadata.

## I-30 — No success on ambiguous durable error

If storage reports an error and AstraLog cannot prove the requested durability condition was met, the client does not receive success.

## I-31 — No compaction resurrection

Once AstraLog declares a tombstone safely removable and removes it, no replica still eligible for leadership may later expose an older value for that key as the latest retained state.

## I-32 — Raft suffix immutability

Compaction never rewrites a range still required by Raft for log matching, voting up-to-date checks, or ordinary follower catch-up. Such rewriting is allowed only below a committed snapshot/compaction safety floor.

## I-33 — Queue lease state is durable before delivery authority

A queue receipt/lease is not authoritative until its assignment is quorum-committed in queue state. In-memory timers alone never establish ownership.

## I-34 — Bounded queue gap state

The number of message positions tracked ahead of a queue's durable completion cursor is bounded by a configured delivery window.

## I-35 — Controller committed-order uniqueness

All correct controllers apply the same committed controller metadata command at a given controller Raft log index.

## I-36 — Broker incarnation monotonicity

The greatest committed incarnation epoch for one broker ID never decreases.

## I-37 — Single broker authority

Only the process holding the greatest committed incarnation for a broker ID may exercise broker authority that is fenced by incarnation.

## I-38 — Assignment uniqueness

For one topic partition there is at most one current committed desired assignment generation in controller state.

## I-39 — Learner before voter

A newly added replica never becomes a voter until it has satisfied the catch-up rule and P3 has committed the safe membership transition.

## I-40 — Placement distinctness

One partition assignment never names the same broker more than once.

## I-41 — Required failure-domain placement

When rack/zone placement policy is Required, the controller never silently weakens the requested failure-domain constraint.

## I-42 — Metadata epoch monotonicity

A controller or broker metadata cache never applies a metadata epoch lower than its current applied epoch.

## I-43 — Movement resumability

Replica movement state is committed and reconciled so crash/restart cannot skip an unsafe movement stage merely because an RPC was previously sent.

## I-44 — Removal after committed exclusion

An old replica is not removed from authoritative assignment obligations until P3 has committed a stable voter configuration that excludes it.

## I-45 — Producer-ID non-reuse

A producer ID included in a committed controller allocation range is never returned by a later allocation, even when the original response is lost.

## I-46 — Feature-level monotonicity

The active cluster feature level never decreases implicitly and changes only through an explicit committed controller command.

## I-47 — Liveness is not authority

Heartbeat timing, suspicion, or failure-detector state alone never grants broker authority, partition leadership, or voter membership.

## I-48 — Controller outage isolation

Loss of controller quorum alone does not revoke already-valid P4 data-plane authority or require already-routed Produce/Fetch traffic to synchronously pass through the controller.
