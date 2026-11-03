# AstraLog P8-G Bounded Backpressure, Watermarks and Quotas Qualification Report

Date: 2026-10-07

Status: **P8-G CLOSURE CANDIDATE — EXACT-HEAD PASS NOTE REQUIRED FOR AUTHORITY**

## Scope

P8-G closes **P8-108 through P8-126 only**.

The authoritative predecessor is P8-F at:

    039c700b1b3787b81a310d1821f1eafb8e177800

That exact SHA carries the P8-F PASS Git note with the complete 604/604 exact-HEAD qualification.

P8-H benchmark tooling, P8-I final compatibility closure and all P9+ behavior remain disabled.

## Global broker data admission

The broker now owns a separate bounded data-admission gate with independent:

- in-flight request count;
- accounted request bytes;
- low memory watermark;
- high memory watermark.

Produce, Fetch and ListOffsets use the global data gate. Control/admin traffic remains outside that gate, so saturated data admission cannot consume the control-plane reserve. Existing per-connection frame, in-flight request and pending-response byte bounds remain unchanged and continue to apply independently.

Configuration fails closed unless all count/byte limits are finite and positive, the high watermark is strictly above the low watermark, the high watermark fits inside the global byte budget, and the global byte budget can admit one maximum Wire V1 frame.

## Watermark hysteresis

Pressure mode enters deterministically when admitted data bytes reach or cross the configured high watermark.

While pressure mode is active, new data-plane admission is rejected before task growth or mutation work begins.

Pressure mode clears only after retained data bytes fall **strictly below** the low watermark. Returning merely to the low watermark does not resume admission, preventing rapid oscillation.

Focused one-over tests prove rejected count/byte/pressure admissions do not grow the tracked state.

## Broker-local token buckets

P8-G adds bounded, broker-local performance shaping only; it does not create authenticated quota authority.

The quota gate owns:

- one anonymous/client traffic bucket;
- a bounded TopicId -> token-bucket table;
- finite anonymous and topic burst capacities;
- finite refill rates;
- a nonzero minimum throttle hint.

Refill uses only monotonic elapsed time. Topic bucket cardinality cannot exceed the configured maximum topic count. Missing buckets are created only after the complete quota decision is known to be admissible, so a rejected request cannot partially charge anonymous or existing topic buckets.

The request's accounted Wire V1 bytes are charged to the anonymous bucket and, when a topic scope exists, to each distinct topic bucket in canonical TopicId order.

## Pre-mutation throttle boundary

Response-bearing quota decisions execute before request dispatch/mutation.

Insufficient quota returns the existing retryable `BROKER_BUSY` surface with nonzero response `throttle_ms`. The request frame remains unchanged; request `throttle_ms` continues to be zero under the frozen Wire V1 rules.

Real broker tests prove both required mutation classes:

- CreateTopic requests are throttled before catalog mutation; the rejected topic name is absent from Metadata;
- Produce V0 quorum requests are throttled before log mutation; paginated committed Fetch returns every admitted value and never returns the rejected value.

The Produce proof deliberately paginates under the frozen Fetch `max_bytes` ceiling rather than widening the response merely for testing.

## acks=0 overload behavior

Produce V0 `acks=0` has no authoritative response channel and therefore receives no quota/throttle response.

It remains bounded by:

- maximum Wire V1 frame size;
- the existing per-connection in-flight permit bound;
- the global P8-G data-admission count/byte/watermark gate;
- bounded socket/decoder buffering and ordinary TCP flow control.

When those no-response admission bounds cannot accept more work, the request is not admitted and no delivery success is claimed. The frozen `acks=0` delivery-unreliable semantics are preserved.

A real broker pressure campaign holds the single configured data-admission slot with a long-poll Fetch, floods no-response Produce frames, and verifies that none of the overload-marked values become committed.

## Client throttle handling

Each broker connection now tracks the latest nonzero response throttle deadline.

Before sending another request or no-response frame on that connection, the client waits until the recorded deadline while remaining bounded by the caller's existing request deadline.

Later/larger throttle hints extend the deadline; an older waiter cannot clear a newer throttle. Zero hints do not shorten an existing throttle.

Focused client tests prove both the minimum advertised delay and extension behavior.

## Pressure and control-progress qualification

The real broker pressure campaign combines:

- a committed backlog;
- a slow long-poll consumer under a one-request global data-admission limit;
- a second connection attempting more data work;
- an admin Metadata request while the data gate is saturated;
- a no-response producer flood.

The second data request receives bounded `BROKER_BUSY` plus a nonzero throttle hint, while Metadata remains outside the data gate and completes successfully. The slow Fetch still observes the correct committed high watermark, and the overload producer values do not appear in committed history.

The RF3 deterministic simulation independently combines sustained quorum writes, one isolated/lagging follower, bounded committed-read pages, bounded per-follower replication windows and repeated Raft ticks. Quorum writes and control progress continue while the follower is isolated, then the follower catches up to the exact committed prefix after healing.

## Producer-faster-than-disk proof composition

P8-123 is intentionally qualified across the already-frozen adjacent P8 layers instead of introducing an artificial new disk-delay semantic:

- P8-D proves client accumulator admission happens before record copy and is bounded by total bytes, records and active partitions;
- P8-G proves broker data admission is bounded by global count/bytes/watermarks before mutation work;
- P8-E proves persistence staging is bounded by token count, logical bytes and maximum group delay, and its sustained append campaign preserves control progress while physical syncs remain finite.

The complete P8-G regression reruns those client and persistence tests together with the new broker pressure tests. Therefore producer pressure cannot create an unbounded client, broker or group-commit queue even when durable progress is the limiting layer.

## Slow-consumer and simultaneous-pressure proof

P8-124 is covered by the real long-poll/backlog campaign plus the frozen P8-F committed Fetch byte/record ceilings and existing per-connection pending-response budget.

P8-125 combines the real broker producer/consumer pressure campaign with the RF3 producer/follower/read-pressure simulation. Across the two deterministic layers, all relevant client/broker/fetch/replication ceilings remain finite and authoritative progress is unchanged.

P8-126 is closed by both sides of the control-starvation proof:

- broker control/admin traffic bypasses the global data-admission gate and remains serviceable under saturated data work;
- Raft ticks/control progress continue while follower replication and producer pressure are simultaneously active.

## Qualification evidence

Focused P8-G proofs:

- broker admission/watermark/quota/no-mutation/pressure tests: **11/11 PASS**;
- client throttle tests: **2/2 PASS**;
- combined RF3 producer/follower/consumer pressure simulation: **1/1 PASS**;
- focused total: **14/14 PASS**.

Complete bounded workspace qualification:

- broker units: **37/37 PASS**;
- P2 network: **9/9 PASS**;
- P4 RF3: **1/1 PASS**;
- P5/P8 dynamic: **5/5 PASS**;
- P6 RF3/group/offset/fencing/churn/public integration: **9/9 PASS**, serialized;
- P7 runtime: **2/2 PASS**;
- P7 failover: **12/12 PASS**, serialized;
- client: **24/24 PASS**;
- common: **3/3 PASS**;
- controller: **113/113 PASS**;
- groups: **55/55 PASS**;
- protocol: **45/45 PASS**;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4 PASS**;
- Raft: **128/128 PASS**;
- replication: **101/101 PASS**;
- storage: **53/53 PASS**.

Total:

    618/618 PASS

Mechanical/frozen compatibility:

- `cargo fmt --all -- --check`: **PASS**;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**;
- workspace doc tests: **PASS**;
- `git diff --check`: **PASS**;
- Cargo manifests/lockfile unchanged from P8-F authority;
- original P2/P6/P7 golden sources unchanged;
- Storage test/golden surface unchanged.

## Closure boundary

P8-108 through P8-126 are mechanically satisfied on this pre-commit closure candidate.

P8-G becomes authoritative only after:

1. this exact bounded source/document set is committed as one P8-G checkpoint;
2. the committed clean HEAD repeats the focused, compatibility, complete **618/618**, and mechanical/frozen gates; and
3. that exact SHA receives the P8-G PASS Git note.

Only then may **P8-H — beginning at P8-127 benchmark harness/baseline work** start. P8-I and later phases remain disabled.
