# P1 Closure Report

Phase: **P1 — Single-Node Durable Log Engine**

Result: **PASS — Storage Format V1 frozen**

Qualification date: **2026-09-23**

## Scope

P1 implemented only the single-node durable storage foundation required by the frozen architecture. It did **not** implement networking, the Raft state machine, cluster metadata, consumer groups, work queues, compression execution, or P2+ behavior.

## Frozen artifacts

- `docs/FORMAT_V1.md`
- segment-header V1
- partition-metadata V1
- data/control frame V1
- record-set V1
- offset/time/Raft index V1
- copy-on-write segment-manifest V1
- ten immutable hexadecimal golden byte fixtures under `crates/astralog-storage/tests/golden/`

Future incompatible byte changes require a new format version. V1 golden vectors do not change.

## Implementation delivered

### Storage vocabulary

Strong checked types exist for topic ID, partition ID, stream offset, segment base offset, Raft index, and Raft term. Raft index zero is reserved, and arithmetic that could wrap uses checked operations.

### Frame codec and integrity

- data batches and generic control entries
- 96-byte V1 frame header
- CRC32C / Castagnoli validation
- strict version/magic/reserved-field checks
- length validation before allocation
- nullable key/value/header semantics
- producer identity/epoch/sequence fields reserved in V1
- compression IDs reserved while P1 writers accept only uncompressed payloads

### Segment I/O

- crash-safe segment creation through a durable temporary file and rename
- sequential append
- tracked byte position
- partial-write failure rollback to the prior known-valid boundary
- writer poisoning if rollback itself cannot be proven
- data/full durability flushes
- size and time roll hooks
- filename identity containing base stream offset, base Raft index, and generation
- sequential scanning
- direct frame-at-position reads

### Derived indexes

- sparse stream-offset index
- monotonic time index
- sparse Raft-index index
- CRC/version/identity/range validation
- complete rebuild from `.log` bytes
- missing or stale index recovery

Indexes are never authoritative.

### Partition log and recovery

- active plus immutable rolled segment inventory
- append/read/roll APIs
- separate logical stream offsets and Raft indexes
- persisted committed-boundary placeholder
- local durable boundary cannot be advanced by an append alone
- restart does not treat an uncommitted visible tail as proven durable
- invalid uncommitted active tail may be truncated
- damage intersecting the committed boundary is a hard committed-corruption error
- damaged rolled segments are never silently repaired

### Copy-on-write generation primitive

- staged temporary files
- per-file durability before publication
- parent-directory synchronization on Linux
- atomic manifest publication
- immutable-file size validation
- at most one mutable active entry
- path-traversal rejection
- monotonically increasing generation numbers
- stale generation transactions cannot overwrite a newer manifest
- files prepared/renamed before the manifest switch remain non-authoritative

This is only the safe replacement/publication primitive. P1's live `PartitionLog` does not yet use `segments.manifest` as its inventory source, and using `GenerationTransaction` against a live P1 partition directory is unsupported. P9 owns integration of manifest-selected generations together with actual retention/compaction policy.

## Qualification evidence

Qualification used the pinned Linux container `rust:1.98.1`.

```text
cargo fmt --all -- --check
PASS

cargo clippy --workspace --all-targets --all-features -- -D warnings
PASS

cargo test --workspace --all-features
astralog-common:   2 passed, 0 failed
astralog-storage: 39 passed, 0 failed
TOTAL:            41 passed, 0 failed
doc tests:         PASS
```

`git diff --check` also passed before closure.

## Adversarial proof coverage

The P1 suite includes:

- every partial-byte truncation boundary of a representative encoded frame
- every single-bit mutation of a representative checksummed frame
- explicit CRC corruption
- impossible frame-length rejection
- 10,000 deterministic arbitrary-byte decoder cases with a no-panic oracle
- simulated append/ENOSPC-style failure
- simulated partial append followed by rollback and successful retry
- simulated durability-flush failure
- proof that commit cannot outrun local durable flush
- proof that restart does not promote uncommitted tail bytes to durable state
- committed corruption rejection
- uncommitted active-tail truncation
- missing-index rebuild
- stale-index rebuild
- restart across controlled append and segment-roll stages
- control-only segment-roll naming, preventing stream-offset filename collisions
- manifest CRC corruption
- manifest path traversal
- immutable manifest size mismatch
- staged-file-before-switch behavior
- renamed-file-before-switch behavior
- prepared-manifest-temp-before-switch behavior
- generation sequencing
- stale generation transaction fencing

## Correctness conclusions

### No silent corruption

CRC/format failures are classified. Recovery never scans ahead for a later magic marker and never guesses how to repair committed bytes.

### Committed data boundary is protected

An active-tail truncation is legal only beyond the persisted committed Raft index. Corruption at or before that boundary fails recovery.

### Durability is not confused with visibility

An append updates the logical end but not the local durable index. A durability flush advances the durable index. After restart, only the persisted committed boundary is initially treated as proven durable; a surviving uncommitted tail must be flushed again before the placeholder commit API can advance through it.

### Partial writes do not create a reusable poisoned suffix

If an append reports an I/O failure after writing a prefix, the writer truncates back to the previous valid boundary and synchronizes that rollback before allowing reuse. If rollback cannot be proven, the writer is poisoned and refuses further durable operations.

### Index loss is data-safe

All index forms are derived from the checksummed log and may be deleted/rebuilt without losing authoritative records.

### Generation publication is fenced

Only an atomically published manifest selects a replacement generation. Generation numbers advance by exactly one and a transaction based on an older manifest cannot overwrite a newer committed manifest.

## Known limitations intentionally deferred

- P1 contains Raft index/term storage fields but no Raft algorithm.
- The committed boundary is a storage placeholder; P3/P4 will supply real consensus commit advancement.
- LZ4 and Zstandard identifiers exist in V1 but compression execution is deferred to P8.
- The generation primitive exists, but retention and compaction policy are deferred to P9.
- Manifest-governed partition inventory is also deferred to P9; P1 qualifies the generation primitive independently rather than mixing it into the normal generation-0 partition path early.
- Directory-fsync durability is production-defined for Linux; Windows is a development environment and does not receive the same P1 power-loss durability claim.
- P1 does not claim protection against a storage device that violates the operating-system/device persistence contract.
- No performance claims are made in P1.

## Exit gate

- [x] no silent corruption path
- [x] incomplete uncommitted active tails recover deterministically
- [x] committed-region corruption is surfaced
- [x] indexes are fully rebuildable
- [x] V1 golden compatibility vectors exist and byte-match encoders/decoders
- [x] deterministic fuzz/adversarial suite is green
- [x] strict Clippy is green with warnings denied
- [x] rustfmt check is green
- [x] P2 code has not started

**P1 is CLOSED. Storage Format V1 is FROZEN.**
