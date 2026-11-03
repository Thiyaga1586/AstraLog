# AstraLog P5-G Heartbeat / Failure Detector Qualification

Status: **QUALIFIED**

P5-G implements actions P5-057 through P5-064 only. Topic placement, metadata
dissemination, dynamic P4 assignments and movement orchestration remain disabled.

## Delivered scope

### Bounded heartbeat model

BrokerHeartbeatRequest contains:

- committed BrokerAuthority: broker ID + node UUID + incarnation epoch;
- broker-applied metadata epoch;
- advertised min/max feature level;
- strictly ordered bounded ReplicaProgressReport entries;
- strictly ordered bounded MovementProgressReport entries.

HeartbeatLimits bounds replica and movement report counts. Replica progress
rejects a committed Raft index beyond its durable index. Zero/reserved identity
fields, invalid feature ranges, duplicate/non-canonical progress entries and
oversized arrays fail closed.

P5-G deliberately does not return metadata deltas or assignment changes. Those
remain P5-I/J responsibilities.

### Committed incarnation fencing

Only the current controller leader accepts a heartbeat. Before updating any
volatile liveness state, ControllerRuntime:

1. validates the heartbeat shape and bounds;
2. passes BrokerAuthority through the qualified P5-F committed-state gate;
3. verifies the broker feature range still matches its committed BrokerRecord;
4. rejects an applied metadata epoch ahead of controller authority.

A stale incarnation or foreign UUID therefore cannot refresh last-seen.

### Volatile leader-local liveness

FailureDetector is in-memory controller runtime state only. It contains:

- validated suspicion thresholds;
- an optional injected logical detector tick;
- a deterministic BTreeMap of broker ID -> authority + last-seen tick.

Heartbeat and failure-detector tick operations never call ControllerRuntime::propose,
do not create ControllerCommand entries, do not touch P3 persistence and do not
advance metadata_epoch.

### Deterministic injected time

Failure-detector time is never derived from wall clock or the P3 election timer.
The current controller leader receives an explicit u64 logical tick through
failure_detector_tick. Equal ticks are idempotent; backward ticks fail closed.

FailureDetectorConfig requires:

~~~text
0 < suspect_after_ticks < unavailable_after_ticks <= 1_000_000_000
~~~

The default classification is:

~~~text
elapsed < 3       -> Healthy
3 <= elapsed < 10 -> Suspect
elapsed >= 10      -> Unavailable
no observation     -> Unknown
~~~

### Leadership reset

When the local P3 role enters or leaves Leader, ControllerRuntime clears the
failure-detector clock and all last-seen observations. The replacement leader
therefore starts with Unknown liveness even though it retains the fully replicated
BrokerRecord/incarnation state.

The real three-controller test proves an old leader can have tick 110 while its
replacement safely starts a new detector epoch at tick 1. No suspicion age is
inherited across controller leadership.

### Suspicion has no replicated authority

Liveness is advisory. Driving a broker from Healthy to Suspect to Unavailable:

- does not change BrokerRecord;
- does not change broker lifecycle;
- does not change broker incarnation;
- does not advance metadata_epoch;
- does not make the P5-F BrokerAuthority gate reject an otherwise current ACTIVE
  broker.

Only an explicit committed controller mutation can change replicated authority.

### Planner-facing observation

ControllerRuntime exposes broker_liveness and planner_liveness_snapshot on the
current leader. BrokerLivenessObservation reports Unknown, Healthy, Suspect or
Unavailable plus last-seen/elapsed ticks.

available_for_new_placement is true only for Healthy liveness. Unknown, Suspect
and Unavailable are false. This is the P5-G input contract for P5-H; no placement
algorithm is implemented in this checkpoint.

## Real three-controller qualification

The P5-G integration scenario uses the real P5-D TCP controller quorum and P5-C
disk persistence. It exercises:

1. broker registration and committed JOINING -> ACTIVE;
2. follower heartbeat rejection;
3. leader detector tick initialization;
4. valid heartbeat -> Healthy;
5. stale-incarnation heartbeat rejection without last-seen refresh;
6. injected tick transition to Suspect;
7. injected tick transition to Unavailable;
8. planner liveness marks Unavailable as not available for new placement;
9. BrokerAuthority and ACTIVE lifecycle remain valid while Unavailable;
10. heartbeat refresh returns to Healthy without metadata mutation;
11. controller leader shutdown and higher-term failover;
12. replacement leader starts Unknown with no inherited last-seen/tick;
13. replacement leader accepts fresh detector tick 1 and heartbeat -> Healthy;
14. old controller restarts from disk with replicated BrokerRecord intact.

The scenario passed **5/5 consecutive focused runs**.

## Qualification evidence

Pre-commit strict qualification:

- cargo fmt --all -- --check: PASS
- workspace Clippy with -D warnings: PASS
- astralog-controller: 60 passed, 0 failed
- astralog-raft: 120 passed, 0 failed
- astralog-replication: 46 passed, 0 failed
- astralog-storage: 43 passed, 0 failed
- protocol unit tests: 33 passed, 0 failed
- protocol goldens: 11 passed, 0 failed
- full workspace: 332 passed, 0 failed
- all doc tests: PASS
- P1-P4 production-source diff from P5-F: empty
- pre-existing P5-D lost-response integration test was made semantically precise: it now proves the mutation is quorum-committed/applied by observing the current leader's metadata epoch before dropping the unread response, rather than requiring unrelated full-follower convergence; this is test-only and changes no controller behavior
- pre-existing P4 RF=3 TCP qualification keeps a 30-second CI observation window for leader/failover discovery; this is test-only and changes no P4 broker, replication or P3 election timing
- git diff --check: PASS

## Scope boundary

P5-G does not implement topic placement, TopicId allocation workflow, metadata
deltas, broker-side heartbeat scheduling/network protocol, dynamic P4 assignment,
replica movement orchestration, automatic lifecycle fencing, or broker removal on
timeout. Those remain later P5 checkpoints.

The next bounded checkpoint is P5-H topic metadata and deterministic placement.
