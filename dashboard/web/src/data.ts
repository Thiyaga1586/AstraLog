export type Broker = {
  id: number
  host: string
  port: number
  incarnationEpoch?: number
}

export type Partition = {
  id: number
  leaderId: number | null
  leaderEpoch?: number
  replicaIds: number[]
  errorCode?: number
}

export type Topic = {
  id?: string
  name: string
  replicationFactor?: number
  partitions: Partition[]
}

export type Cluster = {
  id: string
  metadataEpoch: number
  brokers: Broker[]
  topics: Topic[]
}

export type AdapterState = {
  state: 'connected' | 'disconnected' | 'unavailable'
  bootstrap: string
  observed_at_ms?: number | null
  last_success_at_ms?: number | null
  error?: string | null
}

export type ApiSnapshot = {
  adapter: AdapterState
  cluster: Record<string, unknown> | null
}

const toNumber = (value: unknown, fallback = 0): number => {
  const number = Number(value)
  return Number.isFinite(number) ? number : fallback
}

const objectValue = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' ? (value as Record<string, unknown>) : {}

const arrayValue = (value: unknown): unknown[] => Array.isArray(value) ? value : []

// Translate the thin HTTP adapter response into presentation data.
// Unknown leader IDs remain unknown. Never infer a current leader from a replica list.
export function normalizeCluster(raw: Record<string, unknown> | null): Cluster | null {
  if (!raw) return null
  const brokers = arrayValue(raw.brokers).map((entry) => {
    const broker = objectValue(entry)
    return {
      id: toNumber(broker.id ?? broker.broker_id),
      host: String(broker.host ?? ''),
      port: toNumber(broker.port),
      incarnationEpoch: broker.incarnation_epoch === undefined
        ? undefined : toNumber(broker.incarnation_epoch),
    }
  })

  const topics = arrayValue(raw.topics).map((entry) => {
    const topic = objectValue(entry)
    const partitions = arrayValue(topic.partitions).map((part) => {
      const p = objectValue(part)
      const leaderCandidate = p.leader_id ?? p.leaderId ?? p.leader_broker_id
      const leaderNumber = leaderCandidate === null || leaderCandidate === undefined
        ? null : toNumber(leaderCandidate, -1)
      // AstraLog uses u32::MAX for unknown broker IDs on the wire.
      const leaderId = leaderNumber === -1 || leaderNumber === 4294967295 ? null : leaderNumber
      return {
        id: toNumber(p.id ?? p.partition),
        leaderId,
        leaderEpoch: p.leader_epoch == null ? undefined : toNumber(p.leader_epoch),
        replicaIds: arrayValue(p.replica_ids ?? p.replicaIds ?? p.replica_broker_ids)
          .map((id) => toNumber(id)),
        errorCode: p.error_code === undefined && p.partition_error === undefined
          ? undefined : toNumber(p.error_code ?? p.partition_error),
      } satisfies Partition
    })
    return {
      name: String(topic.name ?? topic.topic_name ?? 'unknown'),
      id: topic.id === undefined && topic.topic_id === undefined
        ? undefined : String(topic.id ?? topic.topic_id),
      replicationFactor: topic.replication_factor === undefined
        ? undefined : toNumber(topic.replication_factor),
      partitions,
    } satisfies Topic
  })

  return {
    id: String(raw.cluster_id ?? raw.clusterId ?? ''),
    metadataEpoch: toNumber(raw.metadata_epoch ?? raw.metadataEpoch),
    brokers,
    topics,
  }
}

// Purely illustrative data. The UI always identifies this mode as a preview.
// These brokers are NOT started and this is NOT a live AstraLog snapshot.
export const previewCluster: Cluster = {
  id: 'preview-cluster-local',
  metadataEpoch: 42,
  brokers: [
    { id: 1, host: '127.0.0.1', port: 7001 },
    { id: 2, host: '127.0.0.1', port: 7002 },
    { id: 3, host: '127.0.0.1', port: 7003 },
  ],
  topics: [
    { name: 'orders', replicationFactor: 3, partitions: [
      { id: 0, leaderId: 1, leaderEpoch: 8, replicaIds: [1, 2, 3] },
      { id: 1, leaderId: 2, leaderEpoch: 6, replicaIds: [1, 2, 3] },
      { id: 2, leaderId: 3, leaderEpoch: 9, replicaIds: [1, 2, 3] },
      { id: 3, leaderId: 1, leaderEpoch: 7, replicaIds: [1, 2, 3] },
    ] },
    { name: 'payments', replicationFactor: 3, partitions: [
      { id: 0, leaderId: 2, leaderEpoch: 5, replicaIds: [1, 2, 3] },
      { id: 1, leaderId: 3, leaderEpoch: 4, replicaIds: [1, 2, 3] },
      { id: 2, leaderId: 1, leaderEpoch: 5, replicaIds: [1, 2, 3] },
    ] },
    { name: 'notifications', replicationFactor: 3, partitions: [
      { id: 0, leaderId: 3, leaderEpoch: 3, replicaIds: [1, 2, 3] },
      { id: 1, leaderId: 1, leaderEpoch: 4, replicaIds: [1, 2, 3] },
    ] },
  ],
}

export const benchmark = {
  source: 'docs/benchmarks/remediation_20261103/SUMMARY.md',
  sourceSha: '35190af265ad5ce9c4e23278785e0a3bd0a7ecdf',
  throughput: [
    { mode: 'Without batching', rate: 10.71, p95: 480.5 },
    { mode: 'With batching', rate: 33.31, p95: 138.83 },
  ],
  otherProfiles: [
    { mode: 'RF1 leader', unbuffered: 76.36 },
    { mode: 'RF1 quorum', unbuffered: 68.03 },
    { mode: 'RF3 leader', unbuffered: 10.12, buffered: 39.60 },
    { mode: 'RF3 quorum', unbuffered: 10.71, buffered: 33.31 },
  ],
  records: 1232,
  latencySamples: 2464,
  errors: 0,
}

export function clusterStats(cluster: Cluster) {
  const partitions = cluster.topics.flatMap((topic) => topic.partitions)
  const leaders = partitions.filter((partition) => partition.leaderId !== null)
  return {
    brokerCount: cluster.brokers.length,
    topicCount: cluster.topics.length,
    partitionCount: partitions.length,
    leaderCount: leaders.length,
    missingLeaderCount: partitions.length - leaders.length,
  }
}
