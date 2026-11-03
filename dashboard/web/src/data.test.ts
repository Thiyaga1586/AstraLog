import { describe, expect, it } from 'vitest'
import { benchmark, clusterStats, normalizeCluster, previewCluster } from './data'

describe('AstraLog Metadata V0 adapter normalization', () => {
  it('maps real snake_case metadata to the dashboard with exact leader and replica IDs', () => {
    const cluster = normalizeCluster({
      cluster_id: 'aabb0011',
      metadata_epoch: 21,
      brokers: [{ broker_id: 7, host: '127.0.0.1', port: 7007, incarnation_epoch: 3 }],
      topics: [{
        name: 'orders', topic_id: '001122', replication_factor: 3,
        partitions: [{
          partition: 4,
          leader_broker_id: 7,
          leader_epoch: 9,
          replica_broker_ids: [7, 8, 9],
          error_code: 0,
        }],
      }],
    })
    expect(cluster?.id).toBe('aabb0011')
    expect(cluster?.metadataEpoch).toBe(21)
    expect(cluster?.brokers[0].id).toBe(7)
    expect(cluster?.brokers[0].incarnationEpoch).toBe(3)
    expect(cluster?.topics[0].partitions[0]).toMatchObject({
      id: 4,
      leaderId: 7,
      leaderEpoch: 9,
      replicaIds: [7, 8, 9],
    })
    expect(cluster && clusterStats(cluster).leaderCount).toBe(1)
  })

  it('preserves missing leaders, never choosing one from the replicas', () => {
    const cluster = normalizeCluster({
      cluster_id: 'unavailable', metadata_epoch: 1, brokers: [],
      topics: [{ name: 'demo', partitions: [
        { partition: 0, leader_broker_id: null, leader_epoch: null, replica_broker_ids: [1, 2, 3] },
        { partition: 1, leader_broker_id: 4294967295, replica_broker_ids: [1, 2, 3] },
      ] }],
    })
    expect(cluster?.topics[0].partitions.map((partition) => partition.leaderId)).toEqual([null, null])
    expect(cluster?.topics[0].partitions[0].leaderEpoch).toBeUndefined()
    expect(cluster && clusterStats(cluster).missingLeaderCount).toBe(2)
  })

  it('keeps null snapshots disconnected rather than fabricating a cluster', () => {
    expect(normalizeCluster(null)).toBeNull()
  })
})

describe('Preview and benchmark provenance', () => {
  it('keeps preview data separate from the actual HTTP response', () => {
    expect(previewCluster.id).toBe('preview-cluster-local')
    expect(previewCluster.brokers).toHaveLength(3)
  })

  it('uses documented historical RF3 quorum measurements', () => {
    expect(benchmark.source).toMatch(/remediation_20261103\/SUMMARY.md/)
    expect(benchmark.throughput.map((point) => point.rate)).toEqual([10.71, 33.31])
    expect(benchmark.throughput.map((point) => point.p95)).toEqual([480.5, 138.83])
    expect(benchmark.latencySamples).toBe(2464)
  })
})
