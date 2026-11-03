import { useMemo, useState } from 'react'
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Database, HardDrive, Network } from 'lucide-react'
import type { Cluster, Topic } from './data'

type BrokerNode = Node<{ id: number; host: string; port: number; leaderCount: number; replicaCount: number; selected: boolean }, 'broker'>
type TopicNode = Node<{ topic: string; partitions: number; replicaCount: number }, 'topic'>

function BrokerView({ data }: NodeProps<BrokerNode>) {
  return (
    <div className={`flow-broker ${data.selected ? 'flow-selected' : ''}`}>
      <Handle type="target" position={Position.Top} className="flow-handle" />
      <div className="flow-broker-top">
        <span className="flow-mini-icon"><HardDrive size={17} /></span>
        <span className="flow-broker-status">REGISTERED</span>
      </div>
      <div className="flow-broker-name">Broker {data.id}</div>
      <div className="flow-broker-host">{data.host}:{data.port}</div>
      <div className="flow-broker-footer">
        <span><strong>{data.leaderCount}</strong> leader</span>
        <span><strong>{data.replicaCount}</strong> replica</span>
      </div>
    </div>
  )
}

function TopicView({ data }: NodeProps<TopicNode>) {
  return (
    <div className="flow-topic">
      <span className="flow-topic-icon"><Database size={19} /></span>
      <div>
        <div className="flow-topic-name">{data.topic}</div>
        <div className="flow-topic-sub">{data.partitions} partitions · RF {data.replicaCount || '?'}</div>
      </div>
      <Handle type="source" position={Position.Bottom} className="flow-handle" />
    </div>
  )
}

const nodeTypes = { broker: BrokerView, topic: TopicView }

function buildGraph(cluster: Cluster, topic: Topic | undefined, selectedBroker: number | null) {
  const nodes: Node[] = []
  const edges: Edge[] = []
  if (!topic) return { nodes, edges, hiddenBrokers: 0 }
  const displayBrokers = cluster.brokers.slice(0, 12)
  const width = (displayBrokers.length - 1) * 248
  nodes.push({
    id: 'topic',
    type: 'topic',
    position: { x: width / 2 - 110, y: 0 },
    data: { topic: topic.name, partitions: topic.partitions.length, replicaCount: topic.replicationFactor ?? 0 },
    draggable: false,
  } satisfies TopicNode)

  for (const [i, broker] of displayBrokers.entries()) {
    const leaderCount = topic.partitions.filter((p) => p.leaderId === broker.id).length
    const replicaCount = topic.partitions.filter((p) => p.replicaIds.includes(broker.id)).length
    nodes.push({
      id: `broker-${broker.id}`,
      type: 'broker',
      position: { x: i * 248, y: 216 },
      data: { id: broker.id, host: broker.host, port: broker.port, leaderCount, replicaCount, selected: selectedBroker === broker.id },
      draggable: false,
    } satisfies BrokerNode)
    if (leaderCount > 0 || replicaCount > 0) {
      edges.push({
        id: `topic-to-broker-${broker.id}`,
        source: 'topic',
        target: `broker-${broker.id}`,
        type: 'smoothstep',
        animated: false,
        label: `${leaderCount} leaders / ${replicaCount} replicas`,
        labelStyle: { fill: '#a6b2c7', fontSize: 10 },
        labelBgStyle: { fill: '#111b2a', fillOpacity: 0.95 },
        labelBgPadding: [6, 4],
        style: { stroke: leaderCount ? '#699cff' : '#476078', strokeWidth: leaderCount ? 2.4 : 1.5 },
        markerEnd: { type: MarkerType.ArrowClosed, color: leaderCount ? '#699cff' : '#476078' },
      })
    }
  }
  return { nodes, edges, hiddenBrokers: cluster.brokers.length - displayBrokers.length }
}

export function Topology({ cluster, expanded = false }: { cluster: Cluster; expanded?: boolean }) {
  const [topicName, setTopicName] = useState('')
  const [selectedBroker, setSelectedBroker] = useState<number | null>(null)
  const topic = cluster.topics.find((value) => value.name === topicName) ?? cluster.topics[0]
  const { nodes, edges, hiddenBrokers } = useMemo(() => buildGraph(cluster, topic, selectedBroker), [cluster, topic, selectedBroker])
  const selectedDetails = cluster.brokers.find((broker) => broker.id === selectedBroker)

  return (
    <section className={`panel topology-panel ${expanded ? 'topology-expanded' : ''}`}>
      <header className="panel-head">
        <div>
          <div className="eyebrow"><Network size={13} /> INTERACTIVE TOPOLOGY</div>
          <h2>Replication map</h2>
          <p>Read-only metadata, not a health or heartbeat measurement.</p>
        </div>
        <div className="topic-select-wrap">
          <label htmlFor={`topology-topic-${expanded ? 'large' : 'small'}`}>Topic</label>
          <select id={`topology-topic-${expanded ? 'large' : 'small'}`} value={topic?.name ?? ''} onChange={(event) => { setTopicName(event.target.value); setSelectedBroker(null) }}>
            {cluster.topics.map((value) => <option key={value.name} value={value.name}>{value.name}</option>)}
          </select>
        </div>
      </header>
      <div className="topology-canvas" role="img" aria-label="Interactive diagram connecting a topic to its registered replica brokers">
        {topic ? (
          <ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.35, maxZoom: 1.3 }} minZoom={0.25} maxZoom={1.65}
            nodesConnectable={false} nodesDraggable={false} edgesReconnectable={false} proOptions={{ hideAttribution: true }}
            onNodeClick={(_, node) => {
              if (node.id.startsWith('broker-')) setSelectedBroker(Number(node.id.slice(7)))
            }}>
            <Background color="#223044" gap={24} size={1} />
            <Controls position="bottom-left" showInteractive={false} />
            {expanded && cluster.brokers.length > 6 && (
              <MiniMap
                ariaLabel="Cluster navigation overview"
                pannable
                zoomable
                nodeColor={(node) => node.type === 'topic' ? '#a18df9' : '#5b93f8'}
                maskColor="rgba(12, 20, 33, 0.68)"
                style={{ width: 148, height: 94 }}
              />
            )}
          </ReactFlow>
        ) : (
          <div className="empty-diagram"><Database size={28} /><strong>No topics in this snapshot</strong><span>The broker has not reported any topics.</span></div>
        )}
      </div>
      <footer className="topology-footer">
        <span className="legend-dot legend-leader" /> Leader count
        <span className="legend-dot legend-replica" /> Replica assignment
        <span className="topology-info">{hiddenBrokers > 0 ? `Showing 12 of ${cluster.brokers.length} brokers` : 'Select a broker to inspect'}</span>
      </footer>
      {selectedDetails && (
        <div className="broker-inspector">
          <div><strong>Broker {selectedDetails.id}</strong><span>{selectedDetails.host}:{selectedDetails.port}</span></div>
          <button onClick={() => setSelectedBroker(null)} aria-label="Close broker inspector">Close</button>
          <p>Assignments listed here come from cluster metadata. Broker reachability is not independently checked.</p>
        </div>
      )}
    </section>
  )
}
