import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Activity, ArrowDownRight, ArrowRight, ArrowUpRight, BarChart3, BookOpen,
  Box, ChevronRight, CircleAlert, Clock3, Database, ExternalLink,
  FileText, GitBranch, HardDrive, Layers, LayoutDashboard, LockKeyhole,
  Menu, Network, RefreshCw, Search, Server, ShieldCheck, Terminal,
  Wifi, WifiOff, Workflow, X, Zap,
  type LucideIcon,
} from 'lucide-react'
import {
  benchmark, clusterStats, normalizeCluster, previewCluster,
  type AdapterState, type ApiSnapshot, type Cluster,
} from './data'

const Topology = lazy(async () => ({ default: (await import('./Topology')).Topology }))
const BenchmarkMiniChart = lazy(async () => ({ default: (await import('./Charts')).BenchmarkMiniChart }))
const BenchmarkLargeChart = lazy(async () => ({ default: (await import('./Charts')).BenchmarkLargeChart }))

type View = 'overview' | 'topology' | 'topics' | 'benchmarks' | 'architecture'
type Mode = 'live' | 'preview'

const pageItems: { id: View; label: string; description: string; icon: LucideIcon }[] = [
  { id: 'overview', label: 'Overview', description: 'Cluster at a glance', icon: LayoutDashboard },
  { id: 'topology', label: 'Topology', description: 'Replication diagram', icon: Network },
  { id: 'topics', label: 'Topics & partitions', description: 'Metadata inventory', icon: Layers },
  { id: 'benchmarks', label: 'Benchmarks', description: 'Measured performance', icon: BarChart3 },
  { id: 'architecture', label: 'Architecture', description: 'How it works', icon: Workflow },
]

const GITHUB = 'https://github.com/Thiyaga1586/AstraLog'
const docs = `${GITHUB}/blob/main/docs`

function compactId(value: string) {
  if (!value) return 'Not available'
  if (value.length <= 20) return value
  return `${value.slice(0, 11)}...${value.slice(-5)}`
}

function relativeTime(timestamp: number | null | undefined, now: number) {
  if (!timestamp || !Number.isFinite(timestamp)) return 'No successful poll yet'
  const seconds = Math.max(0, Math.floor((now - timestamp) / 1000))
  if (seconds < 5) return 'just now'
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  return `${Math.floor(seconds / 3600)}h ago`
}

function Tag({ children, kind = 'neutral' }: { children: React.ReactNode; kind?: 'neutral' | 'positive' | 'warning' | 'violet' }) {
  return <span className={`tag tag-${kind}`}>{children}</span>
}

function StatusIndicator({ mode, state }: { mode: Mode; state: AdapterState['state'] | 'loading' }) {
  if (mode === 'preview') return <Tag kind="violet"><span className="status-dot" /> PREVIEW DATA</Tag>
  if (state === 'connected') return <Tag kind="positive"><span className="status-dot" /> LIVE METADATA</Tag>
  if (state === 'unavailable') return <Tag kind="warning"><span className="status-dot" /> STALE SNAPSHOT</Tag>
  return <Tag kind="warning"><span className="status-dot" /> {state === 'loading' ? 'CONNECTING' : 'DISCONNECTED'}</Tag>
}

function StatCard({ label, value, sub, icon: Icon, index = 0 }: { label: string; value: string | number; sub: string; icon: LucideIcon; index?: number }) {
  return (
    <div className={`stat-card stat-${index}`}>
      <div className="stat-card-upper"><span>{label}</span><Icon size={19} strokeWidth={1.7} /></div>
      <div className="stat-card-number">{value}</div>
      <div className="stat-card-sub">{sub}</div>
    </div>
  )
}

function ReadOnlyNote({ preview }: { preview: boolean }) {
  return (
    <div className="disclosure-note">
      <ShieldCheck size={17} />
      <span>{preview
        ? 'Illustrative topology. Preview brokers, topics, epochs and assignments are not real cluster observations.'
        : 'Read-only metadata. Registered brokers are not independently health-checked. No mutations or failure injection are performed.'}</span>
    </div>
  )
}

function BenchmarkSummary({ large = false, navigate }: { large?: boolean; navigate?: (view: View) => void }) {
  return (
    <section className={`panel benchmark-summary ${large ? 'benchmark-large' : ''}`}>
      <header className="panel-head">
        <div>
          <div className="eyebrow"><BarChart3 size={13} /> RECORDED BENCHMARK</div>
          <h2>Batching changes the equation.</h2>
          <p>RF3 quorum writes, one Windows laptop, November 3, 2026.</p>
        </div>
        <Tag>HISTORICAL</Tag>
      </header>
      <div className="benchmark-header-row">
        <div>
          <div className="benchmark-super">33.31 <span>records/s</span></div>
          <div className="benchmark-gain"><ArrowUpRight size={14} /> 3.11x throughput with buffering</div>
        </div>
        <div className="benchmark-p95">
          <span>Produce p95</span>
          <strong>138.83 <small>ms</small></strong>
          <em><ArrowDownRight size={13} /> from 480.50 ms</em>
        </div>
      </div>
      <Suspense fallback={<div className="lazy-visual-loading">Loading chart...</div>}><BenchmarkMiniChart /></Suspense>
      <footer className="panel-footer">
        <span>1 KiB records · 4 producers · 2 short repeats</span>
        {navigate && <button className="text-button" onClick={() => navigate('benchmarks')}>View evidence <ArrowRight size={14} /></button>}
      </footer>
    </section>
  )
}

function TopicTable({ cluster, compact = false }: { cluster: Cluster; compact?: boolean }) {
  const [query, setQuery] = useState('')
  const [showInternal, setShowInternal] = useState(true)
  const topics = cluster.topics.filter((topic) => {
    return topic.name.toLowerCase().includes(query.toLowerCase()) && (showInternal || !topic.name.startsWith('__'))
  })
  const rows = compact ? topics.slice(0, 5) : topics

  return (
    <div className="topic-table-wrap">
      {!compact && (
        <div className="table-toolbar">
          <div className="search-box"><Search size={16} /><input aria-label="Filter topics" placeholder="Search by topic name" value={query} onChange={(event) => setQuery(event.target.value)} /></div>
          <label className="checkbox-label"><input type="checkbox" checked={showInternal} onChange={(event) => setShowInternal(event.target.checked)} /> Internal topics</label>
        </div>
      )}
      <table className="topic-table">
        <thead><tr><th>Topic</th><th>Partitions</th><th>Replication</th><th>Known leaders</th><th>Metadata</th></tr></thead>
        <tbody>
          {rows.map((topic) => {
            const known = topic.partitions.filter((part) => part.leaderId !== null).length
            const rf = topic.replicationFactor ?? topic.partitions[0]?.replicaIds.length ?? 0
            return <tr key={topic.name}>
              <td><div className="topic-name"><span className="topic-icon"><Database size={15} /></span><span>{topic.name}</span>{topic.name.startsWith('__') && <Tag>INTERNAL</Tag>}</div></td>
              <td>{topic.partitions.length}</td><td>RF {rf || '?'}</td><td>{known} / {topic.partitions.length}</td>
              <td><Tag kind={known === topic.partitions.length ? 'positive' : 'warning'}>{known === topic.partitions.length ? 'ASSIGNED' : 'UNKNOWN LEADER'}</Tag></td>
            </tr>
          })}
        </tbody>
      </table>
      {rows.length === 0 && <div className="empty-table">No topics match your filter.</div>}
      {compact && topics.length > rows.length && <div className="table-more">+ {topics.length - rows.length} more topics</div>}
    </div>
  )
}

function Overview({ cluster, preview, navigate }: { cluster: Cluster; preview: boolean; navigate: (view: View) => void }) {
  const stats = clusterStats(cluster)
  return (
    <div className="page-stack">
      <div className="stat-grid">
        <StatCard index={0} label="Registered brokers" value={stats.brokerCount} icon={Server} sub="From Metadata V0" />
        <StatCard index={1} label="Topics" value={stats.topicCount} icon={Database} sub="Including internal topics" />
        <StatCard index={2} label="Total partitions" value={stats.partitionCount} icon={Layers} sub="Across all topics" />
        <StatCard index={3} label="Leader assignments" value={`${stats.leaderCount}/${stats.partitionCount}`} icon={GitBranch} sub={stats.missingLeaderCount ? `${stats.missingLeaderCount} unknown` : 'All reported in metadata'} />
      </div>
      <div className="split-grid">
        <Suspense fallback={<div className="lazy-visual-loading">Loading topology...</div>}><Topology cluster={cluster} /></Suspense>
        <div className="insight-stack">
          <section className="panel insight-panel">
            <div className="eyebrow"><Zap size={13} /> UNDER THE HOOD</div>
            <div className="insight-visual"><span>PRODUCER</span><ArrowRight size={19} /><span>LEADER</span><ArrowRight size={19} /><span>QUORUM</span></div>
            <h2>Designed around durable agreement.</h2>
            <p>One leader accepts writes. Replicas acknowledge persisted history. The leader responds to a quorum write only after the commit path is complete.</p>
            <button className="link-button" onClick={() => navigate('architecture')}>Explore the write path <ArrowRight size={15} /></button>
          </section>
          <section className="panel cluster-identity-panel">
            <div className="eyebrow">CLUSTER IDENTITY</div>
            <div className="identity-row"><span>Cluster ID</span><strong title={cluster.id}>{compactId(cluster.id)}</strong></div>
            <div className="identity-row"><span>Metadata epoch</span><strong>{cluster.metadataEpoch.toLocaleString()}</strong></div>
            <div className="identity-row"><span>Protocol</span><strong>Metadata V0</strong></div>
          </section>
        </div>
      </div>
      <div className="split-grid lower-grid">
        <BenchmarkSummary navigate={navigate} />
        <section className="panel inventory-panel">
          <header className="panel-head">
            <div><div className="eyebrow"><Layers size={13} /> DATA INVENTORY</div><h2>Topic catalogue</h2><p>Assigned partition metadata.</p></div>
            <button className="icon-button" title="View all topics" aria-label="View all topics" onClick={() => navigate('topics')}><ArrowUpRight size={18} /></button>
          </header>
          <TopicTable cluster={cluster} compact />
        </section>
      </div>
      <ReadOnlyNote preview={preview} />
    </div>
  )
}

function PartitionInventory({ cluster, preview }: { cluster: Cluster; preview: boolean }) {
  const [topicFilter, setTopicFilter] = useState('all')
  const [query, setQuery] = useState('')
  const rows = cluster.topics.flatMap((topic) => topic.partitions.map((p) => ({ topic: topic.name, ...p })))
    .filter((item) => topicFilter === 'all' || item.topic === topicFilter)
    .filter((item) => `${item.topic}/${item.id}`.toLowerCase().includes(query.toLowerCase()))
  return (
    <div className="page-stack">
      <div className="section-intro"><span className="section-overline">PARTITION INDEX</span><h2>Every partition, one authority.</h2><p>Inspect which broker the metadata currently names as leader, and which brokers hold assigned replicas.</p></div>
      <section className="panel">
        <header className="panel-head"><div><div className="eyebrow"><Database size={13} /> TOPICS</div><h2>Topic inventory</h2><p>Filtered from the active metadata snapshot.</p></div></header>
        <TopicTable cluster={cluster} />
      </section>
      <section className="panel">
        <header className="panel-head"><div><div className="eyebrow"><Layers size={13} /> PARTITIONS</div><h2>Assignment details</h2><p>Leader and replica identities, not per-broker liveness.</p></div></header>
        <div className="table-toolbar">
          <div className="search-box"><Search size={16} /><input aria-label="Search partitions" value={query} placeholder="Search topic or partition" onChange={(event) => setQuery(event.target.value)} /></div>
          <select value={topicFilter} onChange={(event) => setTopicFilter(event.target.value)} aria-label="Filter partitions by topic">
            <option value="all">All topics</option>{cluster.topics.map((topic) => <option key={topic.name} value={topic.name}>{topic.name}</option>)}
          </select>
        </div>
        <div className="topic-table-wrap"><table className="topic-table">
          <thead><tr><th>Partition</th><th>Leader</th><th>Leader epoch</th><th>Assigned replicas</th><th>Metadata</th></tr></thead>
          <tbody>{rows.slice(0, 200).map((row) => <tr key={`${row.topic}:${row.id}`}>
            <td><span className="code-label">{row.topic} / {row.id}</span></td><td>{row.leaderId === null ? 'Unknown' : `Broker ${row.leaderId}`}</td>
            <td>{row.leaderEpoch ?? 'Not provided'}</td><td>{row.replicaIds.map((id) => `B${id}`).join(', ') || 'None reported'}</td>
            <td><Tag kind={row.leaderId === null ? 'warning' : 'positive'}>{row.leaderId === null ? 'UNKNOWN' : 'ASSIGNED'}</Tag></td>
          </tr>)}</tbody>
        </table>{rows.length > 200 && <div className="table-more">Displaying the first 200 matching partitions. Narrow your filter to inspect more.</div>}{rows.length === 0 && <div className="empty-table">No partitions match.</div>}</div>
      </section>
      <ReadOnlyNote preview={preview} />
    </div>
  )
}

function BenchmarkPage() {
  const [metric, setMetric] = useState<'rate' | 'p95'>('rate')
  return (
    <div className="page-stack">
      <div className="section-intro"><span className="section-overline">SOURCE-PINNED MEASUREMENTS</span><h2>Measured. Not imagined.</h2><p>These figures come from a recorded release benchmark and do not update when the cluster topology changes.</p></div>
      <div className="stat-grid">
        <StatCard label="RF3 buffered" value="33.31" sub="Records per second" icon={Zap} index={0} />
        <StatCard label="RF3 unbuffered" value="10.71" sub="Records per second" icon={Activity} index={1} />
        <StatCard label="Produce p95" value="138.83" sub="Milliseconds, RF3 buffered" icon={Clock3} index={2} />
        <StatCard label="Producer errors" value={benchmark.errors} sub="In the retained benchmark" icon={ShieldCheck} index={3} />
      </div>
      <section className="panel big-chart-panel">
        <header className="panel-head">
          <div><div className="eyebrow"><BarChart3 size={13} /> QUORUM RF3 COMPARISON</div><h2>Before and after batching</h2><p>Median of two short repeats, 1 KiB records, four concurrent producers.</p></div>
          <div className="segmented"><button className={metric === 'rate' ? 'selected' : ''} onClick={() => setMetric('rate')}>Throughput</button><button className={metric === 'p95' ? 'selected' : ''} onClick={() => setMetric('p95')}>p95 latency</button></div>
        </header>
        <Suspense fallback={<div className="lazy-visual-loading">Loading chart...</div>}><BenchmarkLargeChart metric={metric} /></Suspense>
        <div className="chart-notes"><Tag>RELEASE BUILD</Tag><Tag>LOCAL LOOPBACK</Tag><Tag>2 REPEATS</Tag><Tag>NO SLA CLAIM</Tag></div>
      </section>
      <div className="evidence-grid">
        <section className="panel evidence-panel"><div className="eyebrow"><FileText size={13} /> MEASUREMENT EVIDENCE</div><h2>Trace every number.</h2><p>12 measurement rows, 1,232 acknowledged records, matching fetched values and 2,464 latency samples. No producer errors were observed in this retained run.</p><a href={`${docs}/benchmarks/remediation_20261103/SUMMARY.md`} target="_blank" rel="noreferrer" className="link-button">Read the source report <ExternalLink size={15} /></a></section>
        <section className="panel evidence-panel"><div className="eyebrow"><CircleAlert size={13} /> SCOPE AND LIMITATIONS</div><h2>No performance theater.</h2><p>RF3 brokers and controllers shared one Windows laptop. The runs were short. Several repeat samples are too small to support meaningful production p99 conclusions. This is not a distributed hardware benchmark.</p><a href={`${docs}/INTERVIEW_READY.md`} target="_blank" rel="noreferrer" className="link-button">Read the demo contract <ExternalLink size={15} /></a><div style={{ marginTop: 16 }}><a href="http://127.0.0.1:3000" target="_blank" rel="noreferrer" className="link-button">Open local Grafana for live metrics <ExternalLink size={15} /></a></div></section>
      </div>
    </div>
  )
}

const architectureSteps = [
  { number: '01', name: 'Client', caption: 'Choose topic + partition', icon: Terminal, detail: 'The Rust client discovers broker addresses and finds the current partition leader.' },
  { number: '02', name: 'Leader', caption: 'Validate + append', icon: Server, detail: 'A partition leader checks authority, stages a WAL append, and starts replication.' },
  { number: '03', name: 'Followers', caption: 'Persist replicated entry', icon: HardDrive, detail: 'Peer replicas persist entries. Raft retains commit ordering and quorum rules.' },
  { number: '04', name: 'Commit', caption: 'Advance read visibility', icon: ShieldCheck, detail: 'Once committed, the leader can acknowledge quorum writes and expose committed fetches.' },
]

function ArchitecturePage() {
  return (
    <div className="page-stack">
      <div className="section-intro"><span className="section-overline">DESIGNED FOR CORRECTNESS</span><h2>Two planes. One consistent story.</h2><p>A controller quorum owns cluster metadata. Independent partition Raft groups order and durably replicate event data.</p></div>
      <section className="panel architecture-panel">
        <div className="eyebrow"><Workflow size={13} /> CONTROL PLANE AND DATA PLANE</div>
        <div className="planes-diagram">
          <div className="plane-row"><div className="plane-label"><span>01</span><strong>Control plane</strong><small>Metadata authority</small></div><div className="plane-content"><div className="plane-node controller-node"><Network size={19} /> Controller 1</div><div className="plane-node controller-node"><Network size={19} /> Controller 2</div><div className="plane-node controller-node"><Network size={19} /> Controller 3</div></div></div>
          <div className="plane-divider"><span>Committed assignment metadata</span><ArrowDownRight size={18} /></div>
          <div className="plane-row"><div className="plane-label"><span>02</span><strong>Data plane</strong><small>Partition-local Raft</small></div><div className="plane-content"><div className="plane-node broker-node"><Server size={19} /> Broker 1</div><div className="plane-node broker-node"><Server size={19} /> Broker 2</div><div className="plane-node broker-node"><Server size={19} /> Broker 3</div></div></div>
          <div className="plane-note">Conceptual architecture diagram. It does not assert that three controllers or three brokers are running.</div>
        </div>
      </section>
      <section className="panel architecture-panel">
        <header className="panel-head"><div><div className="eyebrow"><GitBranch size={13} /> WRITE PATH</div><h2>What happens when you produce an event?</h2><p>Quorum acknowledgment semantics, reduced to four steps.</p></div></header>
        <div className="write-steps">
          {architectureSteps.map((step, index) => {
            const Icon = step.icon
            return <div className="write-step" key={step.number}><div className="write-step-icon"><Icon size={22} /></div><span className="step-number">{step.number}</span><strong>{step.name}</strong><small>{step.caption}</small><p>{step.detail}</p>{index < architectureSteps.length - 1 && <ArrowRight className="step-arrow" size={20} />}</div>
          })}
        </div>
      </section>
      <div className="evidence-grid">
        <section className="panel evidence-panel"><div className="eyebrow"><LockKeyhole size={13} /> SAFETY CONTRACT</div><h2>What AstraLog actually guarantees.</h2><p>Quorum-committed record visibility, partition-local ordering, committed identity for duplicate retries, and explicit authority fencing. Cross-partition transactions and end-to-end exactly-once processing are outside this milestone.</p><a className="link-button" href={`${docs}/INVARIANTS.md`} target="_blank" rel="noreferrer">Read invariants <ExternalLink size={15} /></a></section>
        <section className="panel evidence-panel"><div className="eyebrow"><BookOpen size={13} /> ENGINEERING DEEP DIVE</div><h2>Explore all 10 crates.</h2><p>The master guide traces every module, wire protocol, failure case, design tradeoff and 60 adversarial interview questions.</p><a className="link-button" href={`${docs}/ASTRALOG_MASTER_GUIDE.md`} target="_blank" rel="noreferrer">Open the master guide <ExternalLink size={15} /></a></section>
      </div>
    </div>
  )
}

function EmptyLive({ retry, preview, error }: { retry: () => void; preview: () => void; error: string | null }) {
  return <div className="empty-live panel"><div className="empty-live-icon"><WifiOff size={34} /></div><div className="eyebrow">NO LIVE CLUSTER SNAPSHOT</div><h2>Connect the adapter to a running broker.</h2><p>Start the local Rust adapter and point its bootstrap address to a real AstraLog broker. Until that broker responds, live topology and topic counts remain unavailable.</p><div className="terminal-snippet">cargo run -p astralog-dashboard -- --bootstrap 127.0.0.1:7000</div>{error && <div className="connection-error">{error}</div>}<div className="empty-actions"><button className="primary-button" onClick={retry}><RefreshCw size={16} /> Retry connection</button><button className="secondary-button" onClick={preview}>Explore labeled preview <ArrowRight size={15} /></button></div></div>
}

function App() {
  const [view, setView] = useState<View>('overview')
  const [mode, setMode] = useState<Mode>('preview')
  const [mobileMenu, setMobileMenu] = useState(false)
  const [adapter, setAdapter] = useState<AdapterState | null>(null)
  const [liveCluster, setLiveCluster] = useState<Cluster | null>(null)
  const [requestError, setRequestError] = useState<string | null>(null)
  const [isFetching, setIsFetching] = useState(false)
  const [now, setNow] = useState(Date.now())
  const manualSelection = useRef(false)

  const load = useCallback(async () => {
    const controller = new AbortController()
    const timer = window.setTimeout(() => controller.abort(), 4000)
    setIsFetching(true)
    try {
      const response = await fetch('/api/overview', { signal: controller.signal, cache: 'no-store' })
      if (!response.ok) throw new Error(`Adapter returned HTTP ${response.status}`)
      const data: ApiSnapshot = await response.json()
      if (!data.adapter || !('state' in data.adapter)) throw new Error('Unexpected adapter response')
      setAdapter(data.adapter)
      setLiveCluster(normalizeCluster(data.cluster))
      setRequestError(null)
      if (data.adapter.state === 'connected' && !manualSelection.current) setMode('live')
    } catch (err) {
      setAdapter(null)
      setRequestError(err instanceof Error ? err.message : 'Unable to contact adapter')
    } finally {
      window.clearTimeout(timer)
      setIsFetching(false)
      setNow(Date.now())
    }
  }, [])

  useEffect(() => {
    void load()
    const interval = window.setInterval(() => { void load() }, 5000)
    const clock = window.setInterval(() => setNow(Date.now()), 3000)
    return () => { window.clearInterval(interval); window.clearInterval(clock) }
  }, [load])

  const pickMode = (next: Mode) => { manualSelection.current = true; setMode(next) }
  const navigate = (next: View) => { setView(next); setMobileMenu(false); window.scrollTo({ top: 0, behavior: 'smooth' }) }
  const selected = pageItems.find((item) => item.id === view) ?? pageItems[0]
  const cluster = mode === 'preview' ? previewCluster : liveCluster
  const adapterState = adapter?.state ?? (isFetching ? 'loading' : 'disconnected')
  const stateLabel = mode === 'preview' ? 'Showing illustrative data' : adapterState === 'connected' ? 'Connected to a real broker' : adapterState === 'unavailable' ? 'Last known metadata, currently unavailable' : 'Adapter or broker unavailable'
  const observedTime = relativeTime(adapter?.last_success_at_ms, now)
  const message = requestError ?? adapter?.error ?? null
  const titleMap: Record<View, string> = { overview: 'Cluster overview', topology: 'Cluster topology', topics: 'Topics & partitions', benchmarks: 'Benchmark explorer', architecture: 'Architecture' }
  const subtitleMap: Record<View, string> = { overview: 'Observe the shape of your distributed log.', topology: 'Understand replica placement and partition authority.', topics: 'Browse the current metadata inventory.', benchmarks: 'Source-linked, reproducible benchmark evidence.', architecture: 'Explore the systems design behind AstraLog.' }

  const clusterKey = useMemo(() => cluster ? `${cluster.id}:${cluster.metadataEpoch}:${cluster.topics.length}` : 'empty', [cluster])

  return <div className="app-shell">
    <aside className={`sidebar ${mobileMenu ? 'sidebar-open' : ''}`}>
      <div className="sidebar-top">
        <button className="brand" onClick={() => navigate('overview')} aria-label="AstraLog overview"><span className="brand-symbol"><Box size={19} strokeWidth={1.8} /></span><span className="brand-text">Astra<span>Log</span><small>CONTROL CENTER</small></span></button>
        <button className="mobile-menu-close" onClick={() => setMobileMenu(false)} aria-label="Close navigation"><X size={19} /></button>
        <div className="workspace-context" aria-label="Local development workspace, read-only observer">
          <span className="workspace-context-mark" aria-hidden="true" />
          <span><strong>Local workspace</strong><small>Development environment</small></span>
          <span className="workspace-context-label">LOCAL</span>
        </div>
      </div>
      <nav className="sidebar-nav" aria-label="Primary navigation">
        <div className="nav-heading">EXPLORE</div>
        {pageItems.map((item) => { const Icon = item.icon; return <button key={item.id} className={`nav-item ${view === item.id ? 'active' : ''}`} aria-current={view === item.id ? 'page' : undefined} aria-label={item.label} onClick={() => navigate(item.id)} title={item.description}><Icon size={17} strokeWidth={1.85} /><span>{item.label}</span></button> })}
        <div className="nav-heading nav-heading-second">RESOURCES</div>
        <a className="nav-item" href={`${docs}/ASTRALOG_MASTER_GUIDE.md`} target="_blank" rel="noreferrer" aria-label="Engineering guide, opens in a new tab"><BookOpen size={17} strokeWidth={1.85} /><span>Engineering guide</span><ExternalLink size={13} className="nav-external" /></a>
        <a className="nav-item" href="http://127.0.0.1:3000" target="_blank" rel="noreferrer" aria-label="Live Grafana metrics, opens in a new tab" title="Prometheus-backed metrics dashboard requires the local monitoring stack"><Activity size={17} strokeWidth={1.85} /><span>Live Grafana metrics</span><ExternalLink size={13} className="nav-external" /></a>
        <a className="nav-item" href={GITHUB} target="_blank" rel="noreferrer" aria-label="GitHub repository, opens in a new tab"><GitBranch size={17} strokeWidth={1.85} /><span>GitHub repository</span><ExternalLink size={13} className="nav-external" /></a>
      </nav>
      <div className="sidebar-bottom">
        <div className="sidebar-status" title="The Control Center observes metadata and never issues broker mutations.">
          <ShieldCheck size={17} strokeWidth={1.8} />
          <span><strong>Read-only observer</strong><small>No broker mutations</small></span>
          <span className="sidebar-status-dot" aria-hidden="true" />
        </div>
        <div className="sidebar-version"><span>ENGINEERING PREVIEW</span><span>v0.1</span></div>
      </div>
    </aside>
    {mobileMenu && <button className="mobile-backdrop" aria-label="Close navigation" onClick={() => setMobileMenu(false)} />}
    <div className="main-shell">
      <div className="topbar">
        <div className="topbar-left"><button className="mobile-menu-button" aria-label="Open navigation" onClick={() => setMobileMenu(true)}><Menu size={21} /></button><span className="breadcrumbs">Workspace</span><ChevronRight size={14} className="muted" /><strong>{selected.label}</strong></div>
        <div className="topbar-right"><span className="topbar-clock"><Clock3 size={14} /> {new Date(now).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}</span><a className="github-button" href={GITHUB} rel="noreferrer" target="_blank"><GitBranch size={16} /><span>Source</span><ArrowUpRight size={14} /></a></div>
      </div>
      <main className="page-content">
        <div className="page-heading-row">
          <div className="page-heading"><div className="eyebrow top-eyebrow"><span className="accent-square" /> DISTRIBUTED EVENT STREAMING</div><h1>{titleMap[view]}<span className="heading-period">.</span></h1><p>{subtitleMap[view]}</p></div>
          <div className="heading-actions"><div className="mode-switch" role="group" aria-label="Data source"><button className={mode === 'preview' ? 'selected' : ''} onClick={() => pickMode('preview')}><Box size={15} /> Preview</button><button className={mode === 'live' ? 'selected' : ''} onClick={() => pickMode('live')}><Wifi size={15} /> Live</button></div><button className="refresh-button" aria-label="Refresh metadata" title="Refresh live adapter" onClick={() => void load()} disabled={isFetching}><RefreshCw size={17} className={isFetching ? 'rotating' : ''} /></button></div>
        </div>
        <div className={`connection-strip ${mode === 'preview' ? 'connection-preview' : ''} ${mode === 'live' && adapterState !== 'connected' ? 'connection-warning' : ''}`}>
          <div className="connection-main"><StatusIndicator mode={mode} state={adapterState} /><span>{stateLabel}</span>{mode === 'live' && <span className="connection-separator">·</span>}{mode === 'live' && <span className="connection-age">Last success: {observedTime}</span>}</div>
          <span className="connection-end">{mode === 'preview' ? 'Sample data. Never live.' : `Source: ${adapter?.bootstrap ?? 'localhost adapter'}`}</span>
        </div>
        {mode === 'live' && message && <div className="error-banner"><CircleAlert size={16} /><span>{message}</span></div>}
        {view === 'benchmarks' ? <BenchmarkPage /> : view === 'architecture' ? <ArchitecturePage /> : cluster ? (
          view === 'overview' ? <Overview key={clusterKey} cluster={cluster} preview={mode === 'preview'} navigate={navigate} />
            : view === 'topology' ? <div className="page-stack"><div className="section-intro"><span className="section-overline">VISUALIZE THE REPLICATION MAP</span><h2>The cluster, made visible.</h2><p>Select a topic to see which brokers the current metadata assigns to its partition replicas.</p></div><Suspense fallback={<div className="lazy-visual-loading">Loading topology...</div>}><Topology key={clusterKey} cluster={cluster} expanded /></Suspense><ReadOnlyNote preview={mode === 'preview'} /></div>
              : <PartitionInventory key={clusterKey} cluster={cluster} preview={mode === 'preview'} />
        ) : <EmptyLive retry={() => void load()} preview={() => pickMode('preview')} error={message} />}
        <footer className="main-footer"><span><span className="footer-orbit">◈</span> ASTRA<span>LOG</span> / SYSTEMS ENGINEERING PORTFOLIO</span><span>Prototype UI · No authentication · Local use only</span></footer>
      </main>
    </div>
  </div>
}

export default App
