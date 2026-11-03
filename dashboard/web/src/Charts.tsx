import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { benchmark } from './data'

export function BenchmarkMiniChart() {
  return <div className="mini-chart"><ResponsiveContainer width="100%" height={184}>
    <BarChart data={benchmark.throughput} margin={{ top: 12, right: 12, left: -25, bottom: 0 }} barSize={47}>
      <CartesianGrid vertical={false} stroke="#1d2b40" strokeDasharray="4 5" />
      <XAxis dataKey="mode" tick={{ fill: '#8998ae', fontSize: 11 }} tickLine={false} axisLine={false} />
      <YAxis tick={{ fill: '#75869e', fontSize: 11 }} tickLine={false} axisLine={false} domain={[0, 40]} />
      <Tooltip contentStyle={{ background: '#121c2b', border: '1px solid #35435b', borderRadius: 10, color: '#e9f0fb' }}
        formatter={(value) => [`${value} records/s`, 'RF3 quorum']} cursor={{ fill: '#ffffff09' }} />
      <Bar dataKey="rate" radius={[6, 6, 0, 0]}><Cell fill="#3d577f" /><Cell fill="#6d9bff" /></Bar>
    </BarChart>
  </ResponsiveContainer></div>
}

export function BenchmarkLargeChart({ metric }: { metric: 'rate' | 'p95' }) {
  const rows = benchmark.throughput.map((entry) => ({ ...entry, value: metric === 'rate' ? entry.rate : entry.p95 }))
  return <div className="big-chart"><ResponsiveContainer width="100%" height="100%">
    <BarChart data={rows} margin={{ top: 25, right: 35, bottom: 15, left: 5 }} barSize={86}>
      <CartesianGrid strokeDasharray="4 5" vertical={false} stroke="#243247" />
      <XAxis dataKey="mode" tick={{ fill: '#95a5bd', fontSize: 12 }} tickLine={false} axisLine={false} />
      <YAxis tick={{ fill: '#95a5bd', fontSize: 12 }} tickLine={false} axisLine={false} unit={metric === 'rate' ? '' : 'ms'} />
      <Tooltip cursor={{ fill: '#ffffff08' }} contentStyle={{ background: '#131d2c', border: '1px solid #35435b', borderRadius: 12, color: '#e7eef9' }} formatter={(value) => [`${value} ${metric === 'rate' ? 'records/s' : 'ms'}`, metric === 'rate' ? 'Throughput' : 'Produce p95']} />
      <Bar dataKey="value" radius={[9, 9, 0, 0]}><Cell fill="#526684" /><Cell fill="#7d9cff" /></Bar>
    </BarChart>
  </ResponsiveContainer></div>
}
