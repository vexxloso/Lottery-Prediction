import { useEffect, useState, useCallback } from 'react';
import { Pagination } from 'antd';
import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ZAxis,
} from 'recharts';

const API_URL = (import.meta.env.VITE_API_URL ?? 'http://localhost:8000').replace(/\/$/, '');

type LotterySlug = 'euromillones' | 'el-gordo' | 'la-primitiva';

const LOTTERIES: { slug: LotterySlug; label: string; color: string; img: string }[] = [
  { slug: 'euromillones', label: 'Euromillones', color: '#1976D2', img: '/images/euromillones.png' },
  { slug: 'el-gordo',     label: 'El Gordo',     color: '#c41230', img: '/images/el-gordo.png'     },
  { slug: 'la-primitiva', label: 'La Primitiva', color: '#00843d', img: '/images/la-primitiva.png' },
];

// ── Types ─────────────────────────────────────────────────────────────────────

interface AccuracyRow {
  draw_id: string;
  pre_id: string;
  draw_date: string;
  jackpot_position: number;
  total_tickets: number;
  error_rate: number;
  error_rate_pct: number;
  rolling_mean_error_rate: number;
  model_source: string;
}

interface AccuracyData {
  lottery: string;
  total_draws: number;
  total_in_db?: number;
  limit?: number;
  total_tickets: number;
  avg_error_rate: number;
  avg_error_rate_pct: number;
  best_draw: { draw_id: string; jackpot_position: number; draw_date: string } | null;
  worst_draw: { draw_id: string; jackpot_position: number; draw_date: string } | null;
  rows: AccuracyRow[];
}

interface MeanErrorRow {
  draw_id: string;
  draw_date: string;
  jackpot_position: number;
  error_distance: number;
  cumulative_mean_error: number;
}

interface MeanErrorData {
  lottery: string;
  total_draws: number;
  total_in_db?: number;
  limit?: number;
  total_tickets: number;
  overall_mean_error: number;
  rows: MeanErrorRow[];
}

interface CrossValRow {
  draw_id: string;
  pre_id: string;
  draw_date: string;
  jackpot_position: number;
  percentile: number;
  in_top_1pct: boolean;
  in_top_5pct: boolean;
  in_top_10pct: boolean;
  in_top_25pct: boolean;
}

interface CrossValData {
  lottery: string;
  total_draws: number;
  total_tickets: number;
  summary: {
    in_top_1pct: number; in_top_5pct: number; in_top_10pct: number; in_top_25pct: number;
    pct_in_top_1pct: number; pct_in_top_5pct: number; pct_in_top_10pct: number; pct_in_top_25pct: number;
  };
  rows: CrossValRow[];
}

interface OrcRow {
  lottery: string;
  draw_id: string;
  orc_hash: string;
  txt_hash: string | null;
  orc_path: string;
  txt_path: string | null;
  created_at: string;
}

interface OrcData {
  lottery: string;
  total: number;
  rows: OrcRow[];
}

interface FeedbackRow {
  draw_id: string;
  pre_draw_id: string;
  draw_date?: string;
  error_rate: number;
  updated_at: string;
  actual_jackpot_position: number;
  new_orc_hash: string;
  feedback_records: { model: string; added_estimators?: number; gradient_steps?: number; total_estimators?: number }[];
}

interface FeedbackData {
  lottery: string;
  total: number;
  rows: FeedbackRow[];
  diagnostics?: {
    compare_count: number;
    orc_count: number;
    feedback_count: number;
    pending_feedback: number;
  };
}

interface HashValidation {
  valid: boolean;
  orc_match: boolean;
  txt_match: boolean;
  details: string;
  draw_id: string;
  lottery: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmt(n: number): string {
  return n.toLocaleString('es-ES');
}

function pct(n: number, total: number): string {
  if (!total) return '0%';
  return (n / total * 100).toFixed(2) + '%';
}

function StatCard({ label, value, sub, color }: { label: string; value: string | number; sub?: string; color?: string }) {
  return (
    <div style={{
      background: '#fff', border: '1px solid #e0e0e0', borderRadius: 8,
      padding: '14px 18px', minWidth: 160,
    }}>
      <div style={{ fontSize: '0.7rem', color: '#888', textTransform: 'uppercase', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: '1.6rem', fontWeight: 700, color: color ?? '#222' }}>{value}</div>
      {sub && <div style={{ fontSize: '0.72rem', color: '#888', marginTop: 2 }}>{sub}</div>}
    </div>
  );
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <h3 style={{ margin: '28px 0 12px', fontSize: '1.05rem', borderBottom: '2px solid #f0f0f0', paddingBottom: 6 }}>
      {children}
    </h3>
  );
}

function Badge({ ok, label }: { ok: boolean; label: string }) {
  return (
    <span style={{
      fontSize: '0.72rem', padding: '2px 8px', borderRadius: 999, fontWeight: 600,
      background: ok ? '#dcfce7' : '#fee2e2',
      color: ok ? '#16a34a' : '#dc2626',
    }}>{label}</span>
  );
}

function TablePagination({
  page,
  pageSize,
  total,
  onChange,
}: {
  page: number;
  pageSize: number;
  total: number;
  onChange: (page: number, pageSize: number) => void;
}) {
  if (total <= 0) return null;
  return (
    <div style={{ marginTop: 12, display: 'flex', justifyContent: 'flex-end' }}>
      <Pagination
        current={page}
        pageSize={pageSize}
        total={total}
        showSizeChanger
        pageSizeOptions={['20', '50', '100', '200']}
        onChange={onChange}
        showTotal={(t, range) => `${range[0]}-${range[1]} of ${fmt(t)} draws`}
      />
    </div>
  );
}

// ── Mini bar chart (pure CSS) ─────────────────────────────────────────────────

function MiniBarChart({
  rows, valueKey, labelKey, color, height = 120, maxRows = 60,
}: {
  rows: Record<string, number | string>[];
  valueKey: string;
  labelKey: string;
  color: string;
  height?: number;
  maxRows?: number;
}) {
  const data = rows.slice(-maxRows);
  if (!data.length) return <div style={{ color: '#aaa', fontSize: '0.8rem' }}>No data</div>;
  const values = data.map(r => Number(r[valueKey]) || 0);
  const maxVal = Math.max(...values, 1);

  return (
    <div style={{ display: 'flex', alignItems: 'flex-end', gap: 1, height, overflowX: 'auto' }}>
      {data.map((row, i) => {
        const v = Number(row[valueKey]) || 0;
        const barH = Math.max(2, Math.round((v / maxVal) * (height - 20)));
        return (
          <div
            key={i}
            title={`${row[labelKey]}: ${v.toLocaleString()}`}
            style={{
              flex: '0 0 auto',
              width: Math.max(3, Math.floor(600 / data.length)),
              height: barH,
              background: color,
              borderRadius: '2px 2px 0 0',
              opacity: 0.8,
              cursor: 'default',
            }}
          />
        );
      })}
    </div>
  );
}

// ── Line chart (pure SVG) ─────────────────────────────────────────────────────

function LineChart({
  rows, valueKey, labelKey, color, width = 700, height = 140, maxRows = 80,
}: {
  rows: Record<string, number | string>[];
  valueKey: string;
  labelKey: string;
  color: string;
  width?: number;
  height?: number;
  maxRows?: number;
}) {
  const data = rows.slice(-maxRows);
  if (data.length < 2) return <div style={{ color: '#aaa', fontSize: '0.8rem' }}>Not enough data</div>;

  const values = data.map(r => Number(r[valueKey]) || 0);
  const minVal = Math.min(...values);
  const maxVal = Math.max(...values, minVal + 1);
  const pad = { top: 10, right: 10, bottom: 20, left: 50 };
  const W = width - pad.left - pad.right;
  const H = height - pad.top - pad.bottom;

  const pts = data.map((_, i) => {
    const x = pad.left + (i / (data.length - 1)) * W;
    const y = pad.top + H - ((values[i] - minVal) / (maxVal - minVal)) * H;
    return `${x},${y}`;
  });

  const polyline = pts.join(' ');

  // Y-axis labels
  const yLabels = [minVal, (minVal + maxVal) / 2, maxVal].map(v =>
    v >= 1_000_000 ? (v / 1_000_000).toFixed(1) + 'M'
    : v >= 1_000 ? (v / 1_000).toFixed(0) + 'K'
    : v.toFixed(2)
  );

  return (
    <svg width="100%" viewBox={`0 0 ${width} ${height}`} style={{ overflow: 'visible' }}>
      {/* Y-axis labels */}
      {[0, 1, 2].map((i) => {
        const y = pad.top + H - (i / 2) * H;
        return (
          <g key={i}>
            <line x1={pad.left} y1={y} x2={pad.left + W} y2={y} stroke="#f0f0f0" strokeWidth={1} />
            <text x={pad.left - 4} y={y + 4} textAnchor="end" fontSize={9} fill="#aaa">{yLabels[i]}</text>
          </g>
        );
      })}
      {/* Line */}
      <polyline points={polyline} fill="none" stroke={color} strokeWidth={1.5} />
      {/* Dots for small datasets */}
      {data.length <= 30 && pts.map((pt, i) => {
        const [x, y] = pt.split(',').map(Number);
        return (
          <circle key={i} cx={x} cy={y} r={3} fill={color}>
            <title>{`${data[i][labelKey]}: ${values[i]}`}</title>
          </circle>
        );
      })}
      {/* X-axis labels (first, middle, last) */}
      {[0, Math.floor(data.length / 2), data.length - 1].map(i => {
        const x = pad.left + (i / (data.length - 1)) * W;
        return (
          <text key={i} x={x} y={height - 2} textAnchor="middle" fontSize={9} fill="#aaa">
            {String(data[i][labelKey]).slice(0, 10)}
          </text>
        );
      })}
    </svg>
  );
}

// ── Accuracy Chart Section ────────────────────────────────────────────────────

function AccuracySection({ lottery, color }: { lottery: LotterySlug; color: string }) {
  const [data, setData] = useState<AccuracyData | null>(null);
  const [loading, setLoading] = useState(false);
  const [limit, setLimit] = useState(100);
  const [tableRows, setTableRows] = useState<AccuracyRow[]>([]);
  const [tableTotal, setTableTotal] = useState(0);
  const [tableLoading, setTableLoading] = useState(false);
  const [tablePage, setTablePage] = useState(1);
  const [tablePageSize, setTablePageSize] = useState(50);

  useEffect(() => {
    setLoading(true);
    setData(null);
    fetch(`${API_URL}/api/validation/accuracy-chart?lottery=${lottery}&limit=${limit}`)
      .then(r => r.json())
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [lottery, limit]);

  useEffect(() => {
    setTableLoading(true);
    const skip = (tablePage - 1) * tablePageSize;
    fetch(`${API_URL}/api/validation/accuracy-rows?lottery=${lottery}&skip=${skip}&limit=${tablePageSize}`)
      .then(r => r.json())
      .then(d => {
        setTableRows(d.rows ?? []);
        setTableTotal(d.total ?? 0);
      })
      .catch(() => {
        setTableRows([]);
        setTableTotal(0);
      })
      .finally(() => setTableLoading(false));
  }, [lottery, tablePage, tablePageSize]);

  useEffect(() => {
    setTablePage(1);
  }, [lottery]);

  const handleTablePageChange = (page: number, pageSize: number) => {
    setTablePage(page);
    setTablePageSize(pageSize);
  };

  if (loading && !data) return <div style={{ color: '#888', padding: 16 }}>Loading accuracy data...</div>;
  if (!data) return <div style={{ color: '#aaa', padding: 16 }}>No accuracy data available.</div>;

  const totalInDb = data.total_in_db ?? tableTotal;

  return (
    <div>
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
        <StatCard label="Draws in chart" value={fmt(data.total_draws)} color={color}
          sub={totalInDb > data.total_draws ? `last ${data.limit ?? limit} of ${fmt(totalInDb)} in DB` : undefined} />
        <StatCard label="Avg error rate" value={data.avg_error_rate_pct.toFixed(4) + '%'}
          sub="lower = model ranked jackpot higher" color="#f59e0b" />
        <StatCard label="Best draw" value={data.best_draw ? fmt(data.best_draw.jackpot_position) : '—'}
          sub={data.best_draw?.draw_date ?? ''} color="#22c55e" />
        <StatCard label="Worst draw" value={data.worst_draw ? fmt(data.worst_draw.jackpot_position) : '—'}
          sub={data.worst_draw?.draw_date ?? ''} color="#ef4444" />
      </div>

      <div style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <span style={{ fontSize: '0.8rem', color: '#888' }}>
          Most recent draws — jackpot position (lower = better)
        </span>
        <select value={limit} onChange={e => setLimit(Number(e.target.value))}
          style={{ fontSize: '0.8rem', padding: '2px 6px', borderRadius: 4, border: '1px solid #ddd' }}>
          {[50, 100, 200, 500].map(v => <option key={v} value={v}>Chart: last {v}</option>)}
        </select>
      </div>

      <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: '12px 16px', marginBottom: 12 }}>
        <div style={{ fontSize: '0.72rem', color: '#888', marginBottom: 6 }}>Jackpot position (bar = per draw)</div>
        <MiniBarChart rows={data.rows as unknown as Record<string, number | string>[]}
          valueKey="jackpot_position" labelKey="draw_date" color={color} height={100} maxRows={limit} />
      </div>

      <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: '12px 16px', marginBottom: 12 }}>
        <div style={{ fontSize: '0.72rem', color: '#888', marginBottom: 6 }}>Error rate % over time (line = rolling mean)</div>
        <LineChart rows={data.rows as unknown as Record<string, number | string>[]}
          valueKey="rolling_mean_error_rate" labelKey="draw_date" color={color} height={130} maxRows={limit} />
      </div>

      {/* Table: all draws via pagination */}
      <div style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 12 }}>
        <span style={{ fontSize: '0.8rem', color: '#888' }}>
          All draws — {fmt(tableTotal)} total
        </span>
        {tableLoading && <span style={{ fontSize: '0.78rem', color: '#aaa' }}>Loading table…</span>}
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
          <thead>
            <tr style={{ background: '#f5f5f5' }}>
              {['Draw date', 'Draw ID', 'Jackpot position', 'Error rate %'].map(h => (
                <th key={h} style={{ padding: '6px 10px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e0e0e0' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {tableRows.length === 0 && !tableLoading ? (
              <tr>
                <td colSpan={4} style={{ padding: 16, textAlign: 'center', color: '#aaa' }}>No draws found</td>
              </tr>
            ) : tableRows.map((row, i) => (
              <tr key={row.draw_id} style={{ background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}>{row.draw_date}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', color: '#888' }}>{row.draw_id}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', fontWeight: 600 }}>{fmt(row.jackpot_position)}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', color: row.error_rate_pct < 10 ? '#22c55e' : '#f59e0b' }}>
                  {row.error_rate_pct.toFixed(4)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <TablePagination
        page={tablePage}
        pageSize={tablePageSize}
        total={tableTotal}
        onChange={handleTablePageChange}
      />
    </div>
  );
}

// ── Mean Error Section ────────────────────────────────────────────────────────

function MeanErrorSection({ lottery, color }: { lottery: LotterySlug; color: string }) {
  const [data, setData] = useState<MeanErrorData | null>(null);
  const [loading, setLoading] = useState(false);
  const [limit, setLimit] = useState(100);
  const [tableRows, setTableRows] = useState<MeanErrorRow[]>([]);
  const [tableTotal, setTableTotal] = useState(0);
  const [tableLoading, setTableLoading] = useState(false);
  const [tablePage, setTablePage] = useState(1);
  const [tablePageSize, setTablePageSize] = useState(50);

  useEffect(() => {
    setLoading(true);
    setData(null);
    fetch(`${API_URL}/api/validation/mean-error-chart?lottery=${lottery}&limit=${limit}`)
      .then(r => r.json())
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [lottery, limit]);

  useEffect(() => {
    setTableLoading(true);
    const skip = (tablePage - 1) * tablePageSize;
    fetch(`${API_URL}/api/validation/mean-error-rows?lottery=${lottery}&skip=${skip}&limit=${tablePageSize}`)
      .then(r => r.json())
      .then(d => {
        setTableRows(d.rows ?? []);
        setTableTotal(d.total ?? 0);
      })
      .catch(() => {
        setTableRows([]);
        setTableTotal(0);
      })
      .finally(() => setTableLoading(false));
  }, [lottery, tablePage, tablePageSize]);

  useEffect(() => {
    setTablePage(1);
  }, [lottery]);

  const handleTablePageChange = (page: number, pageSize: number) => {
    setTablePage(page);
    setTablePageSize(pageSize);
  };

  if (loading && !data) return <div style={{ color: '#888', padding: 16 }}>Loading mean error data...</div>;
  if (!data) return <div style={{ color: '#aaa', padding: 16 }}>No mean error data available.</div>;

  const totalInDb = data.total_in_db ?? tableTotal;

  return (
    <div>
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
        <StatCard label="Draws in chart" value={fmt(data.total_draws)} color={color}
          sub={totalInDb > data.total_draws ? `last ${data.limit ?? limit} of ${fmt(totalInDb)} in DB` : undefined} />
        <StatCard label="Overall mean error" value={fmt(Math.round(data.overall_mean_error))}
          sub={`avg jackpot position across ${data.total_draws} draws`} color="#f59e0b" />
        <StatCard label="Total tickets" value={fmt(data.total_tickets)} color="#6366f1" />
      </div>

      <div style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <span style={{ fontSize: '0.8rem', color: '#888' }}>
          Most recent draws — cumulative mean error (should decrease as model learns)
        </span>
        <select value={limit} onChange={e => setLimit(Number(e.target.value))}
          style={{ fontSize: '0.8rem', padding: '2px 6px', borderRadius: 4, border: '1px solid #ddd' }}>
          {[50, 100, 200, 500].map(v => <option key={v} value={v}>Chart: last {v}</option>)}
        </select>
      </div>

      <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: '12px 16px', marginBottom: 12 }}>
        <div style={{ fontSize: '0.72rem', color: '#888', marginBottom: 6 }}>
          Cumulative mean error (jackpot position)
        </div>
        <LineChart rows={data.rows as unknown as Record<string, number | string>[]}
          valueKey="cumulative_mean_error" labelKey="draw_date" color={color} height={140} maxRows={limit} />
      </div>

      <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: '12px 16px', marginBottom: 12 }}>
        <div style={{ fontSize: '0.72rem', color: '#888', marginBottom: 6 }}>Per-draw jackpot position (distance from rank 1)</div>
        <MiniBarChart rows={data.rows as unknown as Record<string, number | string>[]}
          valueKey="error_distance" labelKey="draw_date" color={color} height={90} maxRows={limit} />
      </div>

      <div style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 12 }}>
        <span style={{ fontSize: '0.8rem', color: '#888' }}>
          All draws — {fmt(tableTotal)} total
        </span>
        {tableLoading && <span style={{ fontSize: '0.78rem', color: '#aaa' }}>Loading table…</span>}
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
          <thead>
            <tr style={{ background: '#f5f5f5' }}>
              {['Draw date', 'Draw ID', 'Jackpot position', 'Error distance'].map(h => (
                <th key={h} style={{ padding: '6px 10px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e0e0e0' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {tableRows.length === 0 && !tableLoading ? (
              <tr>
                <td colSpan={4} style={{ padding: 16, textAlign: 'center', color: '#aaa' }}>No draws found</td>
              </tr>
            ) : tableRows.map((row, i) => (
              <tr key={row.draw_id} style={{ background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}>{row.draw_date}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', color: '#888' }}>{row.draw_id}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', fontWeight: 600 }}>{fmt(row.jackpot_position)}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}>{fmt(row.error_distance)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <TablePagination
        page={tablePage}
        pageSize={tablePageSize}
        total={tableTotal}
        onChange={handleTablePageChange}
      />
    </div>
  );
}

// ── Top-N Tickets Section ─────────────────────────────────────────────────────

function TopTicketsSection({ lottery, color }: { lottery: LotterySlug; color: string }) {
  const [drawId, setDrawId] = useState('');
  const [preId, setPreId] = useState('');
  const [limit, setLimit] = useState(100);
  const [data, setData] = useState<null | {
    lottery: string; draw_id: string; pre_id: string; total_returned: number;
    has_actual_draw: boolean; hit_distribution: Record<string, number>;
    rows: { rank: number; mains: number[]; score: number; hits_main: number | null; hits_secondary: number | null; stars?: number[]; clave?: number; reintegro?: number }[];
  }>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const load = useCallback(() => {
    if (!drawId.trim() || !preId.trim()) { setError('Enter both Draw ID and Pre-draw ID'); return; }
    setError('');
    setLoading(true);
    setData(null);
    fetch(`${API_URL}/api/validation/top-tickets?lottery=${lottery}&draw_id=${drawId.trim()}&pre_id=${preId.trim()}&limit=${limit}`)
      .then(r => r.json())
      .then(d => { if (d.detail) setError(d.detail); else setData(d); })
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [lottery, drawId, preId, limit]);

  return (
    <div>
      <p style={{ fontSize: '0.85rem', color: '#666', margin: '0 0 12px' }}>
        Enter a draw ID and its pre-draw ID to see the top-N ranked tickets and how many numbers they hit.
        The client bets between 6,000–25,000 tickets per range, so this shows how the top tickets performed.
      </p>
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12 }}>
        <input value={drawId} onChange={e => setDrawId(e.target.value)} placeholder="Draw ID (current_id)"
          style={{ padding: '6px 10px', borderRadius: 4, border: '1px solid #ddd', fontSize: '0.85rem', width: 160 }} />
        <input value={preId} onChange={e => setPreId(e.target.value)} placeholder="Pre-draw ID"
          style={{ padding: '6px 10px', borderRadius: 4, border: '1px solid #ddd', fontSize: '0.85rem', width: 160 }} />
        <select value={limit} onChange={e => setLimit(Number(e.target.value))}
          style={{ padding: '6px 10px', borderRadius: 4, border: '1px solid #ddd', fontSize: '0.85rem' }}>
          {[100, 500, 1000, 5000, 25000].map(v => <option key={v} value={v}>Top {v.toLocaleString()}</option>)}
        </select>
        <button onClick={load} disabled={loading}
          style={{ padding: '6px 16px', borderRadius: 4, border: 'none', background: color, color: '#fff', fontWeight: 600, cursor: 'pointer', opacity: loading ? 0.6 : 1 }}>
          {loading ? 'Loading...' : 'Load'}
        </button>
      </div>
      {error && <div style={{ color: '#dc2626', fontSize: '0.82rem', marginBottom: 8 }}>{error}</div>}

      {data && (
        <div>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
            <StatCard label="Tickets returned" value={fmt(data.total_returned)} color={color} />
            <StatCard label="Has actual draw" value={data.has_actual_draw ? '✅ Yes' : '⚠️ No'} color={data.has_actual_draw ? '#22c55e' : '#f59e0b'} />
          </div>

          {/* Hit distribution */}
          {data.has_actual_draw && (
            <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: '12px 16px', marginBottom: 16 }}>
              <div style={{ fontSize: '0.75rem', color: '#888', marginBottom: 8, textTransform: 'uppercase' }}>
                Hit distribution (main numbers matched) in top {fmt(data.total_returned)} tickets
              </div>
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                {Object.entries(data.hit_distribution).sort((a, b) => Number(b[0]) - Number(a[0])).map(([hits, count]) => (
                  <div key={hits} style={{
                    background: Number(hits) >= 4 ? '#dcfce7' : Number(hits) >= 2 ? '#fef9c3' : '#f5f5f5',
                    border: '1px solid #e0e0e0', borderRadius: 6, padding: '8px 14px', textAlign: 'center',
                  }}>
                    <div style={{ fontSize: '1.2rem', fontWeight: 700, color: Number(hits) >= 4 ? '#16a34a' : '#555' }}>{count}</div>
                    <div style={{ fontSize: '0.7rem', color: '#888' }}>{hits} hits</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Table */}
          <div style={{ overflowX: 'auto', maxHeight: 400, overflowY: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
              <thead style={{ position: 'sticky', top: 0, background: '#f5f5f5', zIndex: 1 }}>
                <tr>
                  {['Rank', 'Main numbers', lottery === 'euromillones' ? 'Stars' : lottery === 'el-gordo' ? 'Clave' : 'Reintegro', 'Score', 'Hits (main)', 'Hits (sec)'].map(h => (
                    <th key={h} style={{ padding: '6px 10px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e0e0e0' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.rows.slice(0, 1000).map((row, i) => (
                  <tr key={row.rank} style={{ background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                    <td style={{ padding: '4px 10px', borderBottom: '1px solid #f0f0f0', color: '#888' }}>{row.rank}</td>
                    <td style={{ padding: '4px 10px', borderBottom: '1px solid #f0f0f0', fontFamily: 'monospace' }}>
                      {row.mains.join(', ')}
                    </td>
                    <td style={{ padding: '4px 10px', borderBottom: '1px solid #f0f0f0', fontFamily: 'monospace' }}>
                      {lottery === 'euromillones' ? (row.stars ?? []).join(', ')
                        : lottery === 'el-gordo' ? row.clave
                        : row.reintegro}
                    </td>
                    <td style={{ padding: '4px 10px', borderBottom: '1px solid #f0f0f0', color: '#888' }}>{row.score.toFixed(2)}</td>
                    <td style={{ padding: '4px 10px', borderBottom: '1px solid #f0f0f0', fontWeight: row.hits_main != null && row.hits_main >= 4 ? 700 : 400,
                      color: row.hits_main != null && row.hits_main >= 4 ? '#16a34a' : '#555' }}>
                      {row.hits_main ?? '—'}
                    </td>
                    <td style={{ padding: '4px 10px', borderBottom: '1px solid #f0f0f0' }}>{row.hits_secondary ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {data.rows.length > 1000 && (
              <div style={{ padding: '8px 10px', color: '#888', fontSize: '0.78rem' }}>
                Showing first 1,000 of {fmt(data.rows.length)} rows
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Hash Validation Section ───────────────────────────────────────────────────

function HashValidationSection({ lottery, color }: { lottery: LotterySlug; color: string }) {
  const [orcData, setOrcData] = useState<OrcData | null>(null);
  const [orcLoading, setOrcLoading] = useState(false);
  const [validateDrawId, setValidateDrawId] = useState('');
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] = useState<HashValidation | null>(null);
  const [validationError, setValidationError] = useState('');

  useEffect(() => {
    setOrcLoading(true);
    setOrcData(null);
    fetch(`${API_URL}/api/online-learning/orc-snapshots?lottery=${lottery}&limit=20`)
      .then(r => r.json())
      .then(setOrcData)
      .catch(() => {})
      .finally(() => setOrcLoading(false));
  }, [lottery]);

  const validate = useCallback(() => {
    if (!validateDrawId.trim()) { setValidationError('Enter a draw ID'); return; }
    setValidationError('');
    setValidating(true);
    setValidationResult(null);
    fetch(`${API_URL}/api/online-learning/validate-hashes?lottery=${lottery}&draw_id=${validateDrawId.trim()}`, { method: 'POST' })
      .then(r => r.json())
      .then(d => { if (d.detail) setValidationError(d.detail); else setValidationResult(d); })
      .catch(e => setValidationError(String(e)))
      .finally(() => setValidating(false));
  }, [lottery, validateDrawId]);

  return (
    <div>
      <p style={{ fontSize: '0.85rem', color: '#666', margin: '0 0 16px' }}>
        Each draw generates a <strong>.orc</strong> (binary model snapshot) and a <strong>.txt</strong> (full wheel).
        Both share a SHA-256 hash stored in MongoDB. Validate here to confirm files have not been modified.
      </p>

      {/* Manual validation */}
      <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: '14px 16px', marginBottom: 20 }}>
        <div style={{ fontSize: '0.8rem', fontWeight: 600, marginBottom: 10 }}>🔐 Validate hash for a specific draw</div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
          <input value={validateDrawId} onChange={e => setValidateDrawId(e.target.value)} placeholder="Draw ID"
            style={{ padding: '6px 10px', borderRadius: 4, border: '1px solid #ddd', fontSize: '0.85rem', width: 160 }} />
          <button onClick={validate} disabled={validating}
            style={{ padding: '6px 16px', borderRadius: 4, border: 'none', background: color, color: '#fff', fontWeight: 600, cursor: 'pointer', opacity: validating ? 0.6 : 1 }}>
            {validating ? 'Validating...' : 'Validate'}
          </button>
        </div>
        {validationError && <div style={{ color: '#dc2626', fontSize: '0.82rem', marginTop: 8 }}>{validationError}</div>}
        {validationResult && (
          <div style={{ marginTop: 12, padding: '10px 14px', borderRadius: 6,
            background: validationResult.valid ? '#dcfce7' : '#fee2e2',
            border: `1px solid ${validationResult.valid ? '#86efac' : '#fca5a5'}` }}>
            <div style={{ fontWeight: 700, marginBottom: 6, color: validationResult.valid ? '#16a34a' : '#dc2626' }}>
              {validationResult.valid ? '✅ All hashes valid' : '❌ Hash mismatch detected'}
            </div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 6 }}>
              <Badge ok={validationResult.orc_match} label={`ORC: ${validationResult.orc_match ? 'OK' : 'MISMATCH'}`} />
              <Badge ok={validationResult.txt_match} label={`TXT: ${validationResult.txt_match ? 'OK' : 'MISMATCH'}`} />
            </div>
            <div style={{ fontSize: '0.78rem', color: '#555' }}>{validationResult.details}</div>
          </div>
        )}
      </div>

      {/* ORC snapshot history */}
      <div style={{ fontSize: '0.8rem', fontWeight: 600, marginBottom: 8 }}>Recent ORC snapshots</div>
      {orcLoading ? (
        <div style={{ color: '#888', fontSize: '0.82rem' }}>Loading...</div>
      ) : !orcData || !orcData.rows.length ? (
        <div style={{ color: '#aaa', fontSize: '0.82rem' }}>
          No ORC snapshots yet. They are generated automatically by the daily automation after each full wheel.
        </div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
            <thead>
              <tr style={{ background: '#f5f5f5' }}>
                {['Draw ID', 'Created at', 'ORC hash (first 16)', 'TXT hash (first 16)'].map(h => (
                  <th key={h} style={{ padding: '6px 10px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e0e0e0' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {orcData.rows.map((row, i) => (
                <tr key={row.draw_id} style={{ background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                  <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', fontWeight: 600 }}>{row.draw_id}</td>
                  <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', color: '#888' }}>{row.created_at?.slice(0, 16)}</td>
                  <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', fontFamily: 'monospace', color: '#555' }}>
                    {row.orc_hash ? row.orc_hash.slice(0, 16) + '...' : '—'}
                  </td>
                  <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', fontFamily: 'monospace', color: '#555' }}>
                    {row.txt_hash ? row.txt_hash.slice(0, 16) + '...' : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Cross-Validation Section ──────────────────────────────────────────────────

function CrossValidationSection({ lottery, color }: { lottery: LotterySlug; color: string }) {
  const [data, setCvData] = useState<CrossValData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    setCvData(null);
    fetch(`${API_URL}/api/validation/cross-validation?lottery=${lottery}&limit=50`)
      .then(r => r.json())
      .then(setCvData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [lottery]);

  if (loading) return <div style={{ color: '#888', padding: 16 }}>Loading cross-validation data...</div>;
  if (!data) return <div style={{ color: '#aaa', padding: 16 }}>No cross-validation data available.</div>;

  const s = data.summary;

  return (
    <div>
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
        <StatCard label="Draws analysed" value={fmt(data.total_draws)} color={color} />
        <StatCard label="In top 1%" value={`${s.in_top_1pct} (${s.pct_in_top_1pct}%)`}
          sub={`jackpot in top ${pct(data.total_tickets * 0.01, data.total_tickets)} of tickets`} color="#22c55e" />
        <StatCard label="In top 5%" value={`${s.in_top_5pct} (${s.pct_in_top_5pct}%)`} color="#84cc16" />
        <StatCard label="In top 10%" value={`${s.in_top_10pct} (${s.pct_in_top_10pct}%)`} color="#f59e0b" />
        <StatCard label="In top 25%" value={`${s.in_top_25pct} (${s.pct_in_top_25pct}%)`} color="#f97316" />
      </div>

      {/* Percentile bar chart */}
      <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: '12px 16px', marginBottom: 16 }}>
        <div style={{ fontSize: '0.72rem', color: '#888', marginBottom: 6 }}>Jackpot percentile per draw (lower = better)</div>
        <MiniBarChart rows={[...data.rows].reverse() as unknown as Record<string, number | string>[]}
          valueKey="percentile" labelKey="draw_date" color={color} height={90} />
      </div>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
          <thead>
            <tr style={{ background: '#f5f5f5' }}>
              {['Draw date', 'Draw ID', 'Jackpot position', 'Percentile', 'Top 1%', 'Top 5%', 'Top 10%', 'Top 25%'].map(h => (
                <th key={h} style={{ padding: '6px 10px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e0e0e0' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.rows.map((row, i) => (
              <tr key={row.draw_id} style={{ background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}>{row.draw_date}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', color: '#888' }}>{row.draw_id}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', fontWeight: 600 }}>{fmt(row.jackpot_position)}</td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', color: row.percentile < 10 ? '#22c55e' : '#f59e0b' }}>
                  {row.percentile.toFixed(3)}%
                </td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}><Badge ok={row.in_top_1pct} label={row.in_top_1pct ? '✓' : '✗'} /></td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}><Badge ok={row.in_top_5pct} label={row.in_top_5pct ? '✓' : '✗'} /></td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}><Badge ok={row.in_top_10pct} label={row.in_top_10pct ? '✓' : '✗'} /></td>
                <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}><Badge ok={row.in_top_25pct} label={row.in_top_25pct ? '✓' : '✗'} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Online Learning History Section ──────────────────────────────────────────

function OnlineLearningSection({ lottery, color }: { lottery: LotterySlug; color: string }) {
  const [data, setData] = useState<FeedbackData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);

  useEffect(() => {
    setPage(1);
  }, [lottery]);

  useEffect(() => {
    setLoading(true);
    setError('');
    const skip = (page - 1) * pageSize;
    fetch(`${API_URL}/api/online-learning/history?lottery=${lottery}&skip=${skip}&limit=${pageSize}`)
      .then(async r => {
        const body = await r.json();
        if (!r.ok) throw new Error(body.detail || `HTTP ${r.status}`);
        return body;
      })
      .then(setData)
      .catch(e => {
        setData(null);
        setError(String(e.message || e));
      })
      .finally(() => setLoading(false));
  }, [lottery, page, pageSize]);

  const handlePageChange = (nextPage: number, nextPageSize: number) => {
    setPage(nextPage);
    setPageSize(nextPageSize);
  };

  if (loading && !data) return <div style={{ color: '#888', padding: 16 }}>Loading learning history...</div>;
  if (error) return <div style={{ color: '#dc2626', padding: 16 }}>Could not load learning history: {error}</div>;
  if (!data) return <div style={{ color: '#aaa', padding: 16 }}>No learning history available.</div>;

  const diag = data.diagnostics;

  return (
    <div>
      <p style={{ fontSize: '0.85rem', color: '#666', margin: '0 0 16px', lineHeight: 1.5 }}>
        Each row is one <strong>post-draw feedback cycle</strong>: after a compare result exists, the system loads the
        ORC snapshot from the pre-draw, updates GBM/LSTM weights, and logs the result here. Compare results alone do not
        appear in this tab.
      </p>

      {diag && (
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
          <StatCard label="Compare results" value={fmt(diag.compare_count)} color={color}
            sub="draws with jackpot position" />
          <StatCard label="ORC snapshots" value={fmt(diag.orc_count)} color="#6366f1"
            sub="model state before each draw" />
          <StatCard label="Feedback cycles" value={fmt(diag.feedback_count)} color="#22c55e"
            sub="post-draw learning runs logged" />
          {diag.pending_feedback > 0 && (
            <StatCard label="Missing feedback" value={fmt(diag.pending_feedback)} color="#ef4444"
              sub="compares without a feedback log yet" />
          )}
        </div>
      )}

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
        <StatCard label="Total logged" value={fmt(data.total)} color={color}
          sub="each cycle = one draw result processed" />
        {data.rows.length > 0 && (
          <StatCard label="Latest error rate"
            value={(data.rows[0].error_rate * 100).toFixed(4) + '%'}
            sub={`${data.rows[0].draw_date || data.rows[0].draw_id} · draw ${data.rows[0].draw_id}`}
            color={data.rows[0].error_rate < 0.1 ? '#22c55e' : '#f59e0b'} />
        )}
      </div>

      {data.rows.length === 0 ? (
        <div style={{ color: '#555', fontSize: '0.85rem', padding: '12px 14px', background: '#fffbeb',
          border: '1px solid #fde68a', borderRadius: 8, lineHeight: 1.6 }}>
          <strong>No feedback cycles yet.</strong>
          {diag && diag.compare_count > 0 ? (
            <>
              {' '}You have {fmt(diag.compare_count)} compare result{diag.compare_count !== 1 ? 's' : ''} but
              {diag.orc_count === 0 ? ' no ORC snapshots' : ` only ${fmt(diag.orc_count)} ORC snapshot${diag.orc_count !== 1 ? 's' : ''}`}.
              Post-draw feedback needs both. Run the repair script to backfill:
              <pre style={{ margin: '10px 0 0', padding: '10px 12px', background: '#fef3c7', borderRadius: 6,
                fontSize: '0.78rem', overflowX: 'auto', whiteSpace: 'pre-wrap' }}>
{`python scripts/backfill_learning_pipeline.py \\
  --lottery ${lottery} \\
  --mode repair-feedback \\
  --last 24 \\
  --api-url http://localhost:8000`}
              </pre>
            </>
          ) : (
            <> The daily automation runs post-draw feedback after each full pipeline (train → wheel → compare → feedback).</>
          )}
        </div>
      ) : (
        <>
          <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: '12px 16px', marginBottom: 16 }}>
            <div style={{ fontSize: '0.72rem', color: '#888', marginBottom: 6 }}>
              Error rate per feedback cycle — newest draws on the right
            </div>
            <LineChart
              rows={data.rows.slice().reverse().map(r => ({
                ...r,
                chart_label: r.draw_date || r.draw_id,
              })) as unknown as Record<string, number | string>[]}
              valueKey="error_rate" labelKey="chart_label" color={color} height={120} />
          </div>

          <div style={{ marginBottom: 8, fontSize: '0.8rem', color: '#888' }}>
            Newest draws first
          </div>

          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
              <thead>
                <tr style={{ background: '#f5f5f5' }}>
                  {['Draw date', 'Draw ID', 'Pre-draw ID', 'Jackpot position', 'Error rate', 'Models updated'].map(h => (
                    <th key={h} style={{ padding: '6px 10px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e0e0e0' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.rows.map((row, i) => (
                  <tr key={row.draw_id} style={{ background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                    <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}>{row.draw_date || '—'}</td>
                    <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', fontWeight: 600, color: '#888' }}>{row.draw_id}</td>
                    <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0', color: '#888' }}>{row.pre_draw_id}</td>
                    <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}>{fmt(row.actual_jackpot_position)}</td>
                    <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0',
                      color: row.error_rate < 0.1 ? '#22c55e' : '#f59e0b', fontWeight: 600 }}>
                      {(row.error_rate * 100).toFixed(4)}%
                    </td>
                    <td style={{ padding: '5px 10px', borderBottom: '1px solid #f0f0f0' }}>
                      {(row.feedback_records || []).map(fr => (
                        <span key={fr.model} style={{ fontSize: '0.7rem', background: '#e0f2fe', color: '#0369a1',
                          borderRadius: 4, padding: '1px 5px', marginRight: 4 }}>
                          {fr.model.split('_').slice(-2).join('_')}
                          {'added_estimators' in fr && fr.added_estimators != null
                            ? ` +${fr.added_estimators}`
                            : 'gradient_steps' in fr && fr.gradient_steps != null
                              ? ` grad×${fr.gradient_steps}`
                              : ''}
                        </span>
                      ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <TablePagination page={page} pageSize={pageSize} total={data.total} onChange={handlePageChange} />
        </>
      )}
    </div>
  );
}

// ── Score vs position scatter (client metric #4) ─────────────────────────────

function ScoreScatterSection({ lottery, color }: { lottery: LotterySlug; color: string }) {
  const [data, setData] = useState<null | {
    total_points: number;
    points: { draw_id: string; draw_date: string; score: number; jackpot_position: number }[];
  }>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    fetch(`${API_URL}/api/validation/score-vs-position?lottery=${lottery}&limit=120`)
      .then(r => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [lottery]);

  if (loading) return <div style={{ color: '#888', padding: 16 }}>Loading scatter data...</div>;
  if (!data?.points?.length) return <div style={{ color: '#aaa', padding: 16 }}>No scatter data (need compare results + draw_probs).</div>;

  const chartPoints = data.points.map(p => ({
    ...p,
    x: p.score,
    y: p.jackpot_position,
    label: p.draw_date || p.draw_id,
  }));

  return (
    <div>
      <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap' }}>
        <StatCard label="Draws plotted" value={String(data.total_points)} color={color} />
        <StatCard label="Interpretation" value="↙ better" sub="high score + low position = good ranking" color="#22c55e" />
      </div>
      <div style={{ width: '100%', height: 420, background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: 12 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 12, right: 24, bottom: 24, left: 24 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" dataKey="x" name="Model score" tick={{ fontSize: 11 }} label={{ value: 'Model score (log-prob)', position: 'bottom', offset: 0, fontSize: 11 }} />
            <YAxis type="number" dataKey="y" name="Jackpot position" tick={{ fontSize: 11 }} scale="log" domain={['auto', 'auto']} label={{ value: 'Jackpot position (log)', angle: -90, position: 'insideLeft', fontSize: 11 }} />
            <ZAxis range={[40, 40]} />
            <Tooltip cursor={{ strokeDasharray: '3 3' }} />
            <Scatter name="Draws" data={chartPoints} fill={color} />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
      <p style={{ fontSize: '0.78rem', color: '#888', marginTop: 10 }}>
        One point per draw: X = log-probability score of the winning ticket (pre-draw model); Y = its rank in the full wheel.
        Cluster toward bottom-right means the model assigns high scores to tickets that rank well.
      </p>
    </div>
  );
}

// ── Combined model performance (client 1.md) ──────────────────────────────────

interface ModelPerformanceData {
  lottery: string;
  accuracy_history: number[];
  error_history: number[];
  total_draws: number;
  avg_mean_error: number;
  learning_cycles: number;
  last_validation: {
    draw_id: string;
    draw_date: string;
    jackpot_position: number;
    mean_error: number;
    error_rate_pct: number;
    improvement_since_last: string;
  } | null;
  recent_feedback: { draw_id: string; draw_date?: string; error_rate: number; updated_at: string }[];
}

function ModelPerformanceSection({ lottery, color }: { lottery: LotterySlug; color: string }) {
  const [data, setData] = useState<ModelPerformanceData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    fetch(`${API_URL}/api/validation/model-performance?lottery=${lottery}&limit=100`)
      .then(r => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [lottery]);

  if (loading) return <div style={{ color: '#888', padding: 16 }}>Loading model performance...</div>;
  if (!data) return <div style={{ color: '#aaa', padding: 16 }}>No performance data available.</div>;

  const last = data.last_validation;

  return (
    <div>
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
        <StatCard label="Draws in report" value={String(data.total_draws)} color={color} />
        <StatCard label="Avg jackpot position" value={fmt(data.avg_mean_error)} color="#f59e0b" />
        <StatCard label="Learning cycles" value={String(data.learning_cycles)} sub="post-draw feedback runs" color="#6366f1" />
      </div>

      {last && (
        <div style={{ background: '#f0fdf4', border: '1px solid #86efac', borderRadius: 8, padding: '14px 18px', marginBottom: 16, fontSize: '0.85rem' }}>
          <div style={{ fontWeight: 700, marginBottom: 8, color: '#166534' }}>MODEL VALIDATION — draw {last.draw_date}</div>
          <div>Model trained with data up to pre-draw · Tested on draw <strong>{last.draw_id}</strong></div>
          <div style={{ marginTop: 6 }}>Jackpot position: <strong>{fmt(last.jackpot_position)}</strong> · Error rate: <strong>{last.error_rate_pct}%</strong></div>
          <div style={{ marginTop: 4 }}>Trend vs previous draw: <strong>{last.improvement_since_last}</strong></div>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
        <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: 12 }}>
          <div style={{ fontSize: '0.72rem', color: '#888', marginBottom: 6 }}>Rank quality % over time (higher = better)</div>
          <LineChart rows={data.accuracy_history.map((v, i) => ({ i, v })) as unknown as Record<string, number | string>[]} valueKey="v" labelKey="i" color={color} height={120} />
        </div>
        <div style={{ background: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 8, padding: 12 }}>
          <div style={{ fontSize: '0.72rem', color: '#888', marginBottom: 6 }}>Jackpot position per draw (lower = better)</div>
          <MiniBarChart rows={data.error_history.map((v, i) => ({ i, v })) as unknown as Record<string, number | string>[]} valueKey="v" labelKey="i" color="#f59e0b" height={120} maxRows={50} />
        </div>
      </div>

      {data.recent_feedback.length > 0 && (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
            <thead>
              <tr style={{ background: '#f5f5f5' }}>
                {['Draw date', 'Draw ID', 'Error rate', 'Updated'].map(h => (
                  <th key={h} style={{ padding: '6px 10px', textAlign: 'left', fontWeight: 600 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.recent_feedback.map((row, i) => (
                <tr key={row.draw_id} style={{ background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                  <td style={{ padding: '5px 10px' }}>{row.draw_date || '—'}</td>
                  <td style={{ padding: '5px 10px', color: '#888' }}>{row.draw_id}</td>
                  <td style={{ padding: '5px 10px' }}>{(row.error_rate * 100).toFixed(4)}%</td>
                  <td style={{ padding: '5px 10px', color: '#888' }}>{row.updated_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Main Dashboard ────────────────────────────────────────────────────────────

type TabId = 'accuracy' | 'mean-error' | 'top-tickets' | 'cross-validation' | 'hash-validation' | 'learning-history' | 'score-scatter' | 'model-performance';

const TABS: { id: TabId; label: string; icon: string }[] = [
  { id: 'accuracy',          label: 'Accuracy per draw',    icon: '📈' },
  { id: 'mean-error',        label: 'Mean error',           icon: '📉' },
  { id: 'top-tickets',       label: 'Top tickets',          icon: '🎫' },
  { id: 'cross-validation',  label: 'Cross-validation',     icon: '🔬' },
  { id: 'score-scatter',     label: 'Score vs position',    icon: '📊' },
  { id: 'model-performance', label: 'Model performance',    icon: '📋' },
  { id: 'hash-validation',   label: 'Hash validation',      icon: '🔐' },
  { id: 'learning-history',  label: 'Learning history',     icon: '🧠' },
];

export function ValidationDashboard() {
  const [lottery, setLottery] = useState<LotterySlug>('euromillones');
  const [tab, setTab] = useState<TabId>('accuracy');
  const cfg = LOTTERIES.find(l => l.slug === lottery)!;

  return (
    <div style={{ padding: 24, maxWidth: 1400, margin: '0 auto', fontFamily: 'system-ui, sans-serif' }}>

      {/* Header */}
      <h2 style={{ margin: '0 0 4px', fontSize: '1.4rem' }}>🔬 Validation Dashboard</h2>
      <p style={{ margin: '0 0 20px', color: '#888', fontSize: '0.88rem' }}>
        Model accuracy, learning evolution, top-ticket analysis, and hash validation per draw
      </p>

      {/* Lottery selector */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 20, flexWrap: 'wrap' }}>
        {LOTTERIES.map(l => (
          <button key={l.slug} onClick={() => setLottery(l.slug)} style={{
            display: 'flex', alignItems: 'center', gap: 8,
            padding: '8px 18px', borderRadius: 999, cursor: 'pointer',
            border: `2px solid ${lottery === l.slug ? l.color : '#ddd'}`,
            background: lottery === l.slug ? l.color : '#fff',
            color: lottery === l.slug ? '#fff' : '#333',
            fontWeight: lottery === l.slug ? 700 : 400,
          }}>
            <img src={l.img} alt="" style={{ width: 22, height: 22, objectFit: 'contain' }} />
            {l.label}
          </button>
        ))}
      </div>

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20, flexWrap: 'wrap', borderBottom: '2px solid #f0f0f0', paddingBottom: 0 }}>
        {TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{
            padding: '8px 14px', borderRadius: '6px 6px 0 0', cursor: 'pointer',
            border: 'none', borderBottom: tab === t.id ? `3px solid ${cfg.color}` : '3px solid transparent',
            background: tab === t.id ? '#fff' : 'transparent',
            color: tab === t.id ? cfg.color : '#666',
            fontWeight: tab === t.id ? 700 : 400,
            fontSize: '0.85rem',
          }}>
            {t.icon} {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div style={{ background: '#fff', border: '1px solid #e0e0e0', borderRadius: 8, padding: 20 }}>
        {tab === 'accuracy' && (
          <>
            <SectionTitle>📈 Accuracy chart per draw</SectionTitle>
            <p style={{ fontSize: '0.82rem', color: '#666', margin: '0 0 16px' }}>
              Jackpot position in the ranked full wheel for each draw. Lower position = model ranked the winning ticket higher.
              The rolling mean line shows the learning trend.
            </p>
            <AccuracySection lottery={lottery} color={cfg.color} />
          </>
        )}

        {tab === 'mean-error' && (
          <>
            <SectionTitle>📉 Mean error chart</SectionTitle>
            <p style={{ fontSize: '0.82rem', color: '#666', margin: '0 0 16px' }}>
              Distance between the predicted rank and the actual jackpot position.
              The cumulative mean should decrease over time as the model accumulates knowledge.
            </p>
            <MeanErrorSection lottery={lottery} color={cfg.color} />
          </>
        )}

        {tab === 'top-tickets' && (
          <>
            <SectionTitle>🎫 Top 100 / 1000 tickets</SectionTitle>
            <TopTicketsSection lottery={lottery} color={cfg.color} />
          </>
        )}

        {tab === 'cross-validation' && (
          <>
            <SectionTitle>🔬 Cross-validation report per draw</SectionTitle>
            <p style={{ fontSize: '0.82rem', color: '#666', margin: '0 0 16px' }}>
              For each draw, shows what percentile the jackpot ticket was ranked in.
              "In top 1%" means the model ranked the winning ticket in the top 1% of all tickets.
            </p>
            <CrossValidationSection lottery={lottery} color={cfg.color} />
          </>
        )}

        {tab === 'score-scatter' && (
          <>
            <SectionTitle>📊 Score vs actual position</SectionTitle>
            <p style={{ fontSize: '0.82rem', color: '#666', margin: '0 0 16px' }}>
              Scatter plot (client metric #4): model score vs jackpot rank per draw.
            </p>
            <ScoreScatterSection lottery={lottery} color={cfg.color} />
          </>
        )}

        {tab === 'model-performance' && (
          <>
            <SectionTitle>📋 Model performance summary</SectionTitle>
            <p style={{ fontSize: '0.82rem', color: '#666', margin: '0 0 16px' }}>
              Combined learning report: accuracy trend, error history, and last validation (client spec).
            </p>
            <ModelPerformanceSection lottery={lottery} color={cfg.color} />
          </>
        )}

        {tab === 'hash-validation' && (
          <>
            <SectionTitle>🔐 Hash validation (.orc ↔ .txt)</SectionTitle>
            <HashValidationSection lottery={lottery} color={cfg.color} />
          </>
        )}

        {tab === 'learning-history' && (
          <>
            <SectionTitle>🧠 Online learning history</SectionTitle>
            <p style={{ fontSize: '0.82rem', color: '#666', margin: '0 0 16px' }}>
              Each row is one feedback cycle: after a draw result, the model weights are updated
              using the error signal. The error rate should decrease over time.
            </p>
            <OnlineLearningSection lottery={lottery} color={cfg.color} />
          </>
        )}
      </div>
    </div>
  );
}
