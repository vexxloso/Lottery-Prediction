import { useEffect, useState } from 'react';

const API_URL = (import.meta.env.VITE_API_URL ?? 'http://localhost:8000').replace(/\/$/, '');

type LotterySlug = 'euromillones' | 'el-gordo' | 'la-primitiva';

const LOTTERIES: { slug: LotterySlug; label: string; color: string; img: string }[] = [
  { slug: 'euromillones', label: 'Euromillones', color: '#1976D2', img: '/images/euromillones.png' },
  { slug: 'el-gordo',     label: 'El Gordo',     color: '#c41230', img: '/images/el-gordo.png'     },
  { slug: 'la-primitiva', label: 'La Primitiva', color: '#00843d', img: '/images/la-primitiva.png' },
];

interface Summary {
  total_draws: number;
  ml_draws: number;
  freq_gap_draws: number;
  ml_pct: number;
  first_draw: { id: string; date: string; source: string } | null;
  latest_draw: { id: string; date: string; source: string } | null;
  top_numbers_latest: { number: number; p: number }[];
}

interface ProbItem { number: number; p: number; }

interface ProgressRow {
  draw_id: string;
  draw_date: string;
  source: string;
  top_mains: ProbItem[];
  top_secondary: ProbItem[];
  secondary_label: string;
}

function hexToRgb(hex: string) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return { r, g, b };
}

// Ball with opacity based on probability rank
function ProbBall({ item, maxP, color, size = 28 }: { item: ProbItem; maxP: number; color: string; size?: number }) {
  const ratio = maxP > 0 ? item.p / maxP : 0;
  const { r, g, b } = hexToRgb(color);
  const bg = `rgba(${r},${g},${b},${0.15 + ratio * 0.85})`;
  const textColor = ratio > 0.5 ? '#fff' : '#333';
  return (
    <div style={{ textAlign: 'center', display: 'inline-block', margin: '2px' }}>
      <div style={{
        width: size, height: size, borderRadius: '50%',
        background: bg, color: textColor,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontSize: size < 28 ? '0.65rem' : '0.72rem', fontWeight: 700,
        border: `1px solid rgba(${r},${g},${b},0.3)`,
      }}>
        {item.number}
      </div>
      <div style={{ fontSize: '0.55rem', color: '#999', marginTop: 1 }}>
        {(item.p * 100).toFixed(1)}
      </div>
    </div>
  );
}

function SourceBadge({ source }: { source: string }) {
  const isML = source === 'ml_model';
  return (
    <span style={{
      fontSize: '0.68rem', padding: '2px 7px', borderRadius: 999,
      background: isML ? '#22c55e22' : '#eab30822',
      color: isML ? '#16a34a' : '#a16207', fontWeight: 600,
    }}>
      {isML ? '🤖 ML' : '📊 Freq/Gap'}
    </span>
  );
}

export function RankingDashboard() {
  const [lottery, setLottery] = useState<LotterySlug>('euromillones');
  const cfg = LOTTERIES.find(l => l.slug === lottery)!;

  const [summary, setSummary] = useState<Summary | null>(null);
  const [summaryLoading, setSummaryLoading] = useState(false);

  const [rows, setRows] = useState<ProgressRow[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const PAGE_SIZE = 10;

  useEffect(() => {
    setSummaryLoading(true);
    setSummary(null);
    fetch(`${API_URL}/api/ranking/study-summary?lottery=${lottery}`)
      .then(r => r.json())
      .then(d => setSummary(d))
      .catch(() => {})
      .finally(() => setSummaryLoading(false));
  }, [lottery]);

  useEffect(() => {
    setLoading(true);
    const skip = (page - 1) * PAGE_SIZE;
    fetch(`${API_URL}/api/ranking/study-progress?lottery=${lottery}&limit=${PAGE_SIZE}&skip=${skip}`)
      .then(r => r.json())
      .then(d => { setRows(d.rows ?? []); setTotal(d.total ?? 0); })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [lottery, page]);

  useEffect(() => { setPage(1); }, [lottery]);

  const totalPages = Math.ceil(total / PAGE_SIZE);

  return (
    <div style={{ padding: 24, maxWidth: 1400, margin: '0 auto', fontFamily: 'system-ui, sans-serif' }}>

      {/* Header */}
      <h2 style={{ margin: '0 0 4px', fontSize: '1.4rem' }}>📊 Study Progress Dashboard</h2>
      <p style={{ margin: '0 0 20px', color: '#888', fontSize: '0.88rem' }}>
        Probability evolution per draw — darker ball = higher probability assigned by model
      </p>

      {/* Lottery selector */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 24, flexWrap: 'wrap' }}>
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

      {/* Summary */}
      {summaryLoading ? (
        <p style={{ color: '#888' }}>Loading summary...</p>
      ) : summary ? (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 24 }}>

          <div style={{ background: '#fff', border: '1px solid #e0e0e0', borderRadius: 8, padding: '14px 18px' }}>
            <div style={{ fontSize: '0.7rem', color: '#888', textTransform: 'uppercase', marginBottom: 4 }}>Total draws studied</div>
            <div style={{ fontSize: '1.8rem', fontWeight: 700, color: cfg.color }}>{summary.total_draws.toLocaleString()}</div>
            <div style={{ fontSize: '0.72rem', color: '#888' }}>{summary.first_draw?.date} → {summary.latest_draw?.date}</div>
          </div>

          <div style={{ background: '#fff', border: '1px solid #e0e0e0', borderRadius: 8, padding: '14px 18px' }}>
            <div style={{ fontSize: '0.7rem', color: '#888', textTransform: 'uppercase', marginBottom: 4 }}>🤖 ML model draws</div>
            <div style={{ fontSize: '1.8rem', fontWeight: 700, color: '#22c55e' }}>{summary.ml_draws.toLocaleString()}</div>
            <div style={{ background: '#e0e0e0', borderRadius: 4, height: 6, marginTop: 6 }}>
              <div style={{ width: `${summary.ml_pct}%`, height: '100%', background: '#22c55e', borderRadius: 4 }} />
            </div>
            <div style={{ fontSize: '0.7rem', color: '#888', marginTop: 4 }}>{summary.ml_pct}% of total</div>
          </div>

          <div style={{ background: '#fff', border: '1px solid #e0e0e0', borderRadius: 8, padding: '14px 18px' }}>
            <div style={{ fontSize: '0.7rem', color: '#888', textTransform: 'uppercase', marginBottom: 4 }}>📊 Freq/Gap draws</div>
            <div style={{ fontSize: '1.8rem', fontWeight: 700, color: '#eab308' }}>{summary.freq_gap_draws.toLocaleString()}</div>
            <div style={{ fontSize: '0.72rem', color: '#888' }}>{(100 - summary.ml_pct).toFixed(1)}% of total</div>
          </div>

          {/* Latest draw top numbers */}
          <div style={{ background: '#fff', border: `2px solid ${cfg.color}`, borderRadius: 8, padding: '14px 18px', gridColumn: 'span 2' }}>
            <div style={{ fontSize: '0.7rem', color: '#888', textTransform: 'uppercase', marginBottom: 8 }}>
              Latest draw predictions ({summary.latest_draw?.date}) &nbsp;
              <SourceBadge source={summary.latest_draw?.source ?? ''} />
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
              {summary.top_numbers_latest.map((item) => {
                const maxP = summary.top_numbers_latest[0]?.p ?? 1;
                return <ProbBall key={item.number} item={item} maxP={maxP} color={cfg.color} size={32} />;
              })}
            </div>
          </div>
        </div>
      ) : null}

      {/* Progress table */}
      <div style={{ background: '#fff', border: '1px solid #e0e0e0', borderRadius: 8, overflow: 'hidden' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid #e0e0e0', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
          <h3 style={{ margin: 0, fontSize: '1rem' }}>
            Draw-by-draw probability history — {total.toLocaleString()} draws
          </h3>
          <span style={{ fontSize: '0.78rem', color: '#888' }}>
            Darker = higher probability · Sorted newest first
          </span>
        </div>

        {loading ? (
          <div style={{ padding: 40, textAlign: 'center', color: '#888' }}>Loading...</div>
        ) : rows.length === 0 ? (
          <div style={{ padding: 40, textAlign: 'center', color: '#888' }}>No data</div>
        ) : (
          <div>
            {rows.map((row, i) => {
              const maxMainP = row.top_mains[0]?.p ?? 1;
              const maxSecP  = row.top_secondary[0]?.p ?? 1;
              return (
                <div key={row.draw_id} style={{
                  padding: '14px 16px',
                  borderBottom: i < rows.length - 1 ? '1px solid #f0f0f0' : 'none',
                  background: i % 2 === 0 ? '#fff' : '#fafafa',
                }}>
                  {/* Row header */}
                  <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 10, flexWrap: 'wrap' }}>
                    <span style={{ fontWeight: 700, fontSize: '0.95rem', minWidth: 90 }}>{row.draw_date}</span>
                    <SourceBadge source={row.source} />
                    <span style={{ fontSize: '0.75rem', color: '#aaa' }}>ID: {row.draw_id}</span>
                  </div>

                  {/* All main numbers */}
                  <div style={{ marginBottom: 8 }}>
                    <div style={{ fontSize: '0.7rem', color: '#888', marginBottom: 4, textTransform: 'uppercase' }}>
                      Main numbers ({row.top_mains.length}) — ranked by probability
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 0 }}>
                      {row.top_mains.map(item => (
                        <ProbBall key={item.number} item={item} maxP={maxMainP} color={cfg.color} size={26} />
                      ))}
                    </div>
                  </div>

                  {/* All secondary numbers */}
                  {row.top_secondary.length > 0 && (
                    <div>
                      <div style={{ fontSize: '0.7rem', color: '#888', marginBottom: 4, textTransform: 'uppercase' }}>
                        {row.secondary_label} ({row.top_secondary.length})
                      </div>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 0 }}>
                        {row.top_secondary.map(item => (
                          <ProbBall key={item.number} item={item} maxP={maxSecP} color="#FFC107" size={26} />
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {/* Pagination */}
        {totalPages > 1 && (
          <div style={{ padding: '12px 16px', borderTop: '1px solid #e0e0e0', display: 'flex', justifyContent: 'center', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            <button onClick={() => setPage(1)} disabled={page === 1} style={{ padding: '5px 12px', borderRadius: 4, border: '1px solid #ddd', cursor: page === 1 ? 'not-allowed' : 'pointer', background: '#fff', opacity: page === 1 ? 0.4 : 1 }}>«</button>
            <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} style={{ padding: '5px 12px', borderRadius: 4, border: '1px solid #ddd', cursor: page === 1 ? 'not-allowed' : 'pointer', background: '#fff', opacity: page === 1 ? 0.4 : 1 }}>‹ Prev</button>
            <span style={{ padding: '5px 14px', fontSize: '0.85rem', color: '#555', background: '#f5f5f5', borderRadius: 4 }}>
              Page {page} / {totalPages} &nbsp;·&nbsp; {total.toLocaleString()} draws
            </span>
            <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page === totalPages} style={{ padding: '5px 12px', borderRadius: 4, border: '1px solid #ddd', cursor: page === totalPages ? 'not-allowed' : 'pointer', background: '#fff', opacity: page === totalPages ? 0.4 : 1 }}>Next ›</button>
            <button onClick={() => setPage(totalPages)} disabled={page === totalPages} style={{ padding: '5px 12px', borderRadius: 4, border: '1px solid #ddd', cursor: page === totalPages ? 'not-allowed' : 'pointer', background: '#fff', opacity: page === totalPages ? 0.4 : 1 }}>»</button>
          </div>
        )}
      </div>
    </div>
  );
}
