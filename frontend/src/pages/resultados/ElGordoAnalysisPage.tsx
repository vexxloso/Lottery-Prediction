import { useEffect, useMemo, useState } from 'react';
import { Drawer } from 'antd';
import {
  ResponsiveContainer,
  LineChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Line,
} from 'recharts';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

type CategoryRow = {
  category: string;
  main_hits: number;
  clave_hit: number;
  first_position: number;
  count: number;
};

type AnalysisRowElGordo = {
  date: string;
  current_id: string;
  pre_id: string;
  jackpot_position: number;
  pos_2th: number | null;
  pos_3th: number | null;
  pos_4th: number | null;
  categories: CategoryRow[];
};

export function ElGordoAnalysisPage() {
  const [rows, setRows] = useState<AnalysisRowElGordo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showGraph, setShowGraph] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const res = await fetch(`${API_URL}/api/el-gordo/compare/analysis?limit=200`, {
          cache: 'no-store',
        });
        const data = await res.json();
        if (!res.ok || data.detail) {
          throw new Error(
            typeof data.detail === 'string' ? data.detail : 'Error al cargar análisis full wheel El Gordo',
          );
        }
        if (cancelled) return;
        setRows(Array.isArray(data.rows) ? (data.rows as AnalysisRowElGordo[]) : []);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : 'Error al cargar análisis full wheel El Gordo');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  const chartData = useMemo(
    () =>
      rows
        .slice()
        .reverse()
        .map((r) => ({
          label: r.date || r.current_id,
          pos_1th: r.jackpot_position || null,
          pos_2th: r.pos_2th,
          pos_3th: r.pos_3th,
          pos_4th: r.pos_4th,
        })),
    [rows],
  );

  return (
    <section className="card resultados-features-card resultados-theme-el-gordo">
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 'var(--space-sm)',
        }}
      >
        <h3 style={{ marginTop: 0, marginBottom: '0.75rem' }}>Análisis full wheel (El Gordo)</h3>
        {rows.length > 0 && (
          <button
            type="button"
            className="form-input"
            onClick={() => setShowGraph((v) => !v)}
            title={showGraph ? 'Ocultar gráfico' : 'Ver gráfico (1ª–4ª)'}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '0.35rem 0.5rem',
              minWidth: '2.5rem',
            }}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="18"
              height="18"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden
            >
              <polyline points="3 17 9 11 13 15 21 7" />
              <polyline points="14 7 21 7 21 14" />
            </svg>
          </button>
        )}
      </div>
      {loading && rows.length === 0 && <p style={{ margin: 0 }}>Cargando análisis…</p>}
      {error && !loading && (
        <p style={{ margin: 0, color: 'var(--color-error)' }}>{error}</p>
      )}
      {!loading && !error && rows.length === 0 && (
        <p style={{ margin: 0 }}>No hay resultados de comparación full wheel.</p>
      )}
      {rows.length > 0 && (
        <div className="resultados-features-table-wrap" style={{ marginTop: 'var(--space-sm)' }}>
          <table className="resultados-features-table">
            <thead>
              <tr>
                <th>Fecha</th>
                <th>1ª pos (5+1)</th>
                <th>2ª pos (5+0)</th>
                <th>3ª pos (4+1)</th>
                <th>4ª pos (4+0)</th>
                <th>5ª pos (3+1)</th>
                <th>6ª pos (3+0)</th>
                <th>7ª pos (2+1)</th>
                <th>8ª pos (2+0)</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const getFirstPos = (hm: number, hc: number): number | null => {
                  const cat = (r.categories || []).find(
                    (c) => c.main_hits === hm && c.clave_hit === hc && c.first_position > 0,
                  );
                  return cat ? cat.first_position : null;
                };
                const pos5 = getFirstPos(3, 1);
                const pos6 = getFirstPos(3, 0);
                const pos7 = getFirstPos(2, 1);
                const pos8 = getFirstPos(2, 0);
                return (
                  <tr key={`${r.date}-${r.current_id}-${r.pre_id}`}>
                    <td>{r.date || '—'}</td>
                    <td>{r.jackpot_position ? r.jackpot_position.toLocaleString() : '—'}</td>
                    <td>{r.pos_2th != null ? r.pos_2th.toLocaleString() : '—'}</td>
                    <td>{r.pos_3th != null ? r.pos_3th.toLocaleString() : '—'}</td>
                    <td>{r.pos_4th != null ? r.pos_4th.toLocaleString() : '—'}</td>
                    <td>{pos5 != null ? pos5.toLocaleString() : '—'}</td>
                    <td>{pos6 != null ? pos6.toLocaleString() : '—'}</td>
                    <td>{pos7 != null ? pos7.toLocaleString() : '—'}</td>
                    <td>{pos8 != null ? pos8.toLocaleString() : '—'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      <Drawer
        title="Gráfico de posiciones (1ª–4ª)"
        placement="right"
        width="100%"
        open={showGraph && chartData.length > 0}
        onClose={() => setShowGraph(false)}
        bodyStyle={{ padding: 24 }}
      >
        {chartData.length === 0 ? (
          <p style={{ marginTop: 0 }}>No hay datos para el gráfico.</p>
        ) : (
          <div style={{ width: '100%', height: 520 }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="pos_1th" name="1ª (5+1)" stroke="#dc2626" dot={false} />
                <Line type="monotone" dataKey="pos_2th" name="2ª (5+0)" stroke="#f59e0b" dot={false} />
                <Line type="monotone" dataKey="pos_3th" name="3ª (4+1)" stroke="#2563eb" dot={false} />
                <Line type="monotone" dataKey="pos_4th" name="4ª (4+0)" stroke="#16a34a" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </Drawer>
    </section>
  );
}

