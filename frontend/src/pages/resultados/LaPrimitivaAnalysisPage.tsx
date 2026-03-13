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

type AnalysisRowLaPrimitiva = {
  date: string;
  current_id: string;
  pre_id: string;
  pos_1th: number;
  pos_2th: number | null;
  pos_3th: number | null;
  pos_4th: number | null;
  pos_5th: number | null;
};

export function LaPrimitivaAnalysisPage() {
  const [rows, setRows] = useState<AnalysisRowLaPrimitiva[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showGraph, setShowGraph] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const res = await fetch(`${API_URL}/api/la-primitiva/compare/analysis?limit=200`, {
          cache: 'no-store',
        });
        const data = await res.json();
        if (!res.ok || data.detail) {
          throw new Error(
            typeof data.detail === 'string'
              ? data.detail
              : 'Error al cargar análisis full wheel La Primitiva',
          );
        }
        if (cancelled) return;
        setRows(Array.isArray(data.rows) ? (data.rows as AnalysisRowLaPrimitiva[]) : []);
      } catch (e) {
        if (cancelled) return;
        setError(
          e instanceof Error ? e.message : 'Error al cargar análisis full wheel La Primitiva',
        );
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
          pos_1th: r.pos_1th || null,
          pos_2th: r.pos_2th,
          pos_3th: r.pos_3th,
          pos_4th: r.pos_4th,
          pos_5th: r.pos_5th,
        })),
    [rows],
  );

  return (
    <section className="card resultados-features-card resultados-theme-la-primitiva">
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 'var(--space-sm)',
        }}
      >
        <h3 style={{ marginTop: 0, marginBottom: '0.75rem' }}>
          Análisis full wheel (La Primitiva)
        </h3>
        {rows.length > 0 && (
          <button
            type="button"
            className="form-input"
            onClick={() => setShowGraph((v) => !v)}
            title={showGraph ? 'Ocultar gráfico' : 'Ver gráfico (1ª–5ª)'}
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
                <th>1ª pos (6)</th>
                <th>2ª pos (5 + C)</th>
                <th>3ª pos (5)</th>
                <th>4ª pos (4)</th>
                <th>5ª pos (3)</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={`${r.date}-${r.current_id}-${r.pre_id}`}>
                  <td>{r.date || '—'}</td>
                  <td>{r.pos_1th ? r.pos_1th.toLocaleString() : '—'}</td>
                  <td>{r.pos_2th != null ? r.pos_2th.toLocaleString() : '—'}</td>
                  <td>{r.pos_3th != null ? r.pos_3th.toLocaleString() : '—'}</td>
                  <td>{r.pos_4th != null ? r.pos_4th.toLocaleString() : '—'}</td>
                  <td>{r.pos_5th != null ? r.pos_5th.toLocaleString() : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <Drawer
        title="Gráfico de posiciones (1ª–5ª)"
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
                <Line
                  type="monotone"
                  dataKey="pos_1th"
                  name="1ª (6)"
                  stroke="#dc2626"
                  dot={false}
                />
                <Line
                  type="monotone"
                  dataKey="pos_2th"
                  name="2ª (5 + C)"
                  stroke="#f59e0b"
                  dot={false}
                />
                <Line
                  type="monotone"
                  dataKey="pos_3th"
                  name="3ª (5)"
                  stroke="#2563eb"
                  dot={false}
                />
                <Line
                  type="monotone"
                  dataKey="pos_4th"
                  name="4ª (4)"
                  stroke="#16a34a"
                  dot={false}
                />
                <Line
                  type="monotone"
                  dataKey="pos_5th"
                  name="5ª (3)"
                  stroke="#22c55e"
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </Drawer>
    </section>
  );
}

