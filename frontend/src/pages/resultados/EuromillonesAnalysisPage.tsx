import { useEffect, useMemo, useState } from 'react';
import { Drawer, Pagination } from 'antd';
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

type AnalysisRow = {
  date: string;
  current_id: string;
  pre_id: string;
  pos_1th: number;
  pos_2th: number | null;
  pos_3th: number | null;
  pos_4th: number | null;
};

export function EuromillonesAnalysisPage() {
  const [rows, setRows] = useState<AnalysisRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showGraph, setShowGraph] = useState(false);
  const [page, setPage] = useState(1);
  const pageSize = 100;
  const [total, setTotal] = useState(0);
  const [graphRows, setGraphRows] = useState<AnalysisRow[] | null>(null);
  const [graphMode, setGraphMode] = useState<'page' | 'range2004'>('page');
  const [graphLoading, setGraphLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const skip = (page - 1) * pageSize;
        const res = await fetch(
          `${API_URL}/api/euromillones/compare/analysis?skip=${skip}&limit=${pageSize}`,
          {
            cache: 'no-store',
          },
        );
        const data = await res.json();
        if (!res.ok || data.detail) {
          throw new Error(
            typeof data.detail === 'string' ? data.detail : 'Error al cargar análisis full wheel',
          );
        }
        if (cancelled) return;
        setRows(Array.isArray(data.rows) ? (data.rows as AnalysisRow[]) : []);
        setTotal(typeof data.total === 'number' ? data.total : 0);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : 'Error al cargar análisis full wheel');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [page]);

  const activeRowsForGraph = graphMode === 'range2004' && graphRows ? graphRows : rows;

  const chartData = useMemo(
    () =>
      activeRowsForGraph
        .slice()
        // Ensure graph is always in chronological order (oldest -> newest)
        .sort((a, b) => {
          const da = a.date || '';
          const db = b.date || '';
          if (da < db) return -1;
          if (da > db) return 1;
          return 0;
        })
        .map((r) => ({
          label: r.date || r.current_id,
          pos_1th: r.pos_1th || null,
          pos_2th: r.pos_2th,
          pos_3th: r.pos_3th,
          pos_4th: r.pos_4th,
        })),
    [activeRowsForGraph],
  );

  return (
    <section className="card resultados-features-card resultados-theme-euromillones">
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 'var(--space-sm)',
        }}
      >
        <h3 style={{ marginTop: 0, marginBottom: '0.75rem' }}>Análisis full wheel (Euromillones)</h3>
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          {rows.length > 0 && (
            <button
              type="button"
              className="form-input"
              onClick={() => {
                setGraphMode('page');
                setShowGraph((v) => !v);
              }}
              title={showGraph ? 'Ocultar gráfico (página actual)' : 'Ver gráfico (página actual)'}
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
          <button
            type="button"
            className="form-input"
            onClick={async () => {
              setGraphMode('range2004');
              if (!graphRows) {
                setGraphLoading(true);
                try {
                  const res = await fetch(
                    `${API_URL}/api/euromillones/compare/analysis-graph?max_points=100`,
                    { cache: 'no-store' },
                  );
                  const data = await res.json();
                  if (!res.ok || data.detail) {
                    throw new Error(
                      typeof data.detail === 'string'
                        ? data.detail
                        : 'Error al cargar gráfico (2004–hoy)',
                    );
                  }
                  setGraphRows(Array.isArray(data.rows) ? (data.rows as AnalysisRow[]) : []);
                } catch (e) {
                  setError(
                    e instanceof Error ? e.message : 'Error al cargar gráfico (2004–hoy)',
                  );
                } finally {
                  setGraphLoading(false);
                }
              }
              setShowGraph(true);
            }}
            disabled={graphLoading}
            title="Ver gráfico desde 2004 hasta hoy (máx. 100 sorteos)"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '0.35rem 0.5rem',
              fontSize: '0.8rem',
              minWidth: 'auto',
            }}
          >
            {graphLoading ? 'Cargando 2004–hoy…' : 'Gráfico 2004–hoy (100)'}
          </button>
        </div>
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
                <th>1th pos</th>
                <th>2th pos</th>
                <th>3th pos</th>
                <th>4th pos</th>
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
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {total > pageSize && (
        <div style={{ marginTop: 'var(--space-sm)', display: 'flex', justifyContent: 'flex-end' }}>
          <Pagination
            current={page}
            pageSize={pageSize}
            total={total}
            showSizeChanger={false}
            showQuickJumper
            onChange={(p) => setPage(p)}
          />
        </div>
      )}
      <Drawer
        title={
          graphMode === 'range2004'
            ? 'Gráfico de posiciones (1th–4th) — 2004–hoy (máx. 100 sorteos)'
            : 'Gráfico de posiciones (1th–4th) — página actual'
        }
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
                <Line type="monotone" dataKey="pos_1th" name="1th" stroke="#dc2626" dot={false} />
                <Line type="monotone" dataKey="pos_2th" name="2th" stroke="#f59e0b" dot={false} />
                <Line type="monotone" dataKey="pos_3th" name="3th" stroke="#2563eb" dot={false} />
                <Line type="monotone" dataKey="pos_4th" name="4th" stroke="#16a34a" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </Drawer>
    </section>
  );
}
