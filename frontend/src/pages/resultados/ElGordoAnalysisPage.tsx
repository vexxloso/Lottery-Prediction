import { useEffect, useMemo, useState } from 'react';
import { Drawer, Pagination } from 'antd';
import { Link } from 'react-router-dom';
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
import { AnalysisDifferenceCell } from '../../components/AnalysisDifferenceCell';
import { differenceFromRow, formatPosition } from '../../utils/compareAnalysis';

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
  special_position?: number | null;
  jackpot_position: number;
  prev_special_position?: number | null;
  difference_prev_special?: number | null;
  pos_1th: number;
  pos_2th: number | null;
  pos_3th: number | null;
  pos_4th: number | null;
  pos_5th?: number | null;
  pos_6th?: number | null;
  pos_7th?: number | null;
  pos_8th?: number | null;
  categories: CategoryRow[];
};

export function ElGordoAnalysisPage() {
  const [rows, setRows] = useState<AnalysisRowElGordo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showGraph, setShowGraph] = useState(false);
  const [page, setPage] = useState(1);
  const pageSize = 100;
  const [total, setTotal] = useState(0);
  const [graphRows, setGraphRows] = useState<AnalysisRowElGordo[] | null>(null);
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
          `${API_URL}/api/el-gordo/compare/analysis?skip=${skip}&limit=${pageSize}`,
          {
            cache: 'no-store',
          },
        );
        const data = await res.json();
        if (!res.ok || data.detail) {
          throw new Error(
            typeof data.detail === 'string'
              ? data.detail
              : 'Error al cargar análisis full wheel El Gordo',
          );
        }
        if (cancelled) return;
        setRows(Array.isArray(data.rows) ? (data.rows as AnalysisRowElGordo[]) : []);
        setTotal(typeof data.total === 'number' ? data.total : 0);
      } catch (e) {
        if (cancelled) return;
        setError(
          e instanceof Error ? e.message : 'Error al cargar análisis full wheel El Gordo',
        );
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [page]);

  const activeRowsForGraph =
    graphMode === 'range2004' && graphRows && graphRows.length > 0 ? graphRows : rows;

  const chartData = useMemo(
    () =>
      activeRowsForGraph
        .slice()
        .sort((a, b) => {
          const da = a.date || '';
          const db = b.date || '';
          if (da < db) return -1;
          if (da > db) return 1;
          return 0;
        })
        .map((r) => ({
          label: r.date || r.current_id,
          special_position: r.special_position ?? r.jackpot_position ?? null,
          pos_1th: r.pos_1th || null,
          difference_prev_special: differenceFromRow(r),
          pos_2th: r.pos_2th,
          pos_3th: r.pos_3th,
          pos_4th: r.pos_4th,
        })),
    [activeRowsForGraph],
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
        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
          <Link
            to="/validation"
            style={{ fontSize: '0.8rem', color: '#c41230', fontWeight: 600, textDecoration: 'none', padding: '4px 10px', border: '1px solid #c41230', borderRadius: 6 }}
            title="Ver dashboard de validación completo"
          >
            🔬 Validación
          </Link>
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
                    `${API_URL}/api/el-gordo/compare/analysis-graph?max_points=100`,
                    {
                      cache: 'no-store',
                    },
                  );
                  const data = await res.json();
                  if (!res.ok || data.detail) {
                    throw new Error(
                      typeof data.detail === 'string'
                        ? data.detail
                        : 'Error al cargar gráfico (2004–hoy)',
                    );
                  }
                  setGraphRows(
                    Array.isArray(data.rows) ? (data.rows as AnalysisRowElGordo[]) : [],
                  );
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
            title="Ver gráfico desde 1999 hasta hoy (máx. 100 sorteos)"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '0.35rem 0.5rem',
              fontSize: '0.8rem',
              minWidth: 'auto',
            }}
          >
            {graphLoading ? 'Cargando 1999–hoy…' : 'Gráfico 1999–hoy (100)'}
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
                <th>1ª (5+1)</th>
                <th>2ª (5+0)</th>
                <th>Diff vs prev draw (Special)</th>
                <th>3ª (4+1)</th>
                <th>4ª (4+0)</th>
                <th>5ª (3+1)</th>
                <th>6ª (3+0)</th>
                <th>7ª (2+1)</th>
                <th>8ª (2+0)</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                  <tr key={`${r.date}-${r.current_id}-${r.pre_id}`}>
                    <td>{r.date || '—'}</td>
                    <td>{formatPosition(r.special_position ?? r.jackpot_position)}</td>
                    <td>{formatPosition(r.pos_1th)}</td>
                    <AnalysisDifferenceCell row={r} />
                    <td>{formatPosition(r.pos_2th)}</td>
                    <td>{formatPosition(r.pos_3th)}</td>
                    <td>{formatPosition(r.pos_4th)}</td>
                    <td>{formatPosition(r.pos_5th)}</td>
                    <td>{formatPosition(r.pos_6th)}</td>
                    <td>{formatPosition(r.pos_7th)}</td>
                    <td>{formatPosition(r.pos_8th)}</td>
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
            ? 'Gráfico de posiciones (1ª–4ª) — 2004–hoy (máx. 100 sorteos)'
            : 'Gráfico de posiciones (1ª–4ª) — página actual'
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
                <Line type="monotone" dataKey="special_position" name="Special (5+1)" stroke="#7c3aed" dot={false} />
                <Line type="monotone" dataKey="pos_1th" name="1ª (5+0)" stroke="#dc2626" dot={false} />
                <Line type="monotone" dataKey="difference_prev_special" name="Special diff vs prev draw" stroke="#9333ea" strokeDasharray="4 4" dot={false} />
                <Line type="monotone" dataKey="pos_2th" name="2ª (4+1)" stroke="#f59e0b" dot={false} />
                <Line type="monotone" dataKey="pos_3th" name="3ª (4+0)" stroke="#2563eb" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </Drawer>
    </section>
  );
}

