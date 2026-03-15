import { useState, useMemo } from 'react';
import { Drawer, Pagination, Row, Col } from 'antd';
import { useNavigate, useSearchParams } from 'react-router-dom';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ScatterChart,
  Scatter,
  CartesianGrid,
} from 'recharts';
import type { ElGordoFeatureModelRow } from './useElGordoFeatureModel';
import { useElGordoFeatureModel, useElGordoLast10 } from './useElGordoFeatureModel';

const WEEKDAY_ES = ['domingo', 'lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado'];

function formatFecha(fecha?: string | null): string {
  if (!fecha) return '—';
  try {
    const [y, m, d] = fecha.split('-').map((v) => Number(v));
    if (!y || !m || !d) return fecha;
    const jsDate = new Date(y, m - 1, d);
    const weekday = WEEKDAY_ES[jsDate.getDay()] ?? '';
    const formatted = jsDate.toLocaleDateString('es-ES', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    });
    return `${weekday} - ${formatted}`;
  } catch {
    return fecha;
  }
}

function formatResultado(mains?: number[] | null, clave?: number | null): string {
  const m = (mains ?? []).join(' ');
  if (!m && clave == null) return '—';
  return clave != null ? `${m} (${clave})` : m;
}

function computeHotCold(row: ElGordoFeatureModelRow) {
  const freq = row.frequency ?? [];
  // mains 1–54 → indices 0..53
  const mainFreq = Array.from({ length: 54 }, (_, i) => ({
    num: i + 1,
    freq: Number(freq[i] ?? 0),
  }));
  // clave 0–9 → indices 54..63
  const claveFreq = Array.from({ length: 10 }, (_, i) => ({
    num: i,
    freq: Number(freq[54 + i] ?? 0),
  }));

  const hotMains = mainFreq
    .filter((x) => x.freq > 0)
    .sort((a, b) => b.freq - a.freq)
    .slice(0, 5)
    .map((x) => x.num);

  const coldMains = [...mainFreq]
    .sort((a, b) => a.freq - b.freq)
    .slice(0, 5)
    .map((x) => x.num);

  const hotClave = claveFreq
    .filter((x) => x.freq > 0)
    .sort((a, b) => b.freq - a.freq)
    .slice(0, 3)
    .map((x) => x.num);

  const coldClave = [...claveFreq]
    .sort((a, b) => a.freq - b.freq)
    .slice(0, 3)
    .map((x) => x.num);

  return {
    hotMains,
    coldMains,
    hotClave,
    coldClave,
  };
}

/** Heatmap: X = number, Y = draw (date). Last 10 draws from el_gordo_feature. */
function Last10Heatmap({
  draws,
  type,
  numberType,
  title,
}: {
  draws: ElGordoFeatureModelRow[];
  type: 'gap' | 'frequency';
  numberType: 'main' | 'clave';
  title: string;
}) {
  const { offset, count } =
    numberType === 'main' ? { offset: 0, count: 54 } : { offset: 54, count: 10 };
  const values = useMemo(() => {
    const v: number[] = [];
    draws.forEach((d) => {
      const arr = type === 'gap' ? (d.gap ?? []) : (d.frequency ?? []);
      for (let i = 0; i < count; i++) {
        const x = arr[offset + i];
        v.push(typeof x === 'number' ? x : 0);
      }
    });
    return v;
  }, [draws, type, offset, count]);
  const maxVal = useMemo(() => (values.length ? Math.max(...values) : 0), [values]);

  const getCellStyle = (val: number) => {
    if (type === 'gap') {
      if (val === 0 || Number.isNaN(val)) return { backgroundColor: 'var(--color-surface-hover)' };
      const t = maxVal <= 1 ? 1 : (val - 0) / (maxVal || 1);
      const r = Math.round(255 * Math.min(1, t));
      const g = Math.round(255 * (1 - Math.min(1, t)));
      return { backgroundColor: `rgb(${r},${g},120)` };
    }
    if (maxVal <= 0) return { backgroundColor: 'var(--color-surface-hover)' };
    const t = val / maxVal;
    const alpha = 0.2 + 0.8 * t;
    return { backgroundColor: `rgba(33, 119, 255, ${alpha})` };
  };

  if (!draws.length) return null;

  return (
    <section className="resultados-features-last10-heatmap">
      <h4 className="resultados-features-chart-title">{title}</h4>
      <div className="resultados-features-heatmap-wrap">
        <table className="resultados-features-heatmap-table">
          <thead>
            <tr>
              <th className="resultados-features-heatmap-y-label">Fecha</th>
              {Array.from({ length: count }, (_, i) => (
                <th key={i} className="resultados-features-heatmap-x-cell">
                  {numberType === 'main' ? i + 1 : i}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {draws.map((d, rowIdx) => (
              <tr key={d.id_sorteo ?? rowIdx}>
                <td className="resultados-features-heatmap-y-label">
                  {d.fecha_sorteo ?? '—'}
                </td>
                {Array.from({ length: count }, (_, colIdx) => {
                  const idx = offset + colIdx;
                  const arr = type === 'gap' ? (d.gap ?? []) : (d.frequency ?? []);
                  const val = typeof arr[idx] === 'number' ? Number(arr[idx]) : 0;
                  return (
                    <td
                      key={colIdx}
                      className="resultados-features-heatmap-cell"
                      style={getCellStyle(val)}
                      title={`Nº ${numberType === 'main' ? colIdx + 1 : colIdx}: ${
                        type === 'gap' ? 'gap' : 'freq'
                      } = ${val}`}
                    >
                      {val > 0 ? val : ''}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

/** Dot graph: real chart with X = number, Y = draw (date). Dot when number appeared in that draw. */
function Last10DotGraph({
  draws,
  numberType,
  title,
}: {
  draws: ElGordoFeatureModelRow[];
  numberType: 'main' | 'clave';
  title: string;
}) {
  const domainX: [number, number] =
    numberType === 'main' ? [1, 54] : [0, 9];

  const points = useMemo(() => {
    const out: { number: number; dateIndex: number; date: string }[] = [];
    draws.forEach((d, dateIndex) => {
      const date = d.fecha_sorteo ?? '';
      if (numberType === 'main') {
        const mains = d.main_number ?? [];
        mains.forEach((n) => {
          if (n >= 1 && n <= 54) {
            out.push({ number: n, dateIndex, date });
          }
        });
      } else {
        const clave = d.clave;
        if (typeof clave === 'number' && clave >= 0 && clave <= 9) {
          out.push({ number: clave, dateIndex, date });
        }
      }
    });
    return out;
  }, [draws, numberType]);

  const yTicks = useMemo(() => draws.map((_, i) => i), [draws.length]);
  const yTickFormatter = (dateIndex: number) => draws[dateIndex]?.fecha_sorteo ?? '';

  if (!draws.length) return null;

  return (
    <section className="resultados-features-dotgraph">
      <h4 className="resultados-features-chart-title">{title}</h4>
      <div className="resultados-features-dotgraph-chart-wrap">
        <ResponsiveContainer width="100%" height={280}>
          <ScatterChart margin={{ top: 10, right: 16, left: 8, bottom: 24 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis
              dataKey="number"
              type="number"
              name="Número"
              domain={domainX}
              tick={{ fontSize: 10 }}
              allowDecimals={false}
            />
            <YAxis
              dataKey="dateIndex"
              type="number"
              name="Fecha"
              domain={[0, Math.max(1, draws.length - 1)]}
              ticks={yTicks}
              tickFormatter={yTickFormatter}
              tick={{ fontSize: 10 }}
              width={72}
              reversed
            />
            <Tooltip
              cursor={{ stroke: 'var(--color-border)' }}
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null;
                const p = payload[0].payload as { number: number; date: string };
                return (
                  <div className="resultados-features-dotgraph-tooltip">
                    <span>Nº {p.number}</span>
                    <span>{p.date}</span>
                  </div>
                );
              }}
            />
            <Scatter
              data={points}
              fill={numberType === 'main' ? '#7c3aed' : '#a855f7'}
              shape="circle"
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

export function ElGordoFeatureModelPanel() {
  const {
    rows,
    loading,
    error,
    currentPage,
    totalPages,
    total,
    pageSize,
    setPage,
  } = useElGordoFeatureModel();

  const [selectedRow, setSelectedRow] = useState<ElGordoFeatureModelRow | null>(null);
  const [chartMode, setChartMode] = useState<'frequency' | 'gap'>('frequency');

  const { rows: last10Rows, loading: last10Loading } = useElGordoLast10();

  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  const openDrawer = (row: ElGordoFeatureModelRow, mode: 'frequency' | 'gap') => {
    setSelectedRow(row);
    setChartMode(mode);
  };

  const goToPredictionBacktest = (row: ElGordoFeatureModelRow) => {
    const params = new URLSearchParams(searchParams);
    params.set('tab', 'prediction');
    if (row.id_sorteo) {
      params.set('cutoff_draw_id', row.id_sorteo);
    } else {
      params.delete('cutoff_draw_id');
    }
    setSearchParams(params, { replace: true });
  };

  const goToCompare = (row: ElGordoFeatureModelRow) => {
    const currentId = row.id_sorteo;
    const prevId = row.pre_id_sorteo ?? undefined;
    if (!currentId || !prevId) return;
    const date = (row.fecha_sorteo ?? '').split(' ')[0];
    const params = new URLSearchParams();
    params.set('view', 'compare');
    params.set('prev_id', prevId);
    if (date) params.set('date', date);
    navigate(`/simulacion/el-gordo/${encodeURIComponent(currentId)}?${params.toString()}`);
  };

  return (
    <section className="card resultados-features-card resultados-theme-el-gordo">
      <h2 style={{ marginTop: 0, marginBottom: 'var(--space-md)', fontSize: '1rem' }}>
        El Gordo — predicción (nuevo modelo)
      </h2>

      {error && <p style={{ color: 'var(--color-error)', marginTop: 'var(--space-sm)' }}>{error}</p>}
      {loading && <p style={{ marginTop: 'var(--space-sm)' }}>Cargando features…</p>}
      {!loading && !error && rows.length === 0 && (
        <p style={{ marginTop: 'var(--space-sm)' }}>No hay datos de predicción para El Gordo.</p>
      )}

      {!loading && rows.length > 0 && (
        <>
          <div className="resultados-features-table-wrap" style={{ marginTop: 'var(--space-sm)' }}>
            <table className="resultados-features-table">
              <thead>
                <tr>
                  <th>Fecha</th>
                  <th>Resultado (5 + clave)</th>
                  <th>Hot mains</th>
                  <th>Cold mains</th>
                  <th>Hot clave</th>
                  <th>Cold clave</th>
                  <th>Todo</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const { hotMains, coldMains, hotClave, coldClave } = computeHotCold(row);
                  return (
                    <tr key={row.id_sorteo}>
                      <td>{formatFecha(row.fecha_sorteo)}</td>
                      <td>{formatResultado(row.main_number ?? [], row.clave ?? null)}</td>
                      <td>{hotMains.length ? hotMains.join(' ') : '—'}</td>
                      <td>{coldMains.length ? coldMains.join(' ') : '—'}</td>
                      <td>{hotClave.length ? hotClave.join(' ') : '—'}</td>
                      <td>{coldClave.length ? coldClave.join(' ') : '—'}</td>
                      <td>
                        <button
                          type="button"
                          className="resultados-features-iconbtn"
                          onClick={() => openDrawer(row, 'frequency')}
                          aria-label="Ver gráfico de frecuencias"
                          title="Ver gráfico de frecuencias"
                        >
                          <img src="/images/frequency.svg" alt="" className="resultados-features-icon" />
                        </button>
                        <button
                          type="button"
                          className="resultados-features-iconbtn"
                          style={{ marginLeft: 8 }}
                          onClick={() => openDrawer(row, 'gap')}
                          aria-label="Ver gráfico de gaps"
                          title="Ver gráfico de gaps"
                        >
                          <img src="/images/gape.svg" alt="" className="resultados-features-icon" />
                        </button>
                        <button
                          type="button"
                          className="resultados-features-iconbtn"
                          style={{ marginLeft: 8 }}
                          onClick={() => goToPredictionBacktest(row)}
                          aria-label="Usar este sorteo como corte de backtesting"
                          title="Usar este sorteo como corte de backtesting"
                        >
                          <img src="/images/start.svg" alt="" className="resultados-features-icon" />
                        </button>
                        <button
                          type="button"
                          className="resultados-features-iconbtn"
                          style={{ marginLeft: 8 }}
                          onClick={() => goToCompare(row)}
                          aria-label="Comparar sorteo actual con anterior"
                          title="Comparar sorteo actual con anterior"
                        >
                          <span className="resultados-features-icon resultados-features-compare-icon" aria-hidden>⇄</span>
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <div style={{ marginTop: 'var(--space-sm)' }}>
            <Pagination
              current={currentPage}
              total={total}
              pageSize={pageSize}
              showSizeChanger={false}
              onChange={(page) => setPage(page)}
              showTotal={(t) => `${t} filas`}
            />
          </div>
        </>
      )}

      <Drawer
        title={
          selectedRow
            ? `${chartMode === 'frequency' ? 'Frecuencia' : 'Gaps'} El Gordo — ${formatFecha(
                selectedRow.fecha_sorteo,
              )}`
            : 'El Gordo'
        }
        placement="right"
        width="100%"
        open={!!selectedRow}
        onClose={() => setSelectedRow(null)}
        bodyStyle={{ padding: 16 }}
        rootClassName="resultados-features-drawer resultados-features-drawer-fullscreen"
        styles={{ body: { display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 } }}
      >
        {selectedRow && (
          <div className="resultados-features-drawer-inner">
            {(() => {
              const { hotMains, coldMains, hotClave, coldClave } = computeHotCold(selectedRow);
              return (
                <div className="resultados-features-hotcold">
                  <div>
                    <div className="resultados-features-hotcold-title">
                      <span className="resultados-features-hot-icon">🏆</span>
                      Hot mains
                    </div>
                    <div className="resultados-features-hotcold-values">
                      {hotMains.length ? hotMains.join(' ') : '—'}
                    </div>
                  </div>
                  <div>
                    <div className="resultados-features-hotcold-title">Cold mains</div>
                    <div className="resultados-features-hotcold-values">
                      {coldMains.length ? coldMains.join(' ') : '—'}
                    </div>
                  </div>
                  <div>
                    <div className="resultados-features-hotcold-title">
                      <span className="resultados-features-hot-icon">🏆</span>
                      Hot clave
                    </div>
                    <div className="resultados-features-hotcold-values">
                      {hotClave.length ? hotClave.join(' ') : '—'}
                    </div>
                  </div>
                  <div>
                    <div className="resultados-features-hotcold-title">Cold clave</div>
                    <div className="resultados-features-hotcold-values">
                      {coldClave.length ? coldClave.join(' ') : '—'}
                    </div>
                  </div>
                </div>
              );
            })()}

            <div className="resultados-features-fullcharts">
              <Row gutter={[16, 16]}>
                <Col xs={24} md={18}>
                  <section>
                    <h4 className="resultados-features-chart-title">
                      Este sorteo — Números principales (1–54){' '}
                      {chartMode === 'frequency' ? 'Frecuencia' : 'Gap'}
                    </h4>
                    <div className="resultados-features-chart-container resultados-features-chart-main">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart
                          data={Array.from({ length: 54 }, (_, i) => ({
                            number: i + 1,
                            count:
                              chartMode === 'frequency'
                                ? Number((selectedRow.frequency ?? [])[i] ?? 0)
                                : Number((selectedRow.gap ?? [])[i] ?? 0),
                          }))}
                          margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
                        >
                          <XAxis dataKey="number" />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="count" fill={chartMode === 'frequency' ? '#7c3aed' : '#16a34a'} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </section>
                </Col>
                <Col xs={24} md={6}>
                  <section>
                    <h4 className="resultados-features-chart-title">
                      Este sorteo — Clave (0–9) {chartMode === 'frequency' ? 'Frecuencia' : 'Gap'}
                    </h4>
                    <div className="resultados-features-chart-container resultados-features-chart-stars">
                      <ResponsiveContainer width="100%" height="100%" minHeight={220}>
                        <BarChart
                          data={Array.from({ length: 10 }, (_, i) => ({
                            number: i,
                            count:
                              chartMode === 'frequency'
                                ? Number((selectedRow.frequency ?? [])[54 + i] ?? 0)
                                : Number((selectedRow.gap ?? [])[54 + i] ?? 0),
                          }))}
                          margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
                        >
                          <XAxis dataKey="number" />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="count" fill={chartMode === 'frequency' ? '#a855f7' : '#f97316'} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </section>
                </Col>
              </Row>
            </div>

            {/* For GAP mode, show last-10 overview after the current-draw charts. For FRECUENCIA, skip it. */}
            {chartMode === 'gap' && (
              <>
                {last10Loading ? (
                  <p className="resultados-features-loading">Cargando últimos 10 sorteos de El Gordo…</p>
                ) : last10Rows.length > 0 ? (
                  <div className="resultados-features-last10-section">
                    <h3 className="resultados-features-last10-heading">
                      Últimos 10 sorteos (el_gordo_feature)
                    </h3>
                    <Row gutter={[16, 16]}>
                      {/* Presence (dot) last 10 (main + clave) */}
                      <Col xs={24} lg={14}>
                        <Last10DotGraph
                          draws={last10Rows}
                          numberType="main"
                          title="Presencia (dot) — Números principales (1–54)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10DotGraph
                          draws={last10Rows}
                          numberType="clave"
                          title="Presencia (dot) — Clave (0–9)"
                        />
                      </Col>
                      {/* Gap last 10 (main + clave) */}
                      <Col xs={24} lg={14}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="gap"
                          numberType="main"
                          title="Gap — Números principales (1–54)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="gap"
                          numberType="clave"
                          title="Gap — Clave (0–9)"
                        />
                      </Col>
                      {/* Frequency last 10 (main + clave) */}
                      <Col xs={24} lg={14}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="frequency"
                          numberType="main"
                          title="Frecuencia (dot) — Números principales (1–54)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="frequency"
                          numberType="clave"
                          title="Frecuencia (dot) — Clave (0–9)"
                        />
                      </Col>
                    </Row>
                  </div>
                ) : (
                  <p className="resultados-features-loading">
                    No hay datos de últimos 10 sorteos de El Gordo.
                  </p>
                )}
              </>
            )}
          </div>
        )}
      </Drawer>
    </section>
  );
}

