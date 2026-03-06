import { useState, useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Drawer, Row, Col } from 'antd';
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
import type { EuromillonesFeatureRow } from './useEuromillonesFeatures';
import { useEuromillonesFeatures, useEuromillonesLast10 } from './useEuromillonesFeatures';

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

function formatResultado(mains?: number[] | null, stars?: number[] | null): string {
  const m = (mains ?? []).join(' ');
  const sList = stars ?? [];
  if (!m && !sList.length) return '—';
  const s = sList.join(' ');
  return s ? `${m} (${s})` : m;
}

/** Heatmap: X = number, Y = draw (date). Last 10 draws from euromillones_feature. */
function Last10Heatmap({
  draws,
  type,
  numberType,
  title,
}: {
  draws: EuromillonesFeatureRow[];
  type: 'gap' | 'frequency';
  numberType: 'main' | 'star';
  title: string;
}) {
  const { offset, count } = numberType === 'main' ? { offset: 0, count: 50 } : { offset: 50, count: 12 };
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

  const rows = draws.length;
  if (!rows) return null;

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
                  {i + 1}
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
                  const arr = type === 'gap' ? (d.gap ?? []) : (d.frequency ?? []);
                  const val = typeof arr[offset + colIdx] === 'number' ? Number(arr[offset + colIdx]) : 0;
                  return (
                    <td
                      key={colIdx}
                      className="resultados-features-heatmap-cell"
                      style={getCellStyle(val)}
                      title={`Nº ${colIdx + 1}: ${type === 'gap' ? 'gap' : 'freq'} = ${val}`}
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
  draws: EuromillonesFeatureRow[];
  numberType: 'main' | 'star';
  title: string;
}) {
  const count = numberType === 'main' ? 50 : 12;
  const domainX: [number, number] = [1, count];

  const points = useMemo(() => {
    const out: { number: number; dateIndex: number; date: string }[] = [];
    draws.forEach((d, dateIndex) => {
      const arr = numberType === 'main' ? (d.main_number ?? []) : (d.star_number ?? []);
      const date = d.fecha_sorteo ?? '';
      arr.forEach((n) => {
        if (numberType === 'main' ? n >= 1 && n <= 50 : n >= 1 && n <= 12) {
          out.push({ number: n, dateIndex, date });
        }
      });
    });
    return out;
  }, [draws, numberType]);

  const yTicks = useMemo(() => draws.map((_, i) => i), [draws.length]);
  const yTickFormatter = (dateIndex: number) => draws[dateIndex]?.fecha_sorteo ?? '';

  if (!draws.length) return null;

  return (
    <section className={`resultados-features-dotgraph resultados-features-dotgraph--${numberType}`}>
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
              tickCount={numberType === 'main' ? 10 : 6}
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
              fill={numberType === 'main' ? '#1976d2' : '#ffc107'}
              shape="circle"
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

function computeHotCold(row: EuromillonesFeatureRow) {
  const freq = row.frequency ?? [];
  const mainFreq = Array.from({ length: 50 }, (_, i) => ({
    num: i + 1,
    freq: Number(freq[i] ?? 0),
  }));
  const starFreq = Array.from({ length: 12 }, (_, i) => ({
    num: i + 1,
    freq: Number(freq[50 + i] ?? 0),
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

  const hotStars = starFreq
    .filter((x) => x.freq > 0)
    .sort((a, b) => b.freq - a.freq)
    .slice(0, 5)
    .map((x) => x.num);

  const coldStars = [...starFreq]
    .sort((a, b) => a.freq - b.freq)
    .slice(0, 5)
    .map((x) => x.num);

  return {
    hotMains,
    coldMains,
    hotStars,
    coldStars,
  };
}

export function EuromillonesFeaturesPanel() {
  const {
    rows,
    loading,
    error,
    currentPage,
    totalPages,
    total,
    nextPage,
    prevPage,
  } = useEuromillonesFeatures();
  const { rows: last10Rows, loading: last10Loading } = useEuromillonesLast10();

  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();
  const [selectedRow, setSelectedRow] = useState<EuromillonesFeatureRow | null>(null);
  const [chartMode, setChartMode] = useState<'frequency' | 'gap'>('frequency');

  const openDrawer = (row: EuromillonesFeatureRow, mode: 'frequency' | 'gap') => {
    setSelectedRow(row);
    setChartMode(mode);
  };

  const goToPredictionBacktest = (row: EuromillonesFeatureRow) => {
    const params = new URLSearchParams(searchParams);
    params.set('tab', 'prediction');
    if (row.id_sorteo) {
      params.set('cutoff_draw_id', row.id_sorteo);
    } else {
      params.delete('cutoff_draw_id');
    }
    setSearchParams(params, { replace: true });
  };

  const goToCompare = (row: EuromillonesFeatureRow) => {
    const currentId = row.id_sorteo;
    const prevId = row.pre_id_sorteo ?? undefined;
    if (!currentId || !prevId) return;
    const date = (row.fecha_sorteo ?? '').split(' ')[0];
    const params = new URLSearchParams();
    params.set('view', 'compare');
    params.set('prev_id', prevId);
    if (date) params.set('date', date);
    navigate(`/simulacion/euromillones/${encodeURIComponent(currentId)}?${params.toString()}`);
  };

  return (
    <section className="card resultados-features-card resultados-theme-euromillones">
      {error && <p style={{ color: 'var(--color-error)', marginTop: 'var(--space-sm)' }}>{error}</p>}
      {loading && <p style={{ marginTop: 'var(--space-sm)' }}>Cargando features…</p>}
      {!loading && !error && rows.length === 0 && (
        <p style={{ marginTop: 'var(--space-sm)' }}>No hay datos de predicción para Euromillones.</p>
      )}

      {!loading && rows.length > 0 && (
        <>
          <div className="resultados-features-table-wrap" style={{ marginTop: 'var(--space-sm)' }}>
            <table className="resultados-features-table">
              <thead>
                <tr>
                  <th>Fecha</th>
                  <th>Resultado</th>
                  <th>Hot mains</th>
                  <th>Cold mains</th>
                  <th>Hot stars</th>
                  <th>Cold stars</th>
                  <th>Todo</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row: EuromillonesFeatureRow) => {
                  const { hotMains, coldMains, hotStars, coldStars } = computeHotCold(row);
                  return (
                    <tr key={row.id_sorteo}>
                      <td>{formatFecha(row.fecha_sorteo)}</td>
                      <td>{formatResultado(row.main_number, row.star_number)}</td>
                      <td>{hotMains.length ? hotMains.join(' ') : '—'}</td>
                      <td>{coldMains.length ? coldMains.join(' ') : '—'}</td>
                      <td>{hotStars.length ? hotStars.join(' ') : '—'}</td>
                      <td>{coldStars.length ? coldStars.join(' ') : '—'}</td>
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

          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              marginTop: 'var(--space-sm)',
              gap: 'var(--space-sm)',
            }}
          >
            <button
              type="button"
              className="resultados-features-iconbtn"
              onClick={prevPage}
              disabled={currentPage <= 1}
              aria-label="Página anterior"
              title="Página anterior"
            >
              ←
            </button>
            <span style={{ fontSize: '0.85rem' }}>
              Página {currentPage} / {totalPages} · {total} filas
            </span>
            <button
              type="button"
              className="resultados-features-iconbtn"
              onClick={nextPage}
              disabled={currentPage >= totalPages}
              aria-label="Página siguiente"
              title="Página siguiente"
            >
              →
            </button>
          </div>
        </>
      )}

      <Drawer
        title={
          selectedRow
            ? `${chartMode === 'frequency' ? 'Frecuencia' : 'Gaps'} Euromillones — ${formatFecha(selectedRow.fecha_sorteo)}`
            : 'Euromillones'
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
            {/* Top summary: hot/cold mains and stars for this draw */}
            <div className="resultados-features-hotcold">
              {(() => {
                const { hotMains, coldMains, hotStars, coldStars } = computeHotCold(selectedRow);
                return (
                  <>
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
                        Hot stars
                      </div>
                      <div className="resultados-features-hotcold-values">
                        {hotStars.length ? hotStars.join(' ') : '—'}
                      </div>
                    </div>
                    <div>
                      <div className="resultados-features-hotcold-title">Cold stars</div>
                      <div className="resultados-features-hotcold-values">
                        {coldStars.length ? coldStars.join(' ') : '—'}
                      </div>
                    </div>
                  </>
                );
              })()}
            </div>

            <div className="resultados-features-fullcharts">
              <Row gutter={[16, 16]}>
                <Col xs={24} md={18}>
                  <section>
                    <h4 className="resultados-features-chart-title">
                      Este sorteo — Números principales (1–50) {chartMode === 'frequency' ? 'Frecuencia' : 'Gap'}
                    </h4>
                    <div className="resultados-features-chart-container resultados-features-chart-main">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart
                          data={Array.from({ length: 50 }, (_, i) => ({
                            number: i + 1,
                            count: chartMode === 'frequency'
                              ? Number((selectedRow.frequency ?? [])[i] ?? 0)
                              : Number((selectedRow.gap ?? [])[i] ?? 0),
                          }))}
                          margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
                        >
                          <XAxis dataKey="number" />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="count" fill={chartMode === 'frequency' ? '#1677ff' : '#52c41a'} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </section>
                </Col>
                <Col xs={24} md={6}>
                  <section>
                    <h4 className="resultados-features-chart-title">
                      Este sorteo — Estrellas (1–12) {chartMode === 'frequency' ? 'Frecuencia' : 'Gap'}
                    </h4>
                    <div className="resultados-features-chart-container resultados-features-chart-stars">
                      <ResponsiveContainer width="100%" height="100%" minHeight={220}>
                        <BarChart
                          data={Array.from({ length: 12 }, (_, i) => ({
                            number: i + 1,
                            count: chartMode === 'frequency'
                              ? Number((selectedRow.frequency ?? [])[50 + i] ?? 0)
                              : Number((selectedRow.gap ?? [])[50 + i] ?? 0),
                          }))}
                          margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
                        >
                          <XAxis dataKey="number" />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="count" fill={chartMode === 'frequency' ? '#eab308' : '#fa8c16'} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </section>
                </Col>
              </Row>
            </div>

            {/* For GAP mode, show last-10 overview after the current-draw charts. For FREQUENCY, skip it. */}
            {chartMode === 'gap' && (
              <>
                {/* Last 10 draws: X = number, Y = draw (date) — from euromillones_feature */}
                {last10Loading ? (
                  <p className="resultados-features-loading">Cargando últimos 10 sorteos…</p>
                ) : last10Rows.length > 0 ? (
                  <div className="resultados-features-last10-section">
                    <h3 className="resultados-features-last10-heading">Últimos 10 sorteos (euromillones_feature)</h3>
                    <Row gutter={[16, 16]}>
                      {/* Presence (dot) last 10 (main + stars) */}
                      <Col xs={24} lg={14}>
                        <Last10DotGraph
                          draws={last10Rows}
                          numberType="main"
                          title="Presencia (dot) — Números principales (1–50)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10DotGraph
                          draws={last10Rows}
                          numberType="star"
                          title="Presencia (dot) — Estrellas (1–12)"
                        />
                      </Col>
                      {/* Gap last 10 (main + stars) */}
                      <Col xs={24} lg={14}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="gap"
                          numberType="main"
                          title="Gap — Números principales (1–50)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="gap"
                          numberType="star"
                          title="Gap — Estrellas (1–12)"
                        />
                      </Col>
                      {/* Frequency (dot) last 10 (main + stars) */}
                      <Col xs={24} lg={14}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="frequency"
                          numberType="main"
                          title="Frecuencia (dot) — Números principales (1–50)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="frequency"
                          numberType="star"
                          title="Frecuencia (dot) — Estrellas (1–12)"
                        />
                      </Col>
                    </Row>
                  </div>
                ) : (
                  <p className="resultados-features-loading">No hay datos de últimos 10 sorteos.</p>
                )}
              </>
            )}
          </div>
        )}
      </Drawer>
    </section>
  );
}
