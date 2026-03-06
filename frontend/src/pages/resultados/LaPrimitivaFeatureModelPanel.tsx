import { useState, useMemo } from 'react';
import { Drawer, Row, Col } from 'antd';
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
import type { LaPrimitivaFeatureModelRow } from './useLaPrimitivaFeatureModel';
import { useLaPrimitivaFeatureModel, useLaPrimitivaLast10 } from './useLaPrimitivaFeatureModel';

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

function formatResultado(mains?: number[] | null, c?: number | null, r?: number | null): string {
  const m = (mains ?? []).join(' ');
  const cPart = c != null ? `C ${c}` : 'C —';
  const rPart = r != null ? `R ${r}` : 'R —';
  if (!m && c == null && r == null) return '—';
  return `${m} (${cPart}, ${rPart})`;
}

function computeHotCold(row: LaPrimitivaFeatureModelRow) {
  const freq = row.frequency ?? [];
  // mains: indices 0..48 → 1–49
  const mainFreq = Array.from({ length: 49 }, (_, i) => ({
    num: i + 1,
    freq: Number(freq[i] ?? 0),
  }));
  // complementario: indices 49..97 → 1–49
  const compFreq = Array.from({ length: 49 }, (_, i) => ({
    num: i + 1,
    freq: Number(freq[49 + i] ?? 0),
  }));
  // reintegro: indices 98..107 → 0–9
  const reinFreq = Array.from({ length: 10 }, (_, i) => ({
    num: i,
    freq: Number(freq[98 + i] ?? 0),
  }));

  const hotMains = mainFreq
    .filter((x) => x.freq > 0)
    .sort((a, b) => b.freq - a.freq)
    .slice(0, 6)
    .map((x) => x.num);
  const coldMains = [...mainFreq]
    .sort((a, b) => a.freq - b.freq)
    .slice(0, 6)
    .map((x) => x.num);

  const hotComp = compFreq
    .filter((x) => x.freq > 0)
    .sort((a, b) => b.freq - a.freq)
    .slice(0, 3)
    .map((x) => x.num);
  const coldComp = [...compFreq]
    .sort((a, b) => a.freq - b.freq)
    .slice(0, 3)
    .map((x) => x.num);

  const hotRein = reinFreq
    .filter((x) => x.freq > 0)
    .sort((a, b) => b.freq - a.freq)
    .slice(0, 3)
    .map((x) => x.num);
  const coldRein = [...reinFreq]
    .sort((a, b) => a.freq - b.freq)
    .slice(0, 3)
    .map((x) => x.num);

  return {
    hotMains,
    coldMains,
    hotComp,
    coldComp,
    hotRein,
    coldRein,
  };
}

/** Dot graph: real chart with X = number, Y = draw (date). Dot when number appeared in that draw. */
function Last10DotGraph({
  draws,
  numberType,
  title,
}: {
  draws: LaPrimitivaFeatureModelRow[];
  numberType: 'main' | 'complementario' | 'reintegro';
  title: string;
}) {
  const domainX: [number, number] =
    numberType === 'main' || numberType === 'complementario' ? [1, 49] : [0, 9];

  const points = useMemo(() => {
    const out: { number: number; dateIndex: number; date: string }[] = [];
    draws.forEach((d, dateIndex) => {
      const date = d.fecha_sorteo ?? '';
      if (numberType === 'main') {
        const mains = d.main_number ?? [];
        mains.forEach((n) => {
          if (n >= 1 && n <= 49) {
            out.push({ number: n, dateIndex, date });
          }
        });
      } else if (numberType === 'complementario') {
        const c = d.complementario;
        if (typeof c === 'number' && c >= 1 && c <= 49) {
          out.push({ number: c, dateIndex, date });
        }
      } else {
        const r = d.reintegro;
        if (typeof r === 'number' && r >= 0 && r <= 9) {
          out.push({ number: r, dateIndex, date });
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
        <ResponsiveContainer width="100%" height={260}>
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
              fill={
                numberType === 'main'
                  ? '#00843d'
                  : numberType === 'complementario'
                  ? '#22c55e'
                  : '#0ea5e9'
              }
              shape="circle"
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

/** Heatmap: X = number, Y = draw (date). Last 10 draws from la_primitiva_feature. */
function Last10Heatmap({
  draws,
  type,
  numberType,
  title,
}: {
  draws: LaPrimitivaFeatureModelRow[];
  type: 'gap' | 'frequency';
  numberType: 'main' | 'complementario' | 'reintegro';
  title: string;
}) {
  const { offset, count } =
    numberType === 'main'
      ? { offset: 0, count: 49 }
      : numberType === 'complementario'
      ? { offset: 49, count: 49 }
      : { offset: 98, count: 10 };

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
                  {numberType === 'main' || numberType === 'complementario' ? i + 1 : i}
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
                      title={`Nº ${
                        numberType === 'main' || numberType === 'complementario'
                          ? colIdx + 1
                          : colIdx
                      }: ${type === 'gap' ? 'gap' : 'freq'} = ${val}`}
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

export function LaPrimitivaFeatureModelPanel() {
  const {
    rows,
    loading,
    error,
    currentPage,
    totalPages,
    total,
    nextPage,
    prevPage,
  } = useLaPrimitivaFeatureModel();

  const [selectedRow, setSelectedRow] = useState<LaPrimitivaFeatureModelRow | null>(null);
  const [chartMode, setChartMode] = useState<'frequency' | 'gap'>('frequency');

  const { rows: last10Rows, loading: last10Loading } = useLaPrimitivaLast10();

  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  const openDrawer = (row: LaPrimitivaFeatureModelRow, mode: 'frequency' | 'gap') => {
    setSelectedRow(row);
    setChartMode(mode);
  };

  const goToPredictionBacktest = (row: LaPrimitivaFeatureModelRow) => {
    const params = new URLSearchParams(searchParams);
    params.set('tab', 'prediction');
    if (row.id_sorteo) {
      params.set('cutoff_draw_id', row.id_sorteo);
    } else {
      params.delete('cutoff_draw_id');
    }
    setSearchParams(params, { replace: true });
  };

  const goToCompare = (row: LaPrimitivaFeatureModelRow) => {
    const currentId = row.id_sorteo;
    const prevId = row.pre_id_sorteo ?? undefined;
    if (!currentId || !prevId) return;
    const date = (row.fecha_sorteo ?? '').split(' ')[0];
    const params = new URLSearchParams();
    params.set('view', 'compare');
    params.set('prev_id', prevId);
    if (date) params.set('date', date);
    navigate(`/simulacion/la-primitiva/${encodeURIComponent(currentId)}?${params.toString()}`);
  };

  return (
    <section className="card resultados-features-card resultados-theme-la-primitiva">
      <h2 style={{ marginTop: 0, marginBottom: 'var(--space-md)', fontSize: '1rem' }}>
        La Primitiva — predicción (nuevo modelo)
      </h2>

      {error && <p style={{ color: 'var(--color-error)', marginTop: 'var(--space-sm)' }}>{error}</p>}
      {loading && <p style={{ marginTop: 'var(--space-sm)' }}>Cargando features…</p>}
      {!loading && !error && rows.length === 0 && (
        <p style={{ marginTop: 'var(--space-sm)' }}>No hay datos de predicción para La Primitiva.</p>
      )}

      {!loading && rows.length > 0 && (
        <>
          <div className="resultados-features-table-wrap" style={{ marginTop: 'var(--space-sm)' }}>
            <table className="resultados-features-table">
              <thead>
                <tr>
                  <th>Fecha</th>
                  <th>Resultado (6 + C + R)</th>
                  <th>Hot mains</th>
                  <th>Cold mains</th>
                  <th>Hot C</th>
                  <th>Cold C</th>
                  <th>Hot R</th>
                  <th>Cold R</th>
                  <th>Todo</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const { hotMains, coldMains, hotComp, coldComp, hotRein, coldRein } =
                    computeHotCold(row);
                  return (
                    <tr key={row.id_sorteo}>
                      <td>{formatFecha(row.fecha_sorteo)}</td>
                      <td>
                        {formatResultado(
                          row.main_number ?? [],
                          row.complementario ?? null,
                          row.reintegro ?? null,
                        )}
                      </td>
                      <td>{hotMains.length ? hotMains.join(' ') : '—'}</td>
                      <td>{coldMains.length ? coldMains.join(' ') : '—'}</td>
                      <td>{hotComp.length ? hotComp.join(' ') : '—'}</td>
                      <td>{coldComp.length ? coldComp.join(' ') : '—'}</td>
                      <td>{hotRein.length ? hotRein.join(' ') : '—'}</td>
                      <td>{coldRein.length ? coldRein.join(' ') : '—'}</td>
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
            ? `${chartMode === 'frequency' ? 'Frecuencia' : 'Gaps'} La Primitiva — ${formatFecha(
                selectedRow.fecha_sorteo,
              )}`
            : 'La Primitiva'
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
              const { hotMains, coldMains, hotComp, coldComp, hotRein, coldRein } =
                computeHotCold(selectedRow);
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
                      Hot C
                    </div>
                    <div className="resultados-features-hotcold-values">
                      {hotComp.length ? hotComp.join(' ') : '—'}
                    </div>
                  </div>
                  <div>
                    <div className="resultados-features-hotcold-title">Cold C</div>
                    <div className="resultados-features-hotcold-values">
                      {coldComp.length ? coldComp.join(' ') : '—'}
                    </div>
                  </div>
                  <div>
                    <div className="resultados-features-hotcold-title">
                      <span className="resultados-features-hot-icon">🏆</span>
                      Hot R
                    </div>
                    <div className="resultados-features-hotcold-values">
                      {hotRein.length ? hotRein.join(' ') : '—'}
                    </div>
                  </div>
                  <div>
                    <div className="resultados-features-hotcold-title">Cold R</div>
                    <div className="resultados-features-hotcold-values">
                      {coldRein.length ? coldRein.join(' ') : '—'}
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
                      Este sorteo — Números principales (1–49){' '}
                      {chartMode === 'frequency' ? 'Frecuencia' : 'Gap'}
                    </h4>
                    <div className="resultados-features-chart-container resultados-features-chart-main">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart
                          data={Array.from({ length: 49 }, (_, i) => ({
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
                          <Bar dataKey="count" fill={chartMode === 'frequency' ? '#00843d' : '#16a34a'} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </section>
                </Col>
                <Col xs={24} md={6}>
                  <section>
                    <h4 className="resultados-features-chart-title">
                      Este sorteo — C (1–49) {chartMode === 'frequency' ? 'Frecuencia' : 'Gap'}
                    </h4>
                    <div className="resultados-features-chart-container resultados-features-chart-stars">
                      <ResponsiveContainer width="100%" height="100%" minHeight={180}>
                        <BarChart
                          data={Array.from({ length: 49 }, (_, idx) => {
                            const number = idx + 1;
                            const base = 49;
                            const count =
                              chartMode === 'frequency'
                                ? Number((selectedRow.frequency ?? [])[base + idx] ?? 0)
                                : Number((selectedRow.gap ?? [])[base + idx] ?? 0);
                            return { number, count };
                          })}
                          margin={{ top: 10, right: 20, left: 10, bottom: 20 }}
                        >
                          <XAxis dataKey="number" tick={{ fontSize: 9 }} interval={4} />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="count" fill={chartMode === 'frequency' ? '#22c55e' : '#eab308'} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </section>
                  <section style={{ marginTop: 'var(--space-md)' }}>
                    <h4 className="resultados-features-chart-title">
                      Este sorteo — R (0–9) {chartMode === 'frequency' ? 'Frecuencia' : 'Gap'}
                    </h4>
                    <div className="resultados-features-chart-container resultados-features-chart-stars">
                      <ResponsiveContainer width="100%" height="100%" minHeight={140}>
                        <BarChart
                          data={Array.from({ length: 10 }, (_, idx) => {
                            const r = idx;
                            const base = 49 + 49;
                            const count =
                              chartMode === 'frequency'
                                ? Number((selectedRow.frequency ?? [])[base + idx] ?? 0)
                                : Number((selectedRow.gap ?? [])[base + idx] ?? 0);
                            return { reintegro: r, count };
                          })}
                          margin={{ top: 10, right: 20, left: 10, bottom: 20 }}
                        >
                          <XAxis dataKey="reintegro" tick={{ fontSize: 10 }} />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="count" fill={chartMode === 'frequency' ? '#0ea5e9' : '#f97316'} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </section>
                </Col>
              </Row>
            </div>

            {/* Últimos 10 sorteos: Presencia (dot) + Gap + Frecuencia (dot) for main, C, R — like Euromillones */}
            {chartMode === 'gap' && (
              <>
                {last10Loading ? (
                  <p className="resultados-features-loading">
                    Cargando últimos 10 sorteos de La Primitiva…
                  </p>
                ) : last10Rows.length > 0 ? (
                  <div className="resultados-features-last10-section">
                    <h3 className="resultados-features-last10-heading">
                      Últimos 10 sorteos (la_primitiva_feature)
                    </h3>
                    <Row gutter={[16, 16]}>
                      <Col xs={24} lg={14}>
                        <Last10DotGraph
                          draws={last10Rows}
                          numberType="main"
                          title="Presencia (dot) — Números principales (1–49)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10DotGraph
                          draws={last10Rows}
                          numberType="complementario"
                          title="Presencia (dot) — C (1–49)"
                        />
                        <Last10DotGraph
                          draws={last10Rows}
                          numberType="reintegro"
                          title="Presencia (dot) — R (0–9)"
                        />
                      </Col>
                    </Row>
                    <Row gutter={[16, 16]}>
                      <Col xs={24} lg={14}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="gap"
                          numberType="main"
                          title="Gap — Números principales (1–49)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="gap"
                          numberType="complementario"
                          title="Gap — C (1–49)"
                        />
                        <Last10Heatmap
                          draws={last10Rows}
                          type="gap"
                          numberType="reintegro"
                          title="Gap — R (0–9)"
                        />
                      </Col>
                    </Row>
                    <Row gutter={[16, 16]}>
                      <Col xs={24} lg={14}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="frequency"
                          numberType="main"
                          title="Frecuencia (dot) — Números principales (1–49)"
                        />
                      </Col>
                      <Col xs={24} lg={10}>
                        <Last10Heatmap
                          draws={last10Rows}
                          type="frequency"
                          numberType="complementario"
                          title="Frecuencia (dot) — C (1–49)"
                        />
                        <Last10Heatmap
                          draws={last10Rows}
                          type="frequency"
                          numberType="reintegro"
                          title="Frecuencia (dot) — R (0–9)"
                        />
                      </Col>
                    </Row>
                  </div>
                ) : (
                  <p className="resultados-features-loading">
                    No hay datos de últimos 10 sorteos de La Primitiva.
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

