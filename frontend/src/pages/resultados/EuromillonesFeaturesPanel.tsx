import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Drawer } from 'antd';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ScatterChart,
  Scatter,
} from 'recharts';
import type { EuromillonesFeatureRow } from './useEuromillonesFeatures';
import { useEuromillonesFeatures } from './useEuromillonesFeatures';

const WEEKDAY_ABBR = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];

function formatDateWithWeekday(date: string, _weekday?: string) {
  if (!date) return '—';
  try {
    const [y, m, d] = date.split('-').map((v) => Number(v));
    const jsDate = new Date(y, m - 1, d);
    const weekdayName = WEEKDAY_ABBR[jsDate.getDay()] ?? '';
    const formatted = jsDate.toLocaleDateString('es-ES', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    });
    return `${weekdayName} - ${formatted}`;
  } catch {
    return date;
  }
}

function NumbersPillList({ values }: { values: number[] }) {
  if (!values || values.length === 0) return <span>—</span>;
  return <span>{values.join(' ')}</span>;
}

function EuromillonesFeaturesTableRow({
  row,
  onShowChart,
  onShowGapChart,
  onSimulate,
}: {
  row: EuromillonesFeatureRow;
  onShowChart: (row: EuromillonesFeatureRow) => void;
  onShowGapChart: (row: EuromillonesFeatureRow) => void;
  onSimulate: (row: EuromillonesFeatureRow) => void;
}) {
  return (
    <tr>
      <td>{formatDateWithWeekday(row.draw_date, row.weekday)}</td>
      <td>
        <NumbersPillList values={row.main_numbers} />
        {row.star_numbers && row.star_numbers.length > 0 && (
          <>
            {' '}
            (
            <NumbersPillList values={row.star_numbers} />
            )
          </>
        )}
      </td>
      <td>
        <NumbersPillList values={row.hot_main_numbers ?? []} />
      </td>
      <td>
        <NumbersPillList values={row.cold_main_numbers ?? []} />
      </td>
      <td>
        <NumbersPillList values={row.hot_star_numbers ?? []} />
      </td>
      <td>
        <NumbersPillList values={row.cold_star_numbers ?? []} />
      </td>
      <td>
        <button
          type="button"
          className="resultados-features-iconbtn"
          onClick={() => onShowChart(row)}
          aria-label="Ver gráfico de frecuencias"
          title="Ver gráfico de frecuencias"
        >
          <img src="/images/frequency.svg" alt="" className="resultados-features-icon" />
        </button>
        <button
          type="button"
          className="resultados-features-iconbtn"
          style={{ marginLeft: 8 }}
          onClick={() => onShowGapChart(row)}
          aria-label="Ver gráfico de gaps"
          title="Ver gráfico de gaps"
        >
          <img src="/images/gape.svg" alt="" className="resultados-features-icon" />
        </button>
        <button
          type="button"
          className="resultados-features-iconbtn"
          style={{ marginLeft: 8 }}
          onClick={() => onSimulate(row)}
          aria-label="Simular con esta predicción"
          title="Simular con esta predicción"
        >
          <img src="/images/start.svg" alt="" className="resultados-features-icon" />
        </button>
      </td>
    </tr>
  );
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

  const navigate = useNavigate();

  const [selectedRow, setSelectedRow] = useState<EuromillonesFeatureRow | null>(null);
  const [modalType, setModalType] = useState<'none' | 'freq' | 'gap'>('none');
  const [gapPointsMain, setGapPointsMain] = useState<{ number: number; ts: number; date: string }[] | null>(null);
  const [gapPointsStar, setGapPointsStar] = useState<{ number: number; ts: number; date: string }[] | null>(null);
  const [gapError, setGapError] = useState('');
  const [gapLoading, setGapLoading] = useState(false);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [historyMain, setHistoryMain] = useState<{ number: number; dates: string[] }[] | null>(null);
  const [historyStar, setHistoryStar] = useState<{ number: number; dates: string[] }[] | null>(null);

  const closeModal = () => {
    setSelectedRow(null);
    setModalType('none');
  };

  const mainBars = (() => {
    if (!selectedRow?.main_frequency_counts) return [];
    return selectedRow.main_frequency_counts.map((count, idx) => ({
      number: idx + 1,
      count: count ?? 0,
    }));
  })();

  const starBars = (() => {
    if (!selectedRow?.star_frequency_counts) return [];
    return selectedRow.star_frequency_counts.map((count, idx) => ({
      number: idx + 1,
      count: count ?? 0,
    }));
  })();

  const ensureHistoryLoaded = async () => {
    if (historyLoaded && historyMain && historyStar) {
      return { main: historyMain, star: historyStar };
    }
    const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';
    const res = await fetch(`${API_URL}/api/euromillones/number-history`);
    const json = await res.json();
    if (!res.ok) {
      throw new Error(json.detail ?? res.statusText);
    }
    const main = (json.main ?? []) as { number: number; dates: string[] }[];
    const star = (json.star ?? []) as { number: number; dates: string[] }[];
    setHistoryMain(main);
    setHistoryStar(star);
    setHistoryLoaded(true);
    return { main, star };
  };

  const loadGapsForDate = async (endDateStr: string) => {
    setGapLoading(true);
    setGapError('');
    try {
      const loaded = await ensureHistoryLoaded();
      const mainHistory = loaded.main;
      const starHistory = loaded.star;

      const endMs = Date.parse(endDateStr);
      if (Number.isNaN(endMs)) {
        throw new Error('Fecha final no válida');
      }
      // 31 days window backwards from selected draw date (1 month)
      const windowMs = 31 * 24 * 60 * 60 * 1000;
      const startMs = endMs - windowMs;

      const buildPoints = (history: { number: number; dates: string[] }[]) => {
        const pts: { number: number; ts: number; date: string }[] = [];
        for (const entry of history) {
          for (const d of entry.dates) {
            const [y, m, day] = d.split('-').map((v) => Number(v));
            if (!y || !m || !day) continue;
            // Use UTC to avoid timezone shifts between stored date and axis labels
            const ms = Date.UTC(y, m - 1, day);
            if (Number.isNaN(ms)) continue;
            if (ms >= startMs && ms <= endMs) {
              pts.push({ number: entry.number, ts: ms, date: d });
            }
          }
        }
        return pts;
      };

      setGapPointsMain(buildPoints(mainHistory));
      setGapPointsStar(buildPoints(starHistory));
    } catch (e) {
      setGapError(e instanceof Error ? e.message : 'Error al cargar historial de gaps');
      setGapPointsMain(null);
      setGapPointsStar(null);
    } finally {
      setGapLoading(false);
    }
  };

  const filteredGapPoints = (points: { number: number; ts: number; date: string }[] | null) => {
    return points ?? [];
  };

  // Load full number history once when component mounts
  useEffect(() => {
    void ensureHistoryLoaded();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <section className="card resultados-features-card">
      <h2 style={{ marginTop: 0, marginBottom: 'var(--space-md)', fontSize: '1rem' }}>
        Euromillones prediction features
      </h2>

      {error && (
        <p style={{ color: 'var(--color-error)', marginTop: 0 }}>{error}</p>
      )}

      {loading && rows.length === 0 && (
        <p style={{ marginTop: 0 }}>Cargando datos de predicción…</p>
      )}

      {!loading && rows.length === 0 && !error && (
        <p style={{ marginTop: 0 }}>No hay datos de predicción para Euromillones.</p>
      )}

      {rows.length > 0 && (
        <div className="resultados-features-table-wrap">
          <table className="resultados-features-table">
            <thead>
              <tr>
                <th>Fecha</th>
                <th>Resultado (mains + estrellas)</th>
                <th>Hot mains</th>
                <th>Cold mains</th>
                <th>Hot stars</th>
                <th>Cold stars</th>
                <th>Todo</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <EuromillonesFeaturesTableRow
                  key={row.draw_id}
                  row={row}
                  onShowChart={(r) => {
                    setSelectedRow(r);
                    setModalType('freq');
                  }}
                  onShowGapChart={(r) => {
                    setSelectedRow(r);
                    setModalType('gap');
                    setGapPointsMain(null);
                    setGapPointsStar(null);
                    setGapError('');
                    const endDateStr = String(r.draw_date ?? '').split(' ')[0];
                    void loadGapsForDate(endDateStr);
                  }}
                  onSimulate={(r) => {
                    const date = String(r.draw_date ?? '').split(' ')[0];
                    navigate(
                      `/simulacion/euromillones/${encodeURIComponent(r.draw_id)}?date=${encodeURIComponent(
                        date,
                      )}`,
                    );
                  }}
                />
              ))}
            </tbody>
          </table>

          <div className="resultados-features-pagination">
            <button type="button" disabled={currentPage <= 1} onClick={prevPage}>
              Anterior
            </button>
            <span>
              Página {currentPage} de {totalPages} ({total} sorteos)
            </span>
            <button
              type="button"
              disabled={currentPage >= totalPages}
              onClick={nextPage}
            >
              Siguiente
            </button>
          </div>
        </div>
      )}

      <Drawer
        title={
          selectedRow
            ? `Frecuencia Euromillones — ${formatDateWithWeekday(
                selectedRow.draw_date,
                selectedRow.weekday,
              )}`
            : ''
        }
        placement="right"
        width="100%"
        open={modalType === 'freq' && !!selectedRow}
        onClose={closeModal}
        bodyStyle={{ padding: 24 }}
      >
        {selectedRow && (
          <>
            <div className="resultados-features-hotcold">
              <div>
                <div className="resultados-features-hotcold-title">
                  <span className="resultados-features-hot-icon">🏆</span>
                  Hot mains
                </div>
                <div className="resultados-features-hotcold-values">
                  {selectedRow.hot_main_numbers?.length
                    ? selectedRow.hot_main_numbers.join(' ')
                    : '—'}
                </div>
              </div>
              <div>
                <div className="resultados-features-hotcold-title">
                  Cold mains
                </div>
                <div className="resultados-features-hotcold-values">
                  {selectedRow.cold_main_numbers?.length
                    ? selectedRow.cold_main_numbers.join(' ')
                    : '—'}
                </div>
              </div>
              <div>
                <div className="resultados-features-hotcold-title">
                  <span className="resultados-features-hot-icon">🏆</span>
                  Hot stars
                </div>
                <div className="resultados-features-hotcold-values">
                  {selectedRow.hot_star_numbers?.length
                    ? selectedRow.hot_star_numbers.join(' ')
                    : '—'}
                </div>
              </div>
              <div>
                <div className="resultados-features-hotcold-title">
                  Cold stars
                </div>
                <div className="resultados-features-hotcold-values">
                  {selectedRow.cold_star_numbers?.length
                    ? selectedRow.cold_star_numbers.join(' ')
                    : '—'}
                </div>
              </div>
            </div>

            <div className="resultados-features-fullcharts">
              <section>
                <h4 className="resultados-features-chart-title">Números principales (1–50)</h4>
                <div style={{ width: '100%', height: 520 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={mainBars}
                      margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                    >
                      <XAxis dataKey="number" tick={{ fontSize: 10 }} />
                      <YAxis allowDecimals={false} />
                      <Tooltip
                        formatter={(value: number) => [value, 'Frecuencia']}
                        labelFormatter={(label: string | number) => `Número ${label}`}
                      />
                      <Bar dataKey="count" fill="#3b82f6" barSize={6} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </section>

              <section>
                <h4 className="resultados-features-chart-title">Estrellas (1–12)</h4>
                <div style={{ width: '100%', height: 400 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={starBars}
                      margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                    >
                      <XAxis dataKey="number" tick={{ fontSize: 10 }} />
                      <YAxis allowDecimals={false} />
                      <Tooltip
                        formatter={(value: number) => [value, 'Frecuencia']}
                        labelFormatter={(label: string | number) => `Estrella ${label}`}
                      />
                      <Bar dataKey="count" fill="#eab308" barSize={10} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </section>
            </div>
          </>
        )}
      </Drawer>

      <Drawer
        title="Gap Euromillones — historial de apariciones"
        placement="right"
        width="100%"
        open={modalType === 'gap'}
        onClose={closeModal}
        bodyStyle={{ padding: 24 }}
      >
        {gapError && (
          <p style={{ color: 'var(--color-error)', marginTop: 0 }}>{gapError}</p>
        )}
        {gapLoading && (
          <p style={{ marginTop: 0 }}>Cargando historial de apariciones…</p>
        )}

        {gapPointsMain && gapPointsStar && (
          <div className="resultados-features-fullcharts">
            <section>
              <h4 className="resultados-features-chart-title">Números principales (1–50)</h4>
              <div style={{ width: '100%', height: 460, marginBottom: 'var(--space-md)' }}>
                <ResponsiveContainer width="100%" height="100%">
                  <ScatterChart
                    margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                  >
                    <XAxis
                      dataKey="number"
                      type="number"
                      name="Número"
                      domain={[1, 50]}
                      tick={{ fontSize: 10 }}
                    />
                    <YAxis
                      dataKey="ts"
                      type="number"
                      domain={['dataMin', 'dataMax']}
                      tickFormatter={(v: number) => {
                        const iso = new Date(v).toISOString().slice(0, 10); // YYYY-MM-DD
                        const [yy, mm, dd] = iso.split('-');
                        return `${dd}/${mm}/${yy.slice(2)}`;
                      }}
                    />
                    <Tooltip
                      formatter={(_value: any, _name: any, props: any) => {
                        const p = props?.payload as { number: number; date: string };
                        return [`${p.date}`, `Número ${p.number}`];
                      }}
                    />
                    <Scatter data={filteredGapPoints(gapPointsMain)} fill="#3b82f6" />
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            </section>

            <section>
              <h4 className="resultados-features-chart-title">Estrellas (1–12)</h4>
              <div style={{ width: '100%', height: 380 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <ScatterChart
                    margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                  >
                    <XAxis
                      dataKey="number"
                      type="number"
                      name="Estrella"
                      domain={[1, 12]}
                      tick={{ fontSize: 10 }}
                    />
                    <YAxis
                      dataKey="ts"
                      type="number"
                      domain={['dataMin', 'dataMax']}
                      tickFormatter={(v: number) => {
                        const iso = new Date(v).toISOString().slice(0, 10);
                        const [yy, mm, dd] = iso.split('-');
                        return `${dd}/${mm}/${yy.slice(2)}`;
                      }}
                    />
                    <Tooltip
                      formatter={(_value: any, _name: any, props: any) => {
                        const p = props?.payload as { number: number; date: string };
                        return [`${p.date}`, `Estrella ${p.number}`];
                      }}
                    />
                    <Scatter data={filteredGapPoints(gapPointsStar)} fill="#eab308" />
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            </section>
          </div>
        )}
      </Drawer>
    </section>
  );
}

