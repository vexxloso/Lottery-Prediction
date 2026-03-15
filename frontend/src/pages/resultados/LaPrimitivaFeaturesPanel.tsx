import { useEffect, useState } from 'react';
import { Drawer, Pagination } from 'antd';
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
import type { LaPrimitivaFeatureRow } from './useLaPrimitivaFeatures';
import { useLaPrimitivaFeatures } from './useLaPrimitivaFeatures';

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

function LaPrimitivaFeaturesTableRow({
  row,
  onShowChart,
  onShowGapChart,
}: {
  row: LaPrimitivaFeatureRow;
  onShowChart: (row: LaPrimitivaFeatureRow) => void;
  onShowGapChart: (row: LaPrimitivaFeatureRow) => void;
}) {
  return (
    <tr>
      <td>{formatDateWithWeekday(row.draw_date, row.weekday)}</td>
      <td>
        <NumbersPillList values={row.main_numbers} />
        {(row.complementario != null || row.reintegro != null) && (
          <>
            {' '}
            (
            {row.complementario != null ? `C ${row.complementario}` : 'C —'}
            {', '}
            {row.reintegro != null ? `R ${row.reintegro}` : 'R —'}
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
        {row.hot_complementario && row.hot_complementario.length > 0
          ? row.hot_complementario.join(' ')
          : '—'}
      </td>
      <td>
        {row.cold_complementario && row.cold_complementario.length > 0
          ? row.cold_complementario.join(' ')
          : '—'}
      </td>
      <td>
        {row.hot_reintegro && row.hot_reintegro.length > 0
          ? row.hot_reintegro.join(' ')
          : '—'}
      </td>
      <td>
        {row.cold_reintegro && row.cold_reintegro.length > 0
          ? row.cold_reintegro.join(' ')
          : '—'}
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
      </td>
    </tr>
  );
}

export function LaPrimitivaFeaturesPanel() {
  const {
    rows,
    loading,
    error,
    currentPage,
    total,
    pageSize,
    setPage,
  } = useLaPrimitivaFeatures();

  const [selectedRow, setSelectedRow] = useState<LaPrimitivaFeatureRow | null>(null);
  const [modalType, setModalType] = useState<'none' | 'freq' | 'gap'>('none');
  const [gapPointsMain, setGapPointsMain] = useState<{ number: number; ts: number; date: string }[] | null>(null);
  const [gapPointsComplementario, setGapPointsComplementario] = useState<{ number: number; ts: number; date: string }[] | null>(null);
  const [gapPointsReintegro, setGapPointsReintegro] = useState<{ number: number; ts: number; date: string }[] | null>(null);
  const [gapError, setGapError] = useState('');
  const [gapLoading, setGapLoading] = useState(false);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [historyMain, setHistoryMain] = useState<{ number: number; dates: string[] }[] | null>(null);
  const [historyComplementario, setHistoryComplementario] = useState<{ number: number; dates: string[] }[] | null>(null);
  const [historyReintegro, setHistoryReintegro] = useState<{ number: number; dates: string[] }[] | null>(null);

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

  const compBars = (() => {
    if (!selectedRow?.complementario_frequency_counts) return [];
    return selectedRow.complementario_frequency_counts.map((count, idx) => ({
      number: idx + 1,
      count: count ?? 0,
    }));
  })();

  const reintegroBars = (() => {
    if (!selectedRow?.reintegro_frequency_counts) return [];
    return selectedRow.reintegro_frequency_counts.map((count, idx) => ({
      number: idx,
      count: count ?? 0,
    }));
  })();

  const ensureHistoryLoaded = async () => {
    if (historyLoaded && historyMain != null) {
      return {
        main: historyMain,
        complementario: historyComplementario ?? [],
        reintegro: historyReintegro ?? [],
      };
    }
    const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';
    const res = await fetch(`${API_URL}/api/la-primitiva/number-history`);
    const json = await res.json();
    if (!res.ok) {
      throw new Error(json.detail ?? res.statusText);
    }
    const main = (json.main ?? []) as { number: number; dates: string[] }[];
    const complementario = (json.complementario ?? []) as { number: number; dates: string[] }[];
    const reintegro = (json.reintegro ?? []) as { number: number; dates: string[] }[];
    setHistoryMain(main);
    setHistoryComplementario(complementario);
    setHistoryReintegro(reintegro);
    setHistoryLoaded(true);
    return { main, complementario, reintegro };
  };

  const buildGapPoints = (history: { number: number; dates: string[] }[], startMs: number, endMs: number) => {
    const pts: { number: number; ts: number; date: string }[] = [];
    for (const entry of history) {
      for (const d of entry.dates) {
        const [y, m, day] = d.split('-').map((v) => Number(v));
        if (!y || !m || !day) continue;
        const ms = Date.UTC(y, m - 1, day);
        if (Number.isNaN(ms)) continue;
        if (ms >= startMs && ms <= endMs) {
          pts.push({ number: entry.number, ts: ms, date: d });
        }
      }
    }
    return pts;
  };

  const loadGapsForDate = async (endDateStr: string) => {
    setGapLoading(true);
    setGapError('');
    try {
      const loaded = await ensureHistoryLoaded();

      const endMs = Date.parse(endDateStr);
      if (Number.isNaN(endMs)) {
        throw new Error('Fecha final no válida');
      }
      const windowMs = 31 * 24 * 60 * 60 * 1000;
      const startMs = endMs - windowMs;

      setGapPointsMain(buildGapPoints(loaded.main, startMs, endMs));
      setGapPointsComplementario(buildGapPoints(loaded.complementario, startMs, endMs));
      setGapPointsReintegro(buildGapPoints(loaded.reintegro, startMs, endMs));
    } catch (e) {
      setGapError(e instanceof Error ? e.message : 'Error al cargar historial de gaps');
      setGapPointsMain(null);
      setGapPointsComplementario(null);
      setGapPointsReintegro(null);
    } finally {
      setGapLoading(false);
    }
  };

  const filteredGapPoints = (points: { number: number; ts: number; date: string }[] | null) => {
    return points ?? [];
  };

  useEffect(() => {
    void ensureHistoryLoaded();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <section className="card resultados-features-card">
      <h2 style={{ marginTop: 0, marginBottom: 'var(--space-md)', fontSize: '1rem' }}>
        La Primitiva prediction features
      </h2>

      {error && (
        <p style={{ color: 'var(--color-error)', marginTop: 0 }}>{error}</p>
      )}

      {loading && rows.length === 0 && (
        <p style={{ marginTop: 0 }}>Cargando datos de predicción…</p>
      )}

      {!loading && rows.length === 0 && !error && (
        <p style={{ marginTop: 0 }}>No hay datos de predicción para La Primitiva.</p>
      )}

      {rows.length > 0 && (
        <div className="resultados-features-table-wrap">
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
              {rows.map((row) => (
                <LaPrimitivaFeaturesTableRow
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
                    setGapPointsComplementario(null);
                    setGapPointsReintegro(null);
                    setGapError('');
                    const endDateStr = String(r.draw_date ?? '').split(' ')[0];
                    void loadGapsForDate(endDateStr);
                  }}
                />
              ))}
            </tbody>
          </table>

          <div className="resultados-features-pagination">
            <Pagination
              current={currentPage}
              total={total}
              pageSize={pageSize}
              showSizeChanger={false}
              onChange={(page) => setPage(page)}
              showTotal={(t) => `${t} sorteos`}
            />
          </div>
        </div>
      )}

      <Drawer
        title={
          selectedRow
            ? `Frecuencia La Primitiva — ${formatDateWithWeekday(
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
                  Hot C
                </div>
                <div className="resultados-features-hotcold-values">
                  {selectedRow.hot_complementario?.length
                    ? selectedRow.hot_complementario.join(' ')
                    : '—'}
                </div>
              </div>
              <div>
                <div className="resultados-features-hotcold-title">
                  Cold C
                </div>
                <div className="resultados-features-hotcold-values">
                  {selectedRow.cold_complementario?.length
                    ? selectedRow.cold_complementario.join(' ')
                    : '—'}
                </div>
              </div>
              <div>
                <div className="resultados-features-hotcold-title">
                  <span className="resultados-features-hot-icon">🏆</span>
                  Hot R
                </div>
                <div className="resultados-features-hotcold-values">
                  {selectedRow.hot_reintegro?.length
                    ? selectedRow.hot_reintegro.join(' ')
                    : '—'}
                </div>
              </div>
              <div>
                <div className="resultados-features-hotcold-title">
                  Cold R
                </div>
                <div className="resultados-features-hotcold-values">
                  {selectedRow.cold_reintegro?.length
                    ? selectedRow.cold_reintegro.join(' ')
                    : '—'}
                </div>
              </div>
            </div>

            <div className="resultados-features-fullcharts">
              <section>
                <h4 className="resultados-features-chart-title">Números principales (1–49)</h4>
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
                      <Bar dataKey="count" fill="#16a34a" barSize={6} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </section>
              <section>
                <h4 className="resultados-features-chart-title">Complementario y Reintegro</h4>
                <div style={{ width: '100%', display: 'flex', flexDirection: 'column', gap: 'var(--space-md)' }}>
                  <div style={{ width: '100%', height: 220 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={compBars}
                        margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                      >
                        <XAxis dataKey="number" tick={{ fontSize: 10 }} />
                        <YAxis allowDecimals={false} />
                      <Tooltip
                        formatter={(value: number) => [value, 'Frecuencia']}
                        labelFormatter={(label: string | number) => `C ${label}`}
                      />
                        <Bar dataKey="count" fill="#0d9488" barSize={6} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div style={{ width: '100%', height: 220 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={reintegroBars}
                        margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                      >
                        <XAxis dataKey="number" tick={{ fontSize: 10 }} />
                        <YAxis allowDecimals={false} />
                      <Tooltip
                        formatter={(value: number) => [value, 'Frecuencia']}
                        labelFormatter={(label: string | number) => `R ${label}`}
                      />
                        <Bar dataKey="count" fill="#ca8a04" barSize={16} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </section>
            </div>
          </>
        )}
      </Drawer>

      <Drawer
        title="Gap La Primitiva — historial de apariciones"
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

        {gapPointsMain != null && gapPointsComplementario != null && gapPointsReintegro != null && (
          <div className="resultados-features-fullcharts">
            <section>
              <h4 className="resultados-features-chart-title">Números principales (1–49)</h4>
              <div style={{ width: '100%', height: 460, marginBottom: 'var(--space-md)' }}>
                <ResponsiveContainer width="100%" height="100%">
                  <ScatterChart
                    margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                  >
                    <XAxis
                      dataKey="number"
                      type="number"
                      name="Número"
                      domain={[1, 49]}
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
                        return [`${p.date}`, `Número ${p.number}`];
                      }}
                    />
                    <Scatter data={filteredGapPoints(gapPointsMain)} fill="#16a34a" />
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            </section>
            <section>
              <h4 className="resultados-features-chart-title">Complementario y Reintegro</h4>
              <div style={{ width: '100%', display: 'flex', flexDirection: 'column', gap: 'var(--space-md)' }}>
                <div style={{ width: '100%', height: 260 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <ScatterChart
                      margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                    >
                      <XAxis
                        dataKey="number"
                        type="number"
                        name="C"
                        domain={[1, 49]}
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
                          return [`${p.date}`, `C ${p.number}`];
                        }}
                      />
                      <Scatter data={filteredGapPoints(gapPointsComplementario)} fill="#0d9488" />
                    </ScatterChart>
                  </ResponsiveContainer>
                </div>
                <div style={{ width: '100%', height: 260 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <ScatterChart
                      margin={{ top: 10, right: 10, left: 10, bottom: 20 }}
                    >
                      <XAxis
                        dataKey="number"
                        type="number"
                        name="R"
                        domain={[0, 9]}
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
                          return [`${p.date}`, `R ${p.number}`];
                        }}
                      />
                      <Scatter data={filteredGapPoints(gapPointsReintegro)} fill="#ca8a04" />
                    </ScatterChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </section>
          </div>
        )}
      </Drawer>
    </section>
  );
}

