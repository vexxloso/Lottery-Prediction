import { useState } from 'react';
import { Drawer } from 'antd';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
} from 'recharts';
import type { EuromillonesFeatureRow } from './useEuromillonesFeatures';
import { useEuromillonesFeatures } from './useEuromillonesFeatures';

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

  const [selectedRow, setSelectedRow] = useState<EuromillonesFeatureRow | null>(null);

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
                          onClick={() => setSelectedRow(row)}
                          aria-label="Ver gráfico de frecuencias"
                          title="Ver gráfico de frecuencias"
                        >
                          <img src="/images/frequency.svg" alt="" className="resultados-features-icon" />
                        </button>
                        <button
                          type="button"
                          className="resultados-features-iconbtn"
                          style={{ marginLeft: 8 }}
                          aria-label="Ver gráfico de gaps"
                          title="Ver gráfico de gaps"
                        >
                          <img src="/images/gape.svg" alt="" className="resultados-features-icon" />
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
            ? `Frecuencia Euromillones — ${formatFecha(selectedRow.fecha_sorteo)}`
            : 'Frecuencia Euromillones'
        }
        placement="right"
        width="100%"
        open={!!selectedRow}
        onClose={() => setSelectedRow(null)}
        bodyStyle={{ padding: 24 }}
      >
        {selectedRow && (
          <div className="resultados-features-fullcharts">
            {(() => {
              const { hotMains, coldMains, hotStars, coldStars } = computeHotCold(selectedRow);
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
                </div>
              );
            })()}

            <div className="resultados-features-fullcharts">
              <section>
                <h4 className="resultados-features-chart-title">Números principales (1–50)</h4>
                <div style={{ width: '100%', height: 260 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={Array.from({ length: 50 }, (_, i) => ({
                        number: i + 1,
                        count: Number((selectedRow.frequency ?? [])[i] ?? 0),
                      }))}
                      margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
                    >
                      <XAxis dataKey="number" />
                      <YAxis />
                      <Tooltip />
                      <Bar dataKey="count" fill="#1677ff" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </section>

              <section>
                <h4 className="resultados-features-chart-title">Estrellas (1–12)</h4>
                <div style={{ width: '100%', height: 260 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={Array.from({ length: 12 }, (_, i) => ({
                        number: i + 1,
                        count: Number((selectedRow.frequency ?? [])[50 + i] ?? 0),
                      }))}
                      margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
                    >
                      <XAxis dataKey="number" />
                      <YAxis />
                      <Tooltip />
                      <Bar dataKey="count" fill="#eab308" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </section>
            </div>
          </div>
        )}
      </Drawer>
    </section>
  );
}
