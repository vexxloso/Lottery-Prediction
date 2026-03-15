import { useEffect, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { Pagination } from 'antd';
import type { LotterySlug } from './types';
import { LOTTERY_CONFIG } from './types';
import { useDraws, PAGE_SIZE } from './useDraws';
import type { Draw } from './types';
import './resultados.css';

function formatDrawDate(fecha: string): string {
  if (!fecha) return '—';
  const s = fecha.split(' ')[0];
  if (!s) return fecha;
  const [y, m, d] = s.split('-');
  const date = new Date(Number(y), Number(m) - 1, Number(d));
  const weekday = date.toLocaleDateString('es-ES', { weekday: 'long' });
  const day = date.toLocaleDateString('es-ES', { day: '2-digit', month: '2-digit', year: 'numeric' });
  return `${weekday} - ${day}`;
}

function formatJackpot(premio: string): string {
  if (!premio) return '—';
  const n = Number(premio);
  if (Number.isNaN(n)) return premio;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(0)} MILLONES DE €`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)} MIL €`;
  return `${n} €`;
}

/** Format prize from escrutinio (e.g. "199961.61" or "199961,61") to Spanish locale: 199.961,61 € */
function formatPremio(premio: string | null | undefined): string {
  if (premio == null || premio === '') return '—';
  const s = String(premio).replace(',', '.');
  const n = Number(s);
  if (Number.isNaN(n)) return premio.includes('€') ? premio : `${premio} €`;
  return `${n.toLocaleString('es-ES', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} €`;
}

/** Format winner count with Spanish thousands (e.g. 195211 -> 195.211) */
function formatGanadores(val: string | null | undefined): string {
  if (val == null || val === '') return '—';
  const n = Number(String(val).replace(/\./g, '').replace(',', '.'));
  if (Number.isNaN(n)) return String(val);
  return n.toLocaleString('es-ES');
}

/** Generic formatter for integer-like counts (e.g. apuestas recibidas) */
function formatEntero(val: string | number | null | undefined): string {
  if (val == null || val === '') return '—';
  const n = Number(String(val).replace(/\./g, '').replace(',', '.'));
  if (Number.isNaN(n)) return String(val);
  return n.toLocaleString('es-ES', { maximumFractionDigits: 0 });
}

/** Generic formatter for euro totals (recaudación, premios, etc.) */
function formatEuroTotal(val: string | number | null | undefined): string {
  if (val == null || val === '') return '—';
  const s = String(val).replace(',', '.');
  const n = Number(s);
  if (Number.isNaN(n)) {
    return String(val).includes('€') ? String(val) : `${val} €`;
  }
  return `${n.toLocaleString('es-ES', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} €`;
}

/** Joker number with spaces as thousands separator (e.g. 5580403 -> "5 580 403") */
function formatJokerNumber(val: string | null | undefined): string {
  if (val == null || val === '') return '—';
  const digits = String(val).replace(/\D/g, '');
  if (!digits) return String(val);
  return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
}

/** La Primitiva escrutinio category labels (reference table format) */
const LA_PRIMITIVA_CATEGORIAS = [
  'Especial (6 Aciertos + R)',
  '1ª (6 Aciertos)',
  '2ª (5 Aciertos + C)',
  '3ª (5 Aciertos)',
  '4ª (4 Aciertos)',
  '5ª (3 Aciertos)',
  'Reintegro',
];

function getLaPrimitivaCategoriaLabel(row: { tipo?: string; categoria?: number }, index: number): string {
  if (row.tipo && row.tipo.trim()) return row.tipo;
  if (row.categoria != null && row.categoria >= 0 && row.categoria < LA_PRIMITIVA_CATEGORIAS.length) return LA_PRIMITIVA_CATEGORIAS[row.categoria];
  return LA_PRIMITIVA_CATEGORIAS[index] ?? `Categoría ${index + 1}`;
}

/** El Gordo escrutinio category labels (reference table format) */
const EL_GORDO_CATEGORIAS = [
  '1ª (5+1)',
  '2ª (5+0)',
  '3ª (4+1)',
  '4ª (4+0)',
  '5ª (3+1)',
  '6ª (3+0)',
  '7ª (2+1)',
  '8ª (2+0)',
  'Reintegro',
];

function getElGordoCategoriaLabel(row: { tipo?: string; categoria?: number }, index: number): string {
  if (row.tipo && row.tipo.trim()) return row.tipo;
  if (row.categoria != null && row.categoria >= 0 && row.categoria < EL_GORDO_CATEGORIAS.length) return EL_GORDO_CATEGORIAS[row.categoria];
  return EL_GORDO_CATEGORIAS[index] ?? `Categoría ${index + 1}`;
}

interface ResultadosPageProps {
  lottery: LotterySlug;
}

export function ResultadosPage({ lottery }: ResultadosPageProps) {
  const config = LOTTERY_CONFIG[lottery];
  const themeClass = `resultados-theme-${config.theme}`;
  const [expandedDrawId, setExpandedDrawId] = useState<string | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const pageParam = searchParams.get('page');
  const pageFromUrl = pageParam ? Math.max(1, Number(pageParam) || 1) : 1;
  const {
    draws,
    total,
    loading,
    error,
    fromInput,
    toInput,
    setFromInput,
    setToInput,
    search,
    currentPage,
    totalPages,
    setPage,
  } = useDraws(lottery);

  const latestJackpot = draws.length > 0 ? draws[0].premio_bote : '';

  useEffect(() => {
    if (pageFromUrl !== currentPage) {
      setPage(pageFromUrl);
    }
  }, [pageFromUrl, currentPage, setPage]);

  const updatePageInUrl = (page: number) => {
    const params = new URLSearchParams(searchParams);
    params.set('page', String(page));
    setSearchParams(params, { replace: true });
  };

  const handlePageChange = (page: number) => {
    setPage(page);
    updatePageInUrl(page);
  };

  return (
    <div className={`resultados-page ${themeClass}`}>
      <div>
        <nav className="resultados-breadcrumb" aria-label="Ruta de navegación">
          <Link to="/">inicio</Link>
          {' > '}
          <span>Resultados</span>
          {' > '}
          <span>{config.name}</span>
        </nav>

        <div className="resultados-jackpot-banner">
          <h2>{config.title}</h2>
          <span className="resultados-jackpot-amount">
            {latestJackpot ? formatJackpot(latestJackpot) : '—'}
          </span>
        </div>

        {error && (
          <p className="resultados-empty" style={{ color: 'var(--color-error)' }}>{error}</p>
        )}

        {loading && draws.length === 0 && (
          <p className="resultados-loading">Cargando sorteos…</p>
        )}

        {!loading && draws.length === 0 && !error && (
          <p className="resultados-empty">No hay sorteos. Ajusta fechas o importa datos en Scraping.</p>
        )}

        {draws.length > 0 && (
          <div className="resultados-draw-list">
            {draws.map((draw) => (
              <DrawCard
                key={draw.id_sorteo}
                draw={draw}
                lottery={lottery}
                expanded={expandedDrawId === draw.id_sorteo}
                onInfoClick={() => setExpandedDrawId((id) => (id === draw.id_sorteo ? null : draw.id_sorteo))}
              />
            ))}
          </div>
        )}

        {total > 0 && (
          <div className="resultados-pagination">
            <Pagination
              current={currentPage}
              total={total}
              pageSize={PAGE_SIZE}
              showSizeChanger={false}
              onChange={handlePageChange}
              showTotal={(t) => `${t} sorteos`}
            />
          </div>
        )}
      </div>

      <aside className="resultados-sidebar">
        <div className="resultados-sidebar-card">
          <h3>Buscar sorteos</h3>
          <form
            className="resultados-search-form"
            onSubmit={(e) => {
              e.preventDefault();
              search();
              updatePageInUrl(1);
            }}
          >
            <label htmlFor="resultados-desde">Desde</label>
            <input
              id="resultados-desde"
              type="date"
              value={fromInput}
              onChange={(e) => setFromInput(e.target.value)}
              placeholder="dd/mm/aaaa"
            />
            <label htmlFor="resultados-hasta">Hasta</label>
            <input
              id="resultados-hasta"
              type="date"
              value={toInput}
              onChange={(e) => setToInput(e.target.value)}
              placeholder="dd/mm/aaaa"
            />
            <button type="submit" className="btn-buscar primary">
              Buscar
            </button>
          </form>
        </div>
      </aside>

    </div>
  );
}

/** Parse combinacion_acta keeping order: "24-33-28-35-13-05-09" → main 5, stars 2 */
function parseCombinacionActaEuromillones(combinacion_acta: string | null | undefined): { main: number[]; stars: number[] } {
  if (!combinacion_acta || typeof combinacion_acta !== 'string') return { main: [], stars: [] };
  const parts = combinacion_acta.split(/[\s\-]+/).filter(Boolean);
  const nums = parts.map((p) => parseInt(p, 10)).filter((n) => !Number.isNaN(n));
  return { main: nums.slice(0, 5), stars: nums.slice(5, 7) };
}

/** La Primitiva: combinacion_acta e.g. "48 - 38 - 40 - 08 - 25 - 47 C(20) R(9)" → main 6, complementario, reintegro */
function parseCombinacionActaLaPrimitiva(combinacion_acta: string | null | undefined): { main: number[]; complementario?: number; reintegro?: number } {
  if (!combinacion_acta || typeof combinacion_acta !== 'string') return { main: [] };
  const withoutCR = combinacion_acta.replace(/\s*C\s*\(\s*\d+\s*\)/gi, '').replace(/\s*R\s*\(\s*\d+\s*\)/gi, '');
  const parts = withoutCR.split(/[\s\-]+/).filter(Boolean);
  const nums = parts.map((p) => parseInt(p, 10)).filter((n) => !Number.isNaN(n));
  const main = nums.slice(0, 6);
  const matchC = combinacion_acta.match(/C\s*\(\s*(\d+)\s*\)/i);
  const matchR = combinacion_acta.match(/R\s*\(\s*(\d+)\s*\)/i);
  const complementario = matchC ? parseInt(matchC[1], 10) : undefined;
  const reintegro = matchR ? parseInt(matchR[1], 10) : undefined;
  return { main, complementario, reintegro };
}

/** El Gordo: combinacion_acta e.g. "1-2-3-4-5" + clave → main 5, clave */
function parseCombinacionActaElGordo(combinacion_acta: string | null | undefined): { main: number[]; clave?: number } {
  if (!combinacion_acta || typeof combinacion_acta !== 'string') return { main: [] };
  const parts = combinacion_acta.split(/[\s\-]+/).filter(Boolean);
  const nums = parts.map((p) => parseInt(p, 10)).filter((n) => !Number.isNaN(n));
  return { main: nums.slice(0, 5), clave: nums[5] };
}

function DrawCard({
  draw,
  lottery,
  expanded,
  onInfoClick,
}: {
  draw: Draw;
  lottery: LotterySlug;
  expanded: boolean;
  onInfoClick: () => void;
}) {
  const config = LOTTERY_CONFIG[lottery];
  const themeClass = `resultados-theme-${config.theme}`;
  const [hoverOrden, setHoverOrden] = useState(false);
  const escrutinio = draw.escrutinio || [];
  const escrutinioMillon = lottery === 'euromillones' ? (draw.escrutinio_millon || []) : [];

  const mainNumbers: number[] = [];
  const starNumbers: number[] = [];
  if (draw.numbers && draw.numbers.length >= 5) {
    if (lottery === 'euromillones' && draw.numbers.length >= 7) {
      mainNumbers.push(...draw.numbers.slice(0, 5));
      starNumbers.push(...draw.numbers.slice(5, 7));
    } else {
      mainNumbers.push(...draw.numbers.slice(0, lottery === 'el-gordo' ? 5 : 6));
      if (lottery === 'euromillones' && draw.numbers.length >= 7) {
        starNumbers.push(...draw.numbers.slice(5, 7));
      }
    }
  } else if (draw.numbers) {
    mainNumbers.push(...draw.numbers);
  }

  const actaEuromillones = lottery === 'euromillones' ? parseCombinacionActaEuromillones(draw.combinacion_acta) : { main: [] as number[], stars: [] as number[] };
  const actaLaPrimitiva = lottery === 'la-primitiva' ? parseCombinacionActaLaPrimitiva(draw.combinacion_acta) : { main: [] as number[], complementario: undefined as number | undefined, reintegro: undefined as number | undefined };
  const actaElGordo = lottery === 'el-gordo' ? parseCombinacionActaElGordo(draw.combinacion_acta) : { main: [] as number[], clave: undefined as number | undefined };

  const useActaEuromillones = lottery === 'euromillones' && hoverOrden && actaEuromillones.main.length === 5;
  const useActaLaPrimitiva = lottery === 'la-primitiva' && hoverOrden && actaLaPrimitiva.main.length === 6;
  const useActaElGordo = lottery === 'el-gordo' && hoverOrden && actaElGordo.main.length === 5;

  const displayMainNumbers =
    lottery === 'euromillones' ? (useActaEuromillones ? actaEuromillones.main : mainNumbers)
    : lottery === 'la-primitiva' ? (useActaLaPrimitiva ? actaLaPrimitiva.main : mainNumbers)
    : lottery === 'el-gordo' ? (useActaElGordo ? actaElGordo.main : mainNumbers)
    : mainNumbers;
  const displayStarNumbers = useActaEuromillones && actaEuromillones.stars.length === 2 ? actaEuromillones.stars : starNumbers;
  /* La Primitiva: C and R always shown; on hover use values from combinacion_acta (e.g. C(32) R(9)), else from combinacion/draw */
  const displayComplementario =
    lottery === 'la-primitiva' ? (hoverOrden ? (actaLaPrimitiva.complementario ?? draw.complementario) : draw.complementario) : undefined;
  const displayReintegro =
    lottery === 'la-primitiva' ? (hoverOrden ? (actaLaPrimitiva.reintegro ?? draw.reintegro) : draw.reintegro) : lottery === 'el-gordo' ? draw.reintegro : null;
  /* El Gordo: N° CLAVE always visible; hover = acta order (with fallback), no hover = draw value (with fallback) */
  const displayClave =
    lottery === 'el-gordo'
      ? (useActaElGordo ? (actaElGordo.clave ?? draw.reintegro) : (draw.reintegro ?? actaElGordo.clave))
      : undefined;

  const ordenLabel = hoverOrden ? 'Ver de menor a mayor' : 'Ver por orden de aparición';
  const escrutinioTitle = lottery === 'euromillones' ? 'euromillones' : lottery === 'la-primitiva' ? 'la primitiva' : 'el gordo';

  return (
    <article className={`resultados-draw-card ${themeClass} ${expanded ? 'resultados-draw-card-expanded' : ''}`}>
      <div className="resultados-draw-card-header">
        <span className="lottery-name">{config.title}</span>
        <span className="resultados-draw-card-date">{formatDrawDate(draw.fecha_sorteo)}</span>
        <button
          type="button"
          className="resultados-info-btn"
          onClick={onInfoClick}
          aria-expanded={expanded}
          aria-label={expanded ? 'Menos información' : 'Más información'}
        >
          {expanded ? '− Menos ▴' : '+ Más ▾'}
        </button>
      </div>
      <div className="resultados-draw-card-body resultados-draw-card-column">
        <div className="resultados-draw-hover-zone">
          <p
            className="resultados-ver-orden resultados-ver-orden-toggle"
            onMouseEnter={() => setHoverOrden(true)}
            onMouseLeave={() => setHoverOrden(false)}
            title={ordenLabel}
          >
            {ordenLabel}
          </p>
          <div className="resultados-draw-balls-row">
            <div className="resultados-balls resultados-balls-main">
              {displayMainNumbers.map((n, i) => (
                <span key={i} className="resultados-ball">
                  {String(n).padStart(2, '0')}
                </span>
              ))}
            </div>
            {lottery === 'el-gordo' && displayClave != null && (
              <div className="resultados-balls-clave">
                <span className="resultados-clave-label">N° CLAVE</span>
                <span className="resultados-ball clave">{displayClave}</span>
              </div>
            )}
            {lottery === 'la-primitiva' && (displayComplementario != null || displayReintegro != null) && (
              <div className="resultados-balls-cr">
                {displayComplementario != null && (
                  <span className="resultados-cr-item">
                    <span className="resultados-cr-label">C</span>
                    <span className="resultados-ball complementario">{displayComplementario}</span>
                  </span>
                )}
                {displayReintegro != null && (
                  <span className="resultados-cr-item">
                    <span className="resultados-cr-label">R</span>
                    <span className="resultados-ball reintegro">{displayReintegro}</span>
                  </span>
                )}
              </div>
            )}
            {lottery === 'euromillones' && displayStarNumbers.length > 0 && (
              <div className="resultados-balls resultados-balls-stars">
                {displayStarNumbers.map((n, i) => (
                  <span key={`s-${i}`} className="resultados-ball-star-wrap" title="Estrella">
                    <img src="/images/start.svg" alt="" className="resultados-star-img" aria-hidden />
                    <span className="resultados-star-num">{String(n).padStart(2, '0')}</span>
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
        {lottery === 'euromillones' && draw.joker_combinacion && (
          <div className="resultados-millon-row">
            <span className="resultados-millon-label-text">EL MILLÓN</span>
            <div className="resultados-millon-code-box">{draw.joker_combinacion}</div>
          </div>
        )}
        {lottery === 'la-primitiva' && draw.joker_combinacion && (
          <div className="resultados-millon-row resultados-joker-row">
            <img src="/images/joker.svg" alt="" className="resultados-joker-logo" aria-hidden />
            <div className="resultados-joker-code-box">{formatJokerNumber(draw.joker_combinacion)}</div>
          </div>
        )}
      </div>
      {expanded && (
        <div className="resultados-draw-card-expand" role="region" aria-label="Detalle del sorteo">
          <div className="resultados-escrutinio-wrap">
            <h4 className="resultados-escrutinio-title">{escrutinioTitle} &gt; escrutinio</h4>
            <div className="resultados-escrutinio-table-wrap">
              <table className={`resultados-escrutinio-table ${lottery === 'la-primitiva' ? 'resultados-escrutinio-la-primitiva' : ''}`}>
                <thead>
                  <tr>
                    {(lottery === 'la-primitiva' || lottery === 'el-gordo') ? (
                      <>
                        <th>Categorías</th>
                        <th>Acertantes</th>
                        <th>Premios</th>
                        <th>Agraciados</th>
                      </>
                    ) : (
                      <>
                        <th>categoria</th>
                        <th>ganadores</th>
                        <th>ganadores_eu</th>
                        <th>premio</th>
                        <th>tipo</th>
                      </>
                    )}
                  </tr>
                </thead>
                <tbody>
                  {(escrutinio.length > 0 ? escrutinio : []).map((row, i) => (
                    <tr key={i}>
                      {lottery === 'la-primitiva' ? (
                        <>
                          <td>{getLaPrimitivaCategoriaLabel(row, i)}</td>
                          <td className="resultados-escrutinio-num">{formatGanadores(row.ganadores)}</td>
                          <td className="resultados-escrutinio-num">{formatPremio(row.premio)}</td>
                          <td className="resultados-escrutinio-num">{row.agraciados_espana ?? '—'}</td>
                        </>
                      ) : lottery === 'el-gordo' ? (
                        <>
                          <td>{getElGordoCategoriaLabel(row, i)}</td>
                          <td className="resultados-escrutinio-num">{formatGanadores(row.ganadores)}</td>
                          <td className="resultados-escrutinio-num">{formatPremio(row.premio)}</td>
                          <td className="resultados-escrutinio-num">{row.agraciados_espana ?? '—'}</td>
                        </>
                      ) : (
                        <>
                          <td className="resultados-escrutinio-num">{row.categoria ?? '—'}</td>
                          <td className="resultados-escrutinio-num">{formatGanadores(row.ganadores)}</td>
                          <td className="resultados-escrutinio-num">{formatGanadores(row.ganadores_eu)}</td>
                          <td className="resultados-escrutinio-num">{formatPremio(row.premio)}</td>
                          <td>{row.tipo ?? '—'}</td>
                        </>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {escrutinio.length === 0 && (
              <p className="resultados-escrutinio-disclaimer">No hay datos de escrutinio para este sorteo.</p>
            )}
            {escrutinio.length > 0 && Number(escrutinio[0]?.ganadores) === 0 && (
              <p className="resultados-escrutinio-disclaimer">
                Al no haber acertantes de 1ª categoría, el importe correspondiente pasa a engrosar el BOTE para el siguiente sorteo.
              </p>
            )}
            {lottery === 'la-primitiva' && escrutinio.length > 0 && (
              <p className="resultados-escrutinio-disclaimer">Sin acertantes del premio especial</p>
            )}
          </div>

          {lottery === 'euromillones' && escrutinioMillon.length > 0 && (
            <div className="resultados-detail-millon-banner">
              <h4 className="resultados-escrutinio-title">El Millón</h4>
              <div className="resultados-escrutinio-table-wrap">
                <table className="resultados-escrutinio-table">
                  <thead>
                    <tr>
                      <th>El Millón</th>
                      <th>Acertantes</th>
                      <th>Premios</th>
                      <th>Agraciados</th>
                    </tr>
                  </thead>
                  <tbody>
                    {escrutinioMillon.map((row, i) => (
                      <tr key={i}>
                        <td>Ganador</td>
                        <td className="resultados-escrutinio-num">{formatGanadores(row.ganadores)}</td>
                        <td className="resultados-escrutinio-num">{formatPremio(row.premio)}</td>
                        <td className="resultados-escrutinio-num">{row.agraciados_espana ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {lottery === 'euromillones' ? (
            <div className="resultados-detail-stats resultados-detail-stats--euromillones">
              <div className="resultados-detail-row">
                <span className="resultados-detail-label">Apuestas recibidas:</span>
                <span className="resultados-detail-value">
                  {formatEntero((draw.apuestas ?? draw.aquestas) as any)}
                </span>
                <span className="resultados-detail-label">Bote publicitado:</span>
                <span className="resultados-detail-value">
                  {draw.premio_bote
                    ? `${Number(draw.premio_bote).toLocaleString('es-ES')} €`
                    : '—'}
                </span>
              </div>
              <div className="resultados-detail-row">
                <span className="resultados-detail-label">Recaudación:</span>
                <span className="resultados-detail-value">
                  {formatEuroTotal(draw.recaudacion as any)}
                </span>
                <span className="resultados-detail-label">Premios:</span>
                <span className="resultados-detail-value">
                  {formatEuroTotal(draw.premios as any)}
                </span>
              </div>
              <div className="resultados-detail-row">
                <span className="resultados-detail-label">Recaudación europea:</span>
                <span className="resultados-detail-value">
                  {formatEuroTotal(draw.recaudacion_europea as any)}
                </span>
                <span className="resultados-detail-label" />
                <span className="resultados-detail-value" />
              </div>
            </div>
          ) : lottery === 'el-gordo' ? (
            <div className="resultados-detail-stats resultados-detail-stats--euromillones">
              <div className="resultados-detail-row">
                <span className="resultados-detail-label">Apuestas recibidas:</span>
                <span className="resultados-detail-value">
                  {formatEntero(draw.apuestas as any)}
                </span>
                <span className="resultados-detail-label">Bote publicitado:</span>
                <span className="resultados-detail-value">
                  {draw.premio_bote
                    ? `${Number(draw.premio_bote).toLocaleString('es-ES')} €`
                    : '—'}
                </span>
              </div>
              <div className="resultados-detail-row">
                <span className="resultados-detail-label">Recaudación:</span>
                <span className="resultados-detail-value">
                  {formatEuroTotal(draw.recaudacion as any)}
                </span>
                <span className="resultados-detail-label">Premios:</span>
                <span className="resultados-detail-value">
                  {formatEuroTotal(draw.premios as any)}
                </span>
              </div>
            </div>
          ) : lottery === 'la-primitiva' ? (
            <div className="resultados-detail-stats resultados-detail-stats--euromillones">
              <div className="resultados-detail-row">
                <span className="resultados-detail-label">Apuestas recibidas:</span>
                <span className="resultados-detail-value">
                  {formatEntero(draw.apuestas as any)}
                </span>
                <span className="resultados-detail-label">Bote publicitado:</span>
                <span className="resultados-detail-value">
                  {draw.premio_bote
                    ? `${Number(draw.premio_bote).toLocaleString('es-ES')} €`
                    : '—'}
                </span>
              </div>
              <div className="resultados-detail-row">
                <span className="resultados-detail-label">Recaudación:</span>
                <span className="resultados-detail-value">
                  {formatEuroTotal(draw.recaudacion as any)}
                </span>
                <span className="resultados-detail-label">Premios:</span>
                <span className="resultados-detail-value">
                  {formatEuroTotal(draw.premios as any)}
                </span>
              </div>
            </div>
          ) : (
            <dl className="resultados-detail-stats">
              <dt>Bote publicitado</dt>
              <dd>
                {draw.premio_bote
                  ? `${Number(draw.premio_bote).toLocaleString('es-ES')} €`
                  : '—'}
              </dd>
            </dl>
          )}
        </div>
      )}
    </article>
  );
}
