import { useEffect, useState, type ChangeEvent } from 'react';
import { useParams, useSearchParams, Link } from 'react-router-dom';
import { Drawer } from 'antd';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip as RechartsTooltip,
  Legend,
} from 'recharts';
import type { LotterySlug } from './resultados/types';
import { LOTTERY_CONFIG } from './resultados/types';
import './resultados/resultados.css';

interface RouteParams {
  drawId?: string;
}

interface ScoreRow {
  number: number;
  freq?: number;
  gap?: number;
  hot?: number;
}

interface ElGordoTicket {
  mains: number[];
  clave: number;
}

export function ElGordoSimulationPage() {
  const { drawId } = useParams<RouteParams>();
  const [searchParams, setSearchParams] = useSearchParams();
  const viewParam = searchParams.get('view');
  const view: 'compare' | 'sim' | 'pool' | 'wheel' =
    viewParam === 'compare' || viewParam === 'pool' || viewParam === 'wheel'
      ? (viewParam as 'compare' | 'pool' | 'wheel')
      : 'sim';

  const slug: LotterySlug = 'el-gordo';
  const config = LOTTERY_CONFIG[slug];

  const [simLoading, setSimLoading] = useState(false);
  const [simError, setSimError] = useState('');
  const [simResult, setSimResult] = useState<{ mains: ScoreRow[]; stars: ScoreRow[] } | null>(
    null,
  );
  const [mainSortKey, setMainSortKey] = useState<'number' | 'freq' | 'gap' | 'hot'>('number');
  const [mainSortDir, setMainSortDir] = useState<'asc' | 'desc'>('asc');
  const [starSortKey, setStarSortKey] = useState<'number' | 'freq' | 'gap' | 'hot'>('number');
  const [starSortDir, setStarSortDir] = useState<'asc' | 'desc'>('asc');

  const [kMain, setKMain] = useState(20);
  const [kClave, setKClave] = useState(6);
  const [mainFreqW, setMainFreqW] = useState(0.4);
  const [mainGapW, setMainGapW] = useState(0.3);
  const [mainHotW, setMainHotW] = useState(0.3);
  const [claveFreqW, setClaveFreqW] = useState(0.4);
  const [claveGapW, setClaveGapW] = useState(0.3);
  const [claveHotW, setClaveHotW] = useState(0.3);
  const [poolLoading, setPoolLoading] = useState(false);
  const [poolError, setPoolError] = useState('');
  const [candidatePool, setCandidatePool] = useState<any | null>(null);
  const [poolExpanded, setPoolExpanded] = useState(true);

  const [wheelLoading, setWheelLoading] = useState(false);
  const [wheelError, setWheelError] = useState('');
  const [wheelTickets, setWheelTickets] = useState<ElGordoTicket[] | null>(null);
  const [wheelCount, setWheelCount] = useState(20);

  const [compareLoading, setCompareLoading] = useState(false);
  const [compareError, setCompareError] = useState('');
  const [compareResult, setCompareResult] = useState<any | null>(null);
  const [compareTicketCount, setCompareTicketCount] = useState(10);
  const [showCompareTickets, setShowCompareTickets] = useState(false);
  const [showCompareGraph, setShowCompareGraph] = useState(false);
  const [compareGraphPoints, setCompareGraphPoints] = useState<
    { tickets: number; total: number; cost: number; earning: number }[]
  >([]);

  const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

  const handleIntChange =
    (setter: (v: number) => void) =>
    (e: ChangeEvent<HTMLInputElement>) => {
      const next = parseInt(e.target.value, 10);
      setter(Number.isNaN(next) ? 0 : next);
    };

  const handleFloatChange =
    (setter: (v: number) => void) =>
    (e: ChangeEvent<HTMLInputElement>) => {
      const next = parseFloat(e.target.value);
      setter(Number.isNaN(next) ? 0 : next);
    };

  const runSimulation = async () => {
    try {
      setSimLoading(true);
      setSimError('');
      setSimResult(null);

      const callJson = async (path: string, options?: RequestInit) => {
        const res = await fetch(path, options);
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(data.detail ?? res.statusText);
        }
        return data;
      };

      const params = new URLSearchParams();
      if (drawId) {
        params.set('cutoff_draw_id', drawId);
      }

      // 1) Train + simulate frequency
      await callJson(`${API_URL}/api/el-gordo/simulation/frequency/train`, {
        method: 'POST',
      });
      await callJson(
        `${API_URL}/api/el-gordo/simulation/frequency?${params.toString()}`,
      );

      // 2) Train + simulate gap
      await callJson(`${API_URL}/api/el-gordo/simulation/gap/train`, {
        method: 'POST',
      });
      await callJson(`${API_URL}/api/el-gordo/simulation/gap?${params.toString()}`);

      // 3) Train + simulate hot/cold (final doc returned)
      await callJson(`${API_URL}/api/el-gordo/simulation/hot/train`, {
        method: 'POST',
      });
      const finalData = await callJson(
        `${API_URL}/api/el-gordo/simulation/hot?${params.toString()}`,
      );

      setSimResult({
        mains: (finalData.mains ?? []) as ScoreRow[],
        stars: (finalData.claves ?? []) as ScoreRow[],
      });
    } catch (e) {
      setSimError(
        e instanceof Error ? e.message : 'Error al ejecutar simulación de El Gordo',
      );
    } finally {
      setSimLoading(false);
    }
  };

  const buildCandidatePool = async () => {
    if (!drawId) return;
    try {
      setPoolLoading(true);
      setPoolError('');

      const params = new URLSearchParams();
      params.set('cutoff_draw_id', drawId);
      params.set('k_main', String(kMain));
      params.set('k_clave', String(kClave));
      params.set('w_freq_main', String(mainFreqW));
      params.set('w_gap_main', String(mainGapW));
      params.set('w_hot_main', String(mainHotW));
      params.set('w_freq_clave', String(claveFreqW));
      params.set('w_gap_clave', String(claveGapW));
      params.set('w_hot_clave', String(claveHotW));

      const res = await fetch(
        `${API_URL}/api/el-gordo/simulation/candidate-pool?${params.toString()}`,
      );
      const data = await res.json();
      if (!res.ok) {
        setPoolError(data.detail ?? res.statusText);
        setCandidatePool(null);
        return;
      }
      setCandidatePool(data);
    } catch (e) {
      setPoolError(
        e instanceof Error
          ? e.message
          : 'Error al generar el pool de candidatos de El Gordo',
      );
      setCandidatePool(null);
    } finally {
      setPoolLoading(false);
    }
  };

  const runWheeling = async () => {
    setWheelError('El sistema de wheeling está en reconstrucción.');
  };

  const runComparison = async () => {
    setCompareError('El sistema de wheeling está en reconstrucción.');
    setCompareResult(null);
  };

  // Load latest saved El Gordo simulation for this draw (if any)
  useEffect(() => {
    const loadSavedSimulation = async () => {
      if (!drawId) return;
      try {
        const params = new URLSearchParams();
        params.set('cutoff_draw_id', drawId);
        params.set('limit', '1');
        const res = await fetch(
          `${API_URL}/api/el-gordo/simulation/frequency/history?${params.toString()}`,
        );
        const data = await res.json();
        if (!res.ok) return;
        const sims = (data.simulations ?? []) as any[];
        if (!sims.length) return;
        const latest = sims[0];
        setSimResult({
          mains: (latest.mains ?? []) as ScoreRow[],
          stars: (latest.claves ?? []) as ScoreRow[],
        });

        const cp = latest.candidate_pool;
        if (cp) {
          setCandidatePool(cp);
          if (typeof cp.k_main === 'number') {
            setKMain(cp.k_main);
          }
          if (typeof cp.k_clave === 'number') {
            setKClave(cp.k_clave);
          }
          const mw = cp.main_weights || {};
          const cw = cp.clave_weights || {};
          if (typeof mw.freq === 'number') {
            setMainFreqW(mw.freq);
          }
          if (typeof mw.gap === 'number') {
            setMainGapW(mw.gap);
          }
          if (typeof mw.hot === 'number') {
            setMainHotW(mw.hot);
          }
          if (typeof cw.freq === 'number') {
            setClaveFreqW(cw.freq);
          }
          if (typeof cw.gap === 'number') {
            setClaveGapW(cw.gap);
          }
          if (typeof cw.hot === 'number') {
            setClaveHotW(cw.hot);
          }
        }

        if (Array.isArray(latest.wheeling_tickets)) {
          setWheelTickets(
            latest.wheeling_tickets.map((t: any) => ({
              mains: (t.mains ?? []) as number[],
              clave: Number(t.clave),
            })),
          );
        }
      } catch {
        // ignore history load errors; user can still run a new simulation
      }
    };
    void loadSavedSimulation();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [drawId, API_URL]);

  useEffect(() => {
    // default view for El Gordo: Modelos
    if (!viewParam) {
      const params = new URLSearchParams(searchParams);
      params.set('view', 'sim');
      setSearchParams(params, { replace: true });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Build graph points for comparison (similar to Euromillones)
  useEffect(() => {
    if (!compareResult || !Array.isArray(compareResult.tickets)) {
      setCompareGraphPoints([]);
      return;
    }
    const tickets = compareResult.tickets as { mains: number[]; clave: number }[];
    const limit = Math.min(tickets.length, compareTicketCount || tickets.length);
    if (limit <= 0) {
      setCompareGraphPoints([]);
      return;
    }
    const resultMainSet = new Set((compareResult.result_main_numbers || []).map(Number));
    const resultClave = Number(compareResult.result_clave);
    const categories = (compareResult.categories || []) as {
      name: string;
      hits_main: number;
      hits_clave: number;
      prize_per_ticket?: number;
    }[];
    const getPrize = (mains: number[], clave: number) => {
      const hm = (mains || []).filter((n) => resultMainSet.has(n)).length;
      const hc = Number(clave) === resultClave ? 1 : 0;
      const cat = categories.find(
        (c) => c.hits_main === hm && c.hits_clave === hc,
      );
      return typeof cat?.prize_per_ticket === 'number' ? cat.prize_per_ticket : 0;
    };
    const rawPoints: { tickets: number; total: number; cost: number; earning: number }[] = [];
    let runningPrize = 0;
    const TICKET_COST = 1.5;
    for (let i = 0; i < limit; i += 1) {
      const t = tickets[i];
      const prize = getPrize(t.mains ?? [], Number(t.clave));
      runningPrize += prize;
      const ticketNo = i + 1;
      const cost = ticketNo * TICKET_COST;
      const earning = runningPrize - cost;
      rawPoints.push({ tickets: ticketNo, total: runningPrize, cost, earning });
    }
    const maxPoints = 120;
    const step = Math.max(1, Math.floor(rawPoints.length / maxPoints));
    const sampled: { tickets: number; total: number; cost: number; earning: number }[] = [];
    for (let i = 0; i < rawPoints.length; i += step) {
      sampled.push(rawPoints[i]);
    }
    if (
      rawPoints.length > 0 &&
      sampled[sampled.length - 1]?.tickets !== rawPoints[rawPoints.length - 1].tickets
    ) {
      sampled.push(rawPoints[rawPoints.length - 1]);
    }
    setCompareGraphPoints(sampled);
  }, [compareResult, compareTicketCount]);

  return (
    <div className="resultados-page">
      <div>
        <nav className="resultados-breadcrumb" aria-label="Ruta de navegación">
          <Link to="/">inicio</Link>
          {' > '}
          <Link to="/resultados/el-gordo?tab=prediction">Predicción El Gordo</Link>
          {' > '}
          <span>Simulación</span>
        </nav>

        <div
          className="resultados-tabs"
          role="tablist"
          aria-label={`Simulación ${config.name}`}
        >
          <button
            type="button"
            className={`resultados-tab ${view === 'compare' ? 'resultados-tab--active' : ''}`}
            role="tab"
            aria-selected={view === 'compare'}
            onClick={() => {
              const next = new URLSearchParams(searchParams);
              next.set('view', 'compare');
              setSearchParams(next);
            }}
          >
            Comparación
          </button>
          <button
            type="button"
            className={`resultados-tab ${view === 'sim' ? 'resultados-tab--active' : ''}`}
            role="tab"
            aria-selected={view === 'sim'}
            onClick={() => {
              const next = new URLSearchParams(searchParams);
              next.set('view', 'sim');
              setSearchParams(next);
            }}
          >
            Modelos (freq / gap / hot)
          </button>
          <button
            type="button"
            className={`resultados-tab ${view === 'pool' ? 'resultados-tab--active' : ''}`}
            role="tab"
            aria-selected={view === 'pool'}
            onClick={() => {
              const next = new URLSearchParams(searchParams);
              next.set('view', 'pool');
              setSearchParams(next);
            }}
          >
            Pool de candidatos
          </button>
          <button
            type="button"
            className={`resultados-tab ${view === 'wheel' ? 'resultados-tab--active' : ''}`}
            role="tab"
            aria-selected={view === 'wheel'}
            onClick={() => {
              const next = new URLSearchParams(searchParams);
              next.set('view', 'wheel');
              setSearchParams(next);
            }}
          >
            Wheeling
          </button>
        </div>

        {view === 'compare' && (
          <section
            className="card resultados-features-card"
            style={{ marginTop: 'var(--space-lg)', width: '100%' }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '1rem',
                flexWrap: 'wrap',
                marginBottom: '0.5rem',
              }}
            >
              <h3 style={{ marginTop: 0, marginBottom: 0 }}>Comparación Wheeling (El Gordo)</h3>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <label
                  className="form-label"
                  style={{ margin: 0, display: 'flex', alignItems: 'center', gap: '0.5rem' }}
                >
                  <span>Nº boletos</span>
                  <select
                    className="form-input"
                    value={compareTicketCount}
                    onChange={(e) => {
                      const next = Math.max(1, Number(e.target.value) || 1);
                      setCompareTicketCount(next);
                      // force recompute with new ticket count when tab is active
                      setCompareResult(null);
                    }}
                    disabled={compareLoading}
                    style={{ width: '7rem' }}
                  >
                    {[10, 20, 30, 50, 100, 1000, 3000].map((opt) => (
                      <option key={opt} value={opt}>
                        {opt}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  type="button"
                  className="form-input"
                  onClick={() => setShowCompareTickets((v) => !v)}
                  disabled={!compareResult || !Array.isArray(compareResult.tickets)}
                  title={showCompareTickets ? 'Ocultar boletos' : 'Ver boletos'}
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
                    width="20"
                    height="20"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    aria-hidden
                  >
                    <path d="M2 9a3 3 0 0 1 3-3h14a3 3 0 0 1 3 3v6a3 3 0 0 1-3 3H5a3 3 0 0 1-3-3V9z" />
                    <path d="M2 12h20" />
                    <path d="M8 12v3" />
                    <path d="M16 12v3" />
                  </svg>
                </button>
                <button
                  type="button"
                  className="form-input"
                  onClick={() => setShowCompareGraph(true)}
                  disabled={compareGraphPoints.length === 0}
                  title="Ver gráfico"
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
                    width="20"
                    height="20"
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
              </div>
            </div>
            {compareLoading && (
              <p style={{ marginTop: 0 }}>Cargando comparación de resultados…</p>
            )}
            {compareError && !compareLoading && !compareResult && (
              <p style={{ marginTop: 0, color: 'var(--color-error)' }}>{compareError}</p>
            )}
            {compareResult && (
              <>
                <div
                  style={{
                    display: 'flex',
                    gap: 'var(--space-lg)',
                    alignItems: 'flex-start',
                    flexWrap: 'wrap',
                  }}
                >
                  <div style={{ flex: '1 1 min(100%, 400px)' }}>
                    <div className="resultados-features-table-wrap">
                      <table className="resultados-features-table">
                        <thead>
                          <tr>
                            <th>Categoría</th>
                            <th>Aciertos</th>
                            <th>Boletos</th>
                            <th>Premio por boleto</th>
                            <th>Retorno total</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(compareResult.categories || []).map((cat: any, idx: number) => (
                            <tr key={`${idx}-${cat.name}`}>
                              <td>{cat.name}</td>
                              <td>
                                {cat.hits_main}+{cat.hits_clave}
                              </td>
                              <td>{cat.count}</td>
                              <td>
                                {typeof cat.prize_per_ticket === 'number'
                                  ? `${cat.prize_per_ticket.toFixed(2)} €`
                                  : '—'}
                              </td>
                              <td>
                                {typeof cat.total_return === 'number'
                                  ? `${cat.total_return.toFixed(2)} €`
                                  : '—'}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>

                {showCompareTickets &&
                  Array.isArray(compareResult.tickets) &&
                  compareResult.tickets.length > 0 && (() => {
                    const resultMainSet = new Set(
                      (compareResult.result_main_numbers || []).map(Number),
                    );
                    const resultClave = Number(compareResult.result_clave);
                    const TICKET_COST = 1.5;
                    return (
                      <div
                        className="resultados-features-table-wrap"
                        style={{ marginTop: 'var(--space-lg)' }}
                      >
                        <h4 style={{ margin: '0 0 0.5rem 0', fontSize: '0.95rem' }}>
                          Boletos ({compareResult.tickets.length})
                        </h4>
                        <table className="resultados-features-table">
                          <thead>
                            <tr>
                              <th>#</th>
                              <th>Números principales</th>
                              <th>Clave</th>
                              <th>Aciertos</th>
                              <th>Premio</th>
                              <th>Premio acumulado</th>
                              <th>Coste acumulado</th>
                              <th>Ganancia</th>
                            </tr>
                          </thead>
                          <tbody>
                            {(() => {
                              let runningPrize = 0;
                              return (
                                compareResult.tickets as {
                                  mains: number[];
                                  clave: number;
                                }[]
                              ).map((t, idx) => {
                                const mains = t.mains ?? [];
                                const clave = Number(t.clave);
                                const hitsMain = mains.filter((n) => resultMainSet.has(n)).length;
                                const hitsClave = clave === resultClave ? 1 : 0;
                                // approximate prize from categories
                                const cat = (compareResult.categories || []).find(
                                  (c: any) =>
                                    c.hits_main === hitsMain && c.hits_clave === hitsClave,
                                );
                                const prize =
                                  cat && typeof cat.prize_per_ticket === 'number'
                                    ? cat.prize_per_ticket
                                    : 0;
                                runningPrize += prize;
                                const ticketNo = idx + 1;
                                const costAcc = ticketNo * TICKET_COST;
                                const gain = runningPrize - costAcc;
                                return (
                                  <tr
                                    key={`${idx}-${mains.join('-')}-${clave}`}
                                  >
                                    <td>{ticketNo}</td>
                                    <td>
                                      {mains.map((n) =>
                                        resultMainSet.has(Number(n))
                                          ? (
                                              <strong
                                                key={n}
                                                style={{ marginRight: 4 }}
                                              >
                                                {String(n).padStart(2, '0')}
                                              </strong>
                                            )
                                          : (
                                              <span key={n} style={{ marginRight: 4, opacity: 0.45 }}>
                                                {String(n).padStart(2, '0')}
                                              </span>
                                            ),
                                      )}
                                    </td>
                                    <td>
                                      {clave === resultClave ? (
                                        <strong>{String(clave).padStart(2, '0')}</strong>
                                      ) : (
                                        <span style={{ opacity: 0.45 }}>
                                          {String(clave).padStart(2, '0')}
                                        </span>
                                      )}
                                    </td>
                                    <td>
                                      {hitsMain}+{hitsClave}
                                    </td>
                                    <td>{prize ? `${prize.toFixed(2)} €` : '—'}</td>
                                    <td>{runningPrize ? `${runningPrize.toFixed(2)} €` : '—'}</td>
                                    <td>{costAcc.toFixed(2)} €</td>
                                    <td>
                                      <span
                                        style={{
                                          color:
                                            gain > 0
                                              ? 'var(--color-success, green)'
                                              : gain < 0
                                              ? 'var(--color-error, #c00)'
                                              : 'inherit',
                                        }}
                                      >
                                        {gain === 0
                                          ? '0.00 €'
                                          : `${gain > 0 ? '+' : ''}${gain.toFixed(2)} €`}
                                      </span>
                                    </td>
                                  </tr>
                                );
                              });
                            })()}
                          </tbody>
                        </table>
                      </div>
                    );
                  })()}
              </>
            )}
          </section>
        )}

        {view === 'compare' && drawId && !compareLoading && !compareResult && (() => {
          void runComparison();
          return null;
        })()}

        {view === 'sim' && (
          <section
            className="card resultados-features-card"
            style={{ marginTop: 'var(--space-lg)', width: '100%' }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '1rem',
              }}
            >
              <div>
                <h3 style={{ marginTop: 0, marginBottom: '0.25rem' }}>
                  Simulación (frecuencia / gap / hot-cold)
                </h3>
                <p style={{ margin: 0, fontSize: '0.9rem', color: '#4b5563' }}>
                  Ejecuta los modelos de El Gordo hasta este sorteo y actualiza las
                  probabilidades.
                </p>
              </div>
              <button
                type="button"
                className="primary"
                disabled={simLoading}
                onClick={() => void runSimulation()}
                style={{ minWidth: '9rem' }}
              >
                {simLoading ? 'Ejecutando…' : 'Simulación'}
              </button>
            </div>
            {simError && (
              <p style={{ color: 'var(--color-error)', marginTop: '0.5rem' }}>{simError}</p>
            )}
            {simResult && (
              <div
                className="resultados-features-table-wrap"
                style={{ marginTop: 'var(--space-md)' }}
              >
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr 1fr',
                    gap: 'var(--space-lg)',
                  }}
                >
                  <div>
                    <h4 style={{ marginTop: 0 }}>
                      Todos los números principales (El Gordo)
                    </h4>
                    <table className="resultados-features-table">
                      <thead>
                        <tr>
                          <th
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              setMainSortKey('number');
                              setMainSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
                            }}
                          >
                            Número
                          </th>
                          <th
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              setMainSortKey('freq');
                              setMainSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
                            }}
                          >
                            Frecuencia %
                          </th>
                          <th
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              setMainSortKey('gap');
                              setMainSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
                            }}
                          >
                            Gap %
                          </th>
                          <th
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              setMainSortKey('hot');
                              setMainSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
                            }}
                          >
                            Hot/Cold %
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {[...simResult.mains]
                          .sort((a, b) => {
                            const getVal = (x: ScoreRow) => {
                              if (mainSortKey === 'number') return x.number;
                              if (mainSortKey === 'freq') return x.freq ?? 0;
                              if (mainSortKey === 'gap') return x.gap ?? 0;
                              return x.hot ?? 0;
                            };
                            const va = getVal(a);
                            const vb = getVal(b);
                            if (va === vb) return a.number - b.number;
                            return mainSortDir === 'asc' ? va - vb : vb - va;
                          })
                          .map((m) => (
                            <tr key={m.number}>
                              <td>{m.number.toString().padStart(2, '0')}</td>
                              <td>
                                {m.freq != null ? (m.freq * 100).toFixed(1) + '%' : '—'}
                              </td>
                              <td>
                                {m.gap != null ? (m.gap * 100).toFixed(1) + '%' : '—'}
                              </td>
                              <td>
                                {m.hot != null ? (m.hot * 100).toFixed(1) + '%' : '—'}
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                  <div>
                    <h4 style={{ marginTop: 0 }}>Todos los números clave</h4>
                    <table className="resultados-features-table">
                      <thead>
                        <tr>
                          <th
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              setStarSortKey('number');
                              setStarSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
                            }}
                          >
                            Clave
                          </th>
                          <th
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              setStarSortKey('freq');
                              setStarSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
                            }}
                          >
                            Frecuencia %
                          </th>
                          <th
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              setStarSortKey('gap');
                              setStarSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
                            }}
                          >
                            Gap %
                          </th>
                          <th
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              setStarSortKey('hot');
                              setStarSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
                            }}
                          >
                            Hot/Cold %
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {[...simResult.stars]
                          .sort((a, b) => {
                            const getVal = (x: ScoreRow) => {
                              if (starSortKey === 'number') return x.number;
                              if (starSortKey === 'freq') return x.freq ?? 0;
                              if (starSortKey === 'gap') return x.gap ?? 0;
                              return x.hot ?? 0;
                            };
                            const va = getVal(a);
                            const vb = getVal(b);
                            if (va === vb) return a.number - b.number;
                            return starSortDir === 'asc' ? va - vb : vb - va;
                          })
                          .map((s) => (
                            <tr key={s.number}>
                              <td>{s.number.toString().padStart(2, '0')}</td>
                              <td>
                                {s.freq != null ? (s.freq * 100).toFixed(1) + '%' : '—'}
                              </td>
                              <td>
                                {s.gap != null ? (s.gap * 100).toFixed(1) + '%' : '—'}
                              </td>
                              <td>
                                {s.hot != null ? (s.hot * 100).toFixed(1) + '%' : '—'}
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            )}
          </section>
        )}

        {view === 'pool' && (
          <section
            className="card"
            style={{ marginTop: 'var(--space-lg)', width: '100%' }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '1rem',
                marginBottom: 'var(--space-md)',
              }}
            >
              <div>
                <h3 style={{ marginTop: 0, marginBottom: '0.25rem' }}>
                  Pool de candidatos (El Gordo)
                </h3>
                <p style={{ margin: 0, fontSize: '0.9rem', color: '#4b5563' }}>
                  Ajusta el tamaño del pool y los pesos de cada modelo que se usarán en el
                  generador de combinaciones (5 números + clave).
                </p>
              </div>
              <button
                type="button"
                className="primary"
                disabled={poolLoading}
                onClick={() => void buildCandidatePool()}
                style={{ minWidth: '9rem' }}
              >
                {poolLoading ? 'Generando…' : 'Generador'}
              </button>
            </div>

            {candidatePool && (
              <div
                style={{
                  marginTop: 'var(--space-sm)',
                  marginBottom: 'var(--space-md)',
                  backgroundColor: '#0d6efd',
                  borderRadius: '6px',
                  padding: '0.75rem 1rem',
                  color: '#ffffff',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  gap: '1.5rem',
                }}
              >
                <div>
                  <div style={{ fontSize: '0.85rem', opacity: 0.9 }}>Pool principal</div>
                  <div style={{ marginTop: '0.25rem', fontSize: '0.9rem', fontWeight: 500 }}>
                    Pool main ({candidatePool.k_main}):{' '}
                    {(candidatePool.main_pool || []).join(' ')}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: '0.85rem', opacity: 0.9, textAlign: 'right' }}>
                    Pool clave
                  </div>
                  <div
                    style={{
                      marginTop: '0.25rem',
                      fontSize: '0.9rem',
                      fontWeight: 500,
                      textAlign: 'right',
                    }}
                  >
                    Pool clave ({candidatePool.k_clave}):{' '}
                    {(candidatePool.clave_pool || []).join(' ')}
                  </div>
                </div>
                <button
                  type="button"
                  aria-label={
                    poolExpanded ? 'Ocultar tabla de parámetros' : 'Mostrar tabla de parámetros'
                  }
                  title={
                    poolExpanded ? 'Ocultar tabla de parámetros' : 'Mostrar tabla de parámetros'
                  }
                  onClick={() => setPoolExpanded((v) => !v)}
                  style={{
                    border: 'none',
                    background: 'rgba(255,255,255,0.1)',
                    color: '#ffffff',
                    cursor: 'pointer',
                    borderRadius: '999px',
                    width: '32px',
                    height: '32px',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: '1rem',
                  }}
                >
                  {poolExpanded ? '▴' : '▾'}
                </button>
              </div>
            )}

            {poolExpanded && (
              <table
                className="resultados-features-table"
                style={{ marginBottom: 'var(--space-md)' }}
              >
                <thead>
                  <tr>
                    <th style={{ width: '30%' }}>Parámetro</th>
                    <th>Números principales</th>
                    <th>Número clave</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>Tamaño del pool</td>
                    <td>
                      <input
                        type="number"
                        min={1}
                        max={54}
                        value={kMain}
                        onChange={handleIntChange(setKMain)}
                        className="form-input"
                      />
                    </td>
                    <td>
                      <input
                        type="number"
                        min={1}
                        max={10}
                        value={kClave}
                        onChange={handleIntChange(setKClave)}
                        className="form-input"
                      />
                    </td>
                  </tr>
                  <tr>
                    <td>Peso frecuencia</td>
                    <td>
                      <input
                        type="number"
                        step="0.05"
                        min={0}
                        max={1}
                        value={mainFreqW}
                        onChange={handleFloatChange(setMainFreqW)}
                        className="form-input"
                      />
                    </td>
                    <td>
                      <input
                        type="number"
                        step="0.05"
                        min={0}
                        max={1}
                        value={claveFreqW}
                        onChange={handleFloatChange(setClaveFreqW)}
                        className="form-input"
                      />
                    </td>
                  </tr>
                  <tr>
                    <td>Peso gap</td>
                    <td>
                      <input
                        type="number"
                        step="0.05"
                        min={0}
                        max={1}
                        value={mainGapW}
                        onChange={handleFloatChange(setMainGapW)}
                        className="form-input"
                      />
                    </td>
                    <td>
                      <input
                        type="number"
                        step="0.05"
                        min={0}
                        max={1}
                        value={claveGapW}
                        onChange={handleFloatChange(setClaveGapW)}
                        className="form-input"
                      />
                    </td>
                  </tr>
                  <tr>
                    <td>Peso hot/cold</td>
                    <td>
                      <input
                        type="number"
                        step="0.05"
                        min={0}
                        max={1}
                        value={mainHotW}
                        onChange={handleFloatChange(setMainHotW)}
                        className="form-input"
                      />
                    </td>
                    <td>
                      <input
                        type="number"
                        step="0.05"
                        min={0}
                        max={1}
                        value={claveHotW}
                        onChange={handleFloatChange(setClaveHotW)}
                        className="form-input"
                      />
                    </td>
                  </tr>
                </tbody>
              </table>
            )}

            {poolError && (
              <p style={{ color: 'var(--color-error)', marginTop: '0.5rem' }}>{poolError}</p>
            )}
          </section>
        )}

        {view === 'wheel' && (
          <section
            className="card resultados-features-card"
            style={{ marginTop: 'var(--space-lg)', width: '100%' }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '1rem',
              }}
            >
              <div>
                <h3 style={{ marginTop: 0, marginBottom: '0.25rem' }}>
                  Sistema Wheeling (El Gordo)
                </h3>
                <p style={{ margin: 0, fontSize: '0.9rem', color: '#4b5563' }}>
                  Genera boletos de El Gordo (5 números + clave) a partir del pool de candidatos
                  guardado.
                </p>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <label
                  className="form-label"
                  style={{ margin: 0, display: 'flex', alignItems: 'center', gap: '0.5rem' }}
                >
                  <span>Nº boletos</span>
                  <select
                    className="form-input"
                    value={wheelCount}
                    onChange={(e) => setWheelCount(Math.max(1, Number(e.target.value) || 1))}
                    style={{ width: '7rem' }}
                  >
                    {[10, 20, 30, 50, 100, 1000, 3000].map((opt) => (
                      <option key={opt} value={opt}>
                        {opt}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  type="button"
                  className="primary"
                  disabled={wheelLoading}
                  onClick={() => void runWheeling()}
                  style={{ minWidth: '9rem' }}
                >
                  {wheelLoading ? 'Generando…' : 'Generar boletos'}
                </button>
              </div>
            </div>

            {wheelError && (
              <p style={{ color: 'var(--color-error)', marginTop: '0.5rem' }}>{wheelError}</p>
            )}

            {wheelTickets && wheelTickets.length > 0 && (
              <div
                className="resultados-features-table-wrap"
                style={{ marginTop: 'var(--space-md)' }}
              >
                <table className="resultados-features-table">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>Números principales</th>
                      <th>Clave</th>
                    </tr>
                  </thead>
                  <tbody>
                    {wheelTickets.slice(0, wheelCount).map((t, idx) => (
                      <tr key={`${idx}-${t.mains.join('-')}-${t.clave}`}>
                        <td>{idx + 1}</td>
                        <td>{t.mains.join(' ')}</td>
                        <td>{t.clave}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}
      </div>
      {view === 'compare' && compareResult && (
        <aside
          className="resultados-page-sidebar resultados-theme-el-gordo"
          style={{
            position: 'sticky',
            top: 'var(--space-md)',
          }}
        >
          <div
            className="card"
            style={{
              padding: 'var(--space-md)',
              background: 'var(--color-surface-alt, #f3f4f6)',
              borderRadius: '8px',
              border: '1px solid var(--color-border, #e5e7eb)',
            }}
          >
            <h4 style={{ margin: '0 0 0.75rem 0', fontSize: '1rem' }}>
              Resultado del sorteo
            </h4>
            <p style={{ margin: '0 0 0.5rem 0', fontSize: '0.85rem', color: '#6b7280' }}>
              {compareResult.result_draw_date ?? '—'}
            </p>
            <p style={{ margin: '0 0 0.35rem 0', fontSize: '0.85rem', fontWeight: 600 }}>
              Números principales
            </p>
            <div className="resultados-balls" style={{ marginBottom: '0.75rem' }}>
              {(compareResult.result_main_numbers || []).map((n: number, i: number) => (
                <span key={i} className="resultados-ball">
                  {String(n).padStart(2, '0')}
                </span>
              ))}
            </div>
            <p style={{ margin: '0 0 0.35rem 0', fontSize: '0.85rem', fontWeight: 600 }}>
              Clave
            </p>
            <div className="resultados-balls">
              <span className="resultados-ball">
                {compareResult.result_clave != null
                  ? String(compareResult.result_clave).padStart(2, '0')
                  : '—'}
              </span>
            </div>
          </div>
          {(() => {
            const totalTickets = Number(compareResult.total_tickets) || 0;
            const totalReturn =
              typeof compareResult.total_return === 'number'
                ? compareResult.total_return
                : 0;
            const cost = totalTickets * 1.5;
            const earning = totalReturn - cost;
            return (
              <div
                className="card"
                style={{
                  marginTop: 'var(--space-md)',
                  padding: 'var(--space-md)',
                  background: 'var(--color-surface-alt, #f3f4f6)',
                  borderRadius: '8px',
                  border: '1px solid var(--color-border, #e5e7eb)',
                }}
              >
                <p style={{ margin: 0, fontSize: '0.9rem' }}>
                  Total estimado:{' '}
                  <strong>
                    {typeof compareResult.total_return === 'number'
                      ? `${compareResult.total_return.toFixed(2)} €`
                      : '0.00 €'}
                  </strong>
                </p>
                <p style={{ margin: '0.5rem 0 0', fontSize: '0.9rem' }}>
                  Coste de boletos:{' '}
                  <strong>{cost.toFixed(2)} €</strong>
                </p>
                <p style={{ margin: '0.25rem 0 0', fontSize: '0.9rem' }}>
                  {earning >= 0 ? 'Ganancia' : 'Pérdida'}:{' '}
                  <strong
                    style={{
                      color:
                        earning >= 0
                          ? 'var(--color-success, green)'
                          : 'var(--color-error, #c00)',
                    }}
                  >
                    {earning >= 0 ? '+' : ''}
                    {earning.toFixed(2)} €
                  </strong>
                </p>
              </div>
            );
          })()}
        </aside>
      )}
      <Drawer
        title="Comparación Wheeling El Gordo — gráfico por nº de boletos"
        placement="right"
        width="100%"
        open={showCompareGraph && compareGraphPoints.length > 0}
        onClose={() => setShowCompareGraph(false)}
        bodyStyle={{ padding: 24 }}
      >
        {compareGraphPoints.length === 0 ? (
          <p style={{ marginTop: 0 }}>No hay datos para mostrar el gráfico.</p>
        ) : (
          <div
            className="resultados-features-fullcharts"
            style={{ maxWidth: 960, margin: '0 auto' }}
          >
            <section>
              <h4 className="resultados-features-chart-title">
                Total estimado, coste y ganancia por nº de boletos
              </h4>
              <div style={{ width: '100%', height: 420 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart
                    data={compareGraphPoints}
                    margin={{ top: 10, right: 20, left: 0, bottom: 20 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                    <XAxis
                      dataKey="tickets"
                      label={{ value: 'Boletos', position: 'insideBottom', offset: -10 }}
                    />
                    <YAxis />
                    <RechartsTooltip
                      formatter={(value, key) => {
                        const label =
                          key === 'total'
                            ? 'Total estimado'
                            : key === 'cost'
                            ? 'Coste'
                            : 'Ganancia';
                        return [`${(value as number).toFixed(2)} €`, label];
                      }}
                      labelFormatter={(label) => `Boletos: ${label}`}
                    />
                    <Legend />
                    <Line
                      type="monotone"
                      dataKey="total"
                      name="Total estimado"
                      stroke="#22c55e"
                      strokeWidth={1.2}
                      dot={false}
                    />
                    <Line
                      type="monotone"
                      dataKey="cost"
                      name="Coste de boletos"
                      stroke="#3b82f6"
                      strokeWidth={1.2}
                      dot={false}
                    />
                    <Line
                      type="monotone"
                      dataKey="earning"
                      name="Ganancia"
                      stroke="#f97316"
                      strokeWidth={1.2}
                      dot={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </section>
          </div>
        )}
      </Drawer>
    </div>
  );
}

