import { useEffect, useState, type ChangeEvent } from 'react';
import { useSearchParams, useParams, Link } from 'react-router-dom';
import { Drawer } from 'antd';
import type { LotterySlug } from './resultados/types';
import { LOTTERY_CONFIG } from './resultados/types';
import './resultados/resultados.css';
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

function parseEuroPremio(value: unknown): number {
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const cleaned = value.replace(/\./g, '').replace(',', '.').replace(/[^\d.]/g, '');
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : 0;
  }
  return 0;
}

interface EuromillonesFeatureRow {
  id_sorteo: string;
  pre_id_sorteo?: string | null;
  fecha_sorteo?: string;
  dia_semana?: string;
  main_number?: number[];
  star_number?: number[];
  frequency?: Array<number | null>;
  gap?: Array<number | null>;
  presence_mask?: number[];
}

export function SimulationPage() {
  const { lottery, drawId } = useParams<'lottery' | 'drawId'>();
  const [searchParams, setSearchParams] = useSearchParams();
  const viewParam = searchParams.get('view');
  const view: 'compare' | 'sim' | 'pool' | 'wheel' | 'pred' =
    viewParam === 'sim' ||
    viewParam === 'pool' ||
    viewParam === 'wheel' ||
    viewParam === 'pred'
      ? (viewParam as 'sim' | 'pool' | 'wheel' | 'pred')
      : 'compare';

  const slug = (lottery as LotterySlug) || 'euromillones';
  const config = LOTTERY_CONFIG[slug];

  const [simLoading, setSimLoading] = useState(false);
  const [simError, setSimError] = useState('');
  const [simResult, setSimResult] = useState<{
    mains: { number: number; freq?: number; gap?: number; hot?: number }[];
    stars: { number: number; freq?: number; gap?: number; hot?: number }[];
  } | null>(null);
  const [mainSortKey, setMainSortKey] = useState<'number' | 'freq' | 'gap' | 'hot'>('number');
  const [mainSortDir, setMainSortDir] = useState<'asc' | 'desc'>('asc');
  const [starSortKey, setStarSortKey] = useState<'number' | 'freq' | 'gap' | 'hot'>('number');
  const [starSortDir, setStarSortDir] = useState<'asc' | 'desc'>('asc');

  // Candidate pool dashboard state
  const [kMain, setKMain] = useState(15);
  const [kStar, setKStar] = useState(4);
  const [mainFreqW, setMainFreqW] = useState(0.4);
  const [mainGapW, setMainGapW] = useState(0.3);
  const [mainHotW, setMainHotW] = useState(0.3);
  const [starFreqW, setStarFreqW] = useState(0.5);
  const [starGapW, setStarGapW] = useState(0.25);
  const [starHotW, setStarHotW] = useState(0.25);
  const [poolLoading, setPoolLoading] = useState(false);
  const [poolError, setPoolError] = useState('');
  const [candidatePool, setCandidatePool] = useState<any | null>(null);
  const [poolExpanded, setPoolExpanded] = useState(true);
  const [wheelLoading] = useState(false);
  const [wheelError, setWheelError] = useState('');
  const [wheelTickets, setWheelTickets] = useState<{ mains: number[]; stars: number[] }[] | null>(
    null,
  );
  const [wheelCount, setWheelCount] = useState(20);
  const [compareLoading] = useState(false);
  const [compareError, setCompareError] = useState('');
  const [compareResult, setCompareResult] = useState<any | null>(null);
  const [compareTicketCount, setCompareTicketCount] = useState(10);
  const [showCompareTickets, setShowCompareTickets] = useState(false);
  const [showCompareGraph, setShowCompareGraph] = useState(false);
  const [compareGraphPoints, setCompareGraphPoints] = useState<
    { tickets: number; total: number; cost: number; earning: number }[]
  >([]);
  // Compare page: real draw + train progress (candidate pool)
  const [compareDrawLoading, setCompareDrawLoading] = useState(false);
  const [compareDrawError, setCompareDrawError] = useState('');
  const [compareDraw, setCompareDraw] = useState<{
    id_sorteo: string;
    fecha_sorteo?: string;
    main: number[];
    stars: number[];
    escrutinio?: any[] | null;
  } | null>(null);
  const [compareProgressLoading, setCompareProgressLoading] = useState(false);
  const [compareProgressError, setCompareProgressError] = useState('');
  type CompareTicket =
    | { mains: number[]; stars: number[] }
    | { mains: number[]; clave: number };

  const [compareProgress, setCompareProgress] = useState<{
    cutoff_draw_id: string;
    candidate_pool?: CompareTicket[];
    candidate_pool_count?: number;
  } | null>(null);
  const [comparePoolLimit, setComparePoolLimit] = useState(20);
  const [showComparePoolTable, setShowComparePoolTable] = useState(false);
  const [featureRowsLoading, setFeatureRowsLoading] = useState(false);
  const [featureRowsError, setFeatureRowsError] = useState('');
  const [featureRows, setFeatureRows] = useState<EuromillonesFeatureRow[]>([]);

  const TICKET_BUDGET_EUR = slug === 'el-gordo' ? 1.5 : 2.5;

  const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

  // Load latest saved simulation for this draw (if any)
  useEffect(() => {
    const loadSavedSimulation = async () => {
      if (slug !== 'euromillones' || !drawId) return;
      try {
        const params = new URLSearchParams();
        params.set('cutoff_draw_id', drawId);
        params.set('limit', '1');
        const res = await fetch(
          `${API_URL}/api/euromillones/simulation/frequency/history?${params.toString()}`,
        );
        const data = await res.json();
        if (!res.ok) return;
        const sims = (data.simulations ?? []) as any[];
        if (!sims.length) return;
        const latest = sims[0];
        setSimResult({
          mains: (latest.mains ?? []) as { number: number; freq?: number; gap?: number; hot?: number }[],
          stars: (latest.stars ?? []) as { number: number; freq?: number; gap?: number; hot?: number }[],
        });

        // If a candidate pool was already generated for this simulation, load it
        const cp = latest.candidate_pool;
        if (cp) {
          setCandidatePool(cp);
          if (typeof cp.k_main === 'number') {
            setKMain(cp.k_main);
          }
          if (typeof cp.k_star === 'number') {
            setKStar(cp.k_star);
          }
          const mw = cp.main_weights || {};
          const sw = cp.star_weights || {};
          if (typeof mw.freq === 'number') {
            setMainFreqW(mw.freq);
          }
          if (typeof mw.gap === 'number') {
            setMainGapW(mw.gap);
          }
          if (typeof mw.hot === 'number') {
            setMainHotW(mw.hot);
          }
          if (typeof sw.freq === 'number') {
            setStarFreqW(sw.freq);
          }
          if (typeof sw.gap === 'number') {
            setStarGapW(sw.gap);
          }
          if (typeof sw.hot === 'number') {
            setStarHotW(sw.hot);
          }
        }

        // If wheeling tickets already exist, load them
        if (Array.isArray(latest.wheeling_tickets)) {
          setWheelTickets(
            latest.wheeling_tickets.map((t: any) => ({
              mains: (t.mains ?? []) as number[],
              stars: (t.stars ?? []) as number[],
            })),
          );
          // Keep current wheelCount selection; do not override from history
        }
      } catch {
        // ignore history load errors; user can still run a new simulation
      }
    };
    void loadSavedSimulation();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [slug, drawId]);

  // Load Euromillones feature-model rows for Prediction tab
  useEffect(() => {
    const loadFeatureRows = async () => {
      if (slug !== 'euromillones' || view !== 'pred') return;
      try {
        setFeatureRowsLoading(true);
        setFeatureRowsError('');
        setFeatureRows([]);
        const params = new URLSearchParams();
        params.set('limit', '50');
        if (drawId) {
          params.set('draw_id', drawId);
        }
        const res = await fetch(`${API_URL}/api/euromillones/feature-model?${params.toString()}`);
        const data = await res.json();
        if (!res.ok) {
          setFeatureRowsError(data.detail ?? res.statusText);
          return;
        }
        setFeatureRows((data.features ?? []) as EuromillonesFeatureRow[]);
      } catch (e) {
        setFeatureRowsError(e instanceof Error ? e.message : 'Error al cargar euromillones_feature');
      } finally {
        setFeatureRowsLoading(false);
      }
    };
    void loadFeatureRows();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [slug, drawId, view, API_URL]);

  const runAllSimulation = async () => {
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

      if (slug === 'euromillones') {
        // 1) Train + simulate frequency
        await callJson(`${API_URL}/api/euromillones/simulation/frequency/train`, {
          method: 'POST',
        });
        await callJson(
          `${API_URL}/api/euromillones/simulation/frequency?${params.toString()}`,
        );

        // 2) Train + simulate gap
        await callJson(`${API_URL}/api/euromillones/simulation/gap/train`, {
          method: 'POST',
        });
        await callJson(
          `${API_URL}/api/euromillones/simulation/gap?${params.toString()}`,
        );

        // 3) Train + simulate hot/cold (final doc returned)
        await callJson(`${API_URL}/api/euromillones/simulation/hot/train`, {
          method: 'POST',
        });
        const finalData = await callJson(
          `${API_URL}/api/euromillones/simulation/hot?${params.toString()}`,
        );

        setSimResult({
          mains: (finalData.mains ?? []) as {
            number: number;
            freq?: number;
            gap?: number;
            hot?: number;
          }[],
          stars: (finalData.stars ?? []) as {
            number: number;
            freq?: number;
            gap?: number;
            hot?: number;
          }[],
        });
      } else if (slug === 'el-gordo') {
        const finalData = await callJson(
          `${API_URL}/api/el-gordo/simulation/simple?${params.toString()}`,
        );

        setSimResult({
          mains: (finalData.mains ?? []) as {
            number: number;
            freq?: number;
            gap?: number;
            hot?: number;
          }[],
          stars: (finalData.claves ?? []) as {
            number: number;
            freq?: number;
            gap?: number;
            hot?: number;
          }[],
        });
      }
    } catch (e) {
      setSimError(
        e instanceof Error ? e.message : 'Error al ejecutar simulaciones (todas)',
      );
    } finally {
      setSimLoading(false);
    }
  };

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

  const buildCandidatePool = async () => {
    if (slug !== 'euromillones' || !drawId) return;
    try {
      setPoolLoading(true);
      setPoolError('');

      const params = new URLSearchParams();
      params.set('cutoff_draw_id', drawId);
      params.set('k_main', String(kMain));
      params.set('k_star', String(kStar));
      params.set('w_freq_main', String(mainFreqW));
      params.set('w_gap_main', String(mainGapW));
      params.set('w_hot_main', String(mainHotW));
      params.set('w_freq_star', String(starFreqW));
      params.set('w_gap_star', String(starGapW));
      params.set('w_hot_star', String(starHotW));

      const res = await fetch(
        `${API_URL}/api/euromillones/simulation/candidate-pool?${params.toString()}`,
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
        e instanceof Error ? e.message : 'Error al generar el candidate pool',
      );
      setCandidatePool(null);
    } finally {
      setPoolLoading(false);
    }
  };

  // Wheeling system removed; keep placeholders to avoid runtime errors
  const runWheeling = async () => {
    setWheelError('El sistema de wheeling está en reconstrucción.');
  };

  const openComparePoolGraph = () => {
    if (!compareProgress || !compareDraw) return;
    const pool = compareProgress.candidate_pool ?? [];
    const limit = Math.min(comparePoolLimit, pool.length);
    if (!limit) return;
    const mainSet = new Set(compareDraw.main.map(Number));
    const starSet = new Set(compareDraw.stars.map(Number));
    const escrutinio = Array.isArray(compareDraw.escrutinio)
      ? (compareDraw.escrutinio as any[])
      : [];
    const prizeByHits = new Map<string, number>();
    escrutinio.forEach((row: any) => {
      const aciertos = String(row.tipo ?? row.aciertos ?? row.categoria ?? '').trim();
      const m = aciertos.match(/(\d+)\s*\+\s*(\d+)/);
      if (!m) return;
      const hm = Number(m[1]);
      const hs = Number(m[2]);
      if (!Number.isFinite(hm) || !Number.isFinite(hs)) return;
      const key = `${hm}-${hs}`;
      const premio = parseEuroPremio(row.premio);
      if (premio > 0) {
        prizeByHits.set(key, premio);
      }
    });
    const points: { tickets: number; total: number; cost: number; earning: number }[] = [];
    let runningPrize = 0;
    pool.slice(0, limit).forEach((t, idx) => {
      const anyTicket = t as any;
      const mains = (anyTicket.mains ?? []).map(Number);
      const starsOrClave = Array.isArray(anyTicket.stars)
        ? anyTicket.stars.map(Number)
        : anyTicket.clave != null
        ? [Number(anyTicket.clave)]
        : [];
      const hitsMain = mains.filter((n: number) => mainSet.has(n)).length;
      const hitsStar = starsOrClave.filter((n: number) => starSet.has(n)).length;
      const prizePerTicket = prizeByHits.get(`${hitsMain}-${hitsStar}`) ?? 0;
      runningPrize += prizePerTicket;
      const ticketNo = idx + 1;
      const cost = ticketNo * TICKET_BUDGET_EUR;
      const earning = runningPrize - cost;
      points.push({ tickets: ticketNo, total: runningPrize, cost, earning });
    });
    setCompareGraphPoints(points);
    setShowCompareGraph(true);
  };

  useEffect(() => {
    if (view === 'compare') {
      setCompareError('');
      setCompareResult(null);
      setCompareGraphPoints([]);
    }
  }, [view]);

  // Compare page: fetch real draw (id_sorteo) and train progress (cutoff_draw_id = prev_id)
  useEffect(() => {
    if (view !== 'compare' || !drawId) {
      setCompareDraw(null);
      setCompareProgress(null);
      return;
    }
    const prevId = searchParams.get('prev_id')?.trim() || undefined;

    const loadDraw = async () => {
      setCompareDrawLoading(true);
      setCompareDrawError('');
      setCompareDraw(null);
      try {
        const isEuromillones = slug === 'euromillones';
        const endpoint = isEuromillones ? '/api/euromillones/draw' : '/api/el-gordo/draw';
        const res = await fetch(
          `${API_URL}${endpoint}?draw_id=${encodeURIComponent(drawId)}`,
        );
        const data = await res.json();
        if (!res.ok) {
          setCompareDrawError(data.detail ?? res.statusText ?? 'Error al cargar sorteo');
          return;
        }
        const numbers = Array.isArray(data.numbers) ? data.numbers : [];
        const combinacionActa = data.combinacion_acta;
        let main: number[] = [];
        let stars: number[] = [];
        if (isEuromillones) {
          if (numbers.length >= 7) {
            main = numbers.slice(0, 5).map((n: unknown) => Number(n));
            stars = numbers.slice(5, 7).map((n: unknown) => Number(n));
          } else if (combinacionActa && typeof combinacionActa === 'string') {
            const parts = combinacionActa.split(/[\s\-]+/).filter(Boolean);
            const nums = parts
              .map((p: string) => parseInt(p, 10))
              .filter((n: number) => !Number.isNaN(n));
            main = nums.slice(0, 5);
            stars = nums.slice(5, 7);
          }
        } else {
          // El Gordo: numbers -> main_number, reintegro -> clave.
          const reintegro = typeof data.reintegro === 'number' ? data.reintegro : undefined;
          if (numbers.length >= 5) {
            main = numbers.slice(0, 5).map((n: unknown) => Number(n));
            stars = reintegro != null ? [reintegro] : [];
          } else if (combinacionActa && typeof combinacionActa === 'string') {
            // Fallback: parse strings like "03 - 13 - 25 - 45 - 52 R(0)"
            const mainMatches = combinacionActa.match(/\b\d{1,2}\b/g) || [];
            main = mainMatches.slice(0, 5).map((p) => parseInt(p, 10)).filter((n) => !Number.isNaN(n));
            let claveVal: number | undefined;
            const rMatch = combinacionActa.match(/R\((\d)\)/i);
            if (rMatch) {
              const v = parseInt(rMatch[1], 10);
              if (!Number.isNaN(v)) claveVal = v;
            } else if (mainMatches.length > 5) {
              const v = parseInt(mainMatches[5], 10);
              if (!Number.isNaN(v)) claveVal = v;
            }
            stars = claveVal != null ? [claveVal] : [];
          }
        }
        setCompareDraw({
          id_sorteo: String(data.id_sorteo ?? drawId),
          fecha_sorteo: data.fecha_sorteo,
          main,
          stars,
          escrutinio: Array.isArray(data.escrutinio) ? data.escrutinio : null,
        });
      } catch (e) {
        setCompareDrawError(e instanceof Error ? e.message : 'Error al cargar sorteo');
      } finally {
        setCompareDrawLoading(false);
      }
    };

    const loadProgress = async () => {
      if (!prevId) {
        setCompareProgress(null);
        setCompareProgressError('');
        return;
      }
      setCompareProgressLoading(true);
      setCompareProgressError('');
      setCompareProgress(null);
      try {
        const isEuromillones = slug === 'euromillones';
        const endpoint = isEuromillones
          ? '/api/euromillones/train/progress'
          : '/api/el-gordo/train/progress';
        const res = await fetch(
          `${API_URL}${endpoint}?cutoff_draw_id=${encodeURIComponent(prevId)}`,
        );
        const data = await res.json();
        if (!res.ok) {
          setCompareProgressError(data.detail ?? res.statusText ?? 'Error al cargar progreso');
          return;
        }
        const progress = data.progress ?? null;
        if (!progress) {
          setCompareProgress(null);
          return;
        }
        const pool = progress.candidate_pool;
        setCompareProgress({
          cutoff_draw_id: progress.cutoff_draw_id ?? prevId,
          candidate_pool: Array.isArray(pool) ? (pool as CompareTicket[]) : undefined,
          candidate_pool_count: progress.candidate_pool_count,
        });
      } catch (e) {
        setCompareProgressError(e instanceof Error ? e.message : 'Error al cargar progreso');
      } finally {
        setCompareProgressLoading(false);
      }
    };

    void loadDraw();
    void loadProgress();
  }, [slug, view, drawId, searchParams, API_URL]);

  // Derive graph points from per-ticket prizes, cost, and gains (same logic as table)
  useEffect(() => {
    if (slug !== 'euromillones' || view !== 'compare' || !compareResult) {
      setCompareGraphPoints([]);
      return;
    }
    const tickets = (compareResult.tickets ?? []) as { mains: number[]; stars: number[] }[];
    if (!tickets.length) {
      setCompareGraphPoints([]);
      return;
    }
    const limit = Math.min(compareTicketCount, tickets.length);
    const mainSet = new Set((compareResult.result_main_numbers || []).map(Number));
    const starSet = new Set((compareResult.result_star_numbers || []).map(Number));
    const categories = (compareResult.categories || []) as {
      name: string;
      hits_main: number;
      hits_star: number;
      prize_per_ticket?: number;
    }[];
    const getPrize = (mains: number[], stars: number[]) => {
      const hm = (mains || []).filter((n) => mainSet.has(n)).length;
      const hs = (stars || []).filter((n) => starSet.has(n)).length;
      const cat = categories.find((c) => c.hits_main === hm && c.hits_star === hs);
      return typeof cat?.prize_per_ticket === 'number' ? cat.prize_per_ticket : 0;
    };
    const rawPoints: { tickets: number; total: number; cost: number; earning: number }[] = [];
    let runningPrize = 0;
    for (let i = 0; i < limit; i += 1) {
      const t = tickets[i];
      const prize = getPrize(t.mains ?? [], t.stars ?? []);
      runningPrize += prize;
      const ticketNo = i + 1;
      const cost = ticketNo * TICKET_BUDGET_EUR;
      const earning = runningPrize - cost;
      rawPoints.push({ tickets: ticketNo, total: runningPrize, cost, earning });
    }
    // Downsample to keep the line visually light while preserving shape
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
  }, [slug, view, compareResult, compareTicketCount]);

  return (
    <div className={`resultados-page ${view === 'compare' ? 'resultados-page--single' : ''}`}>
      <div>
        <nav className="resultados-breadcrumb" aria-label="Ruta de navegación">
          <Link to="/">inicio</Link>
          {' > '}
          <Link to={`/resultados/${slug}?tab=prediction`}>Predicción {config.name}</Link>
          {' > '}
          <span>Simulación</span>
        </nav>

        {(slug === 'euromillones' || slug === 'el-gordo') && view !== 'compare' && (
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
            {slug === 'euromillones' && (
              <button
                type="button"
                className={`resultados-tab ${view === 'pred' ? 'resultados-tab--active' : ''}`}
                role="tab"
                aria-selected={view === 'pred'}
                onClick={() => {
                  const next = new URLSearchParams(searchParams);
                  next.set('view', 'pred');
                  setSearchParams(next);
                }}
              >
                Predicción vs resultado
              </button>
            )}
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
        )}

        {view === 'compare' && (
          <>
            {compareDrawLoading && <p style={{ margin: 0 }}>Cargando sorteo…</p>}
            {compareDrawError && !compareDrawLoading && (
              <p style={{ margin: 0, color: 'var(--color-error)' }}>{compareDrawError}</p>
            )}
            {compareProgressLoading && (
              <p style={{ margin: '0.25rem 0 0' }}>Cargando pool de predicción…</p>
            )}
            {compareProgressError && !compareProgressLoading && (
              <p style={{ margin: '0.25rem 0 0', color: 'var(--color-error)' }}>
                {compareProgressError}
              </p>
            )}

            {(compareDraw || compareProgress) && (
              <div className="euromillones-compare-layout">
                <section className="card resultados-features-card resultados-theme-euromillones euromillones-compare-table-col">
                  {compareProgress && compareDraw && (
                    <div className="euromillones-compare-pool">
                      <div
                        style={{
                          display: 'flex',
                          alignItems: 'flex-start',
                          justifyContent: 'space-between',
                          gap: '1rem',
                          flexWrap: 'wrap',
                        }}
                      >
                        <div>
                          <h4 style={{ margin: '0 0 0.25rem 0', fontSize: '0.95rem' }}>
                            Predicción (pool con cutoff {compareProgress.cutoff_draw_id})
                          </h4>
                          <p style={{ margin: 0, fontSize: '0.9rem', color: 'var(--color-text-muted)' }}>
                            {compareProgress.candidate_pool_count ??
                              (compareProgress.candidate_pool?.length ?? 0)}{' '}
                            boletos en el pool.
                          </p>
                        </div>

                        <div
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '0.5rem',
                            flexWrap: 'wrap',
                          }}
                        >
                          <label
                            className="form-label"
                            style={{
                              margin: 0,
                              display: 'flex',
                              alignItems: 'center',
                              gap: '0.5rem',
                              whiteSpace: 'nowrap',
                            }}
                          >
                            <span>Ver</span>
                            <select
                              className="form-input"
                              value={comparePoolLimit}
                              onChange={(e) =>
                                setComparePoolLimit(Math.max(1, Number(e.target.value) || 20))
                              }
                              style={{ width: '7rem' }}
                            >
                              {[10, 20, 30, 50, 100, 500, 1000, 3000].map((opt) => (
                                <option key={opt} value={opt}>
                                  {opt}
                                </option>
                              ))}
                            </select>
                            <span>boletos</span>
                          </label>
                          <button
                            type="button"
                            className="form-input"
                            onClick={() => setShowComparePoolTable((v) => !v)}
                            title={showComparePoolTable ? 'Ocultar tabla' : 'Ver tabla'}
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
                              <rect x="3" y="4" width="18" height="16" rx="2" ry="2" />
                              <line x1="3" y1="10" x2="21" y2="10" />
                              <line x1="3" y1="16" x2="21" y2="16" />
                              <line x1="9" y1="4" x2="9" y2="20" />
                              <line x1="15" y1="4" x2="15" y2="20" />
                            </svg>
                          </button>
                          <button
                            type="button"
                            className="form-input"
                            onClick={openComparePoolGraph}
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
                        </div>
                      </div>

                      {(() => {
                        const pool = compareProgress.candidate_pool ?? [];
                        const limit = Math.min(comparePoolLimit, pool.length);
                        const mainSet = new Set(compareDraw.main.map(Number));
                        const starSet = new Set(compareDraw.stars.map(Number));
                        const escrutinio = Array.isArray(compareDraw.escrutinio)
                          ? (compareDraw.escrutinio as any[])
                          : [];
                        const prizeByHits = new Map<
                          string,
                          { label: string; count: number; prizePerTicket: number; totalPrize: number }
                        >();
                        const prizeLookup = new Map<string, number>();
                        escrutinio.forEach((row: any) => {
                          const aciertos = String(
                            row.tipo ?? row.aciertos ?? row.categoria ?? '',
                          ).trim();
                          const m = aciertos.match(/(\d+)\s*\+\s*(\d+)/);
                          if (!m) return;
                          const hm = Number(m[1]);
                          const hs = Number(m[2]);
                          if (!Number.isFinite(hm) || !Number.isFinite(hs)) return;
                          const key = `${hm}-${hs}`;
                          const premio = parseEuroPremio(row.premio);
                          if (premio > 0) {
                            prizeLookup.set(key, premio);
                          }
                        });
                        let runningPrize = 0;
                        const rows = pool.slice(0, limit).map((t, idx) => {
                          const anyTicket = t as any;
                          const mains = (anyTicket.mains ?? []).map(Number);
                          const starsOrClave = Array.isArray(anyTicket.stars)
                            ? anyTicket.stars.map(Number)
                            : anyTicket.clave != null
                            ? [Number(anyTicket.clave)]
                            : [];
                          const hitsMain = mains.filter((n: number) => mainSet.has(n)).length;
                          const hitsStar = starsOrClave.filter((n: number) => starSet.has(n)).length;
                          const ticketNo = idx + 1;
                          const costAcc = ticketNo * TICKET_BUDGET_EUR;
                          const prizePerTicket =
                            prizeLookup.get(`${hitsMain}-${hitsStar}`) ?? 0;
                          runningPrize += prizePerTicket;
                          const earning = runningPrize - costAcc;
                          const patternKey = `${hitsMain}+${hitsStar}`;
                          if (!prizeByHits.has(patternKey)) {
                            prizeByHits.set(patternKey, {
                              label:
                                prizePerTicket > 0
                                  ? `${patternKey} · ${prizePerTicket.toFixed(2)} €`
                                  : patternKey,
                              count: 0,
                              prizePerTicket,
                              totalPrize: 0,
                            });
                          }
                          const agg = prizeByHits.get(patternKey)!;
                          agg.count += 1;
                          agg.totalPrize += prizePerTicket;
                          return (
                            <tr
                              key={`${idx}-${mains.join('-')}-${
                                Array.isArray(anyTicket.stars)
                                  ? anyTicket.stars.join('-')
                                  : anyTicket.clave ?? ''
                              }`}
                            >
                              <td>{ticketNo}</td>
                              <td>
                                <div
                                  style={{
                                    display: 'flex',
                                    flexWrap: 'wrap',
                                    alignItems: 'center',
                                    gap: 6,
                                  }}
                                >
                                  <div className="resultados-balls">
                                    {mains.map((n, i) => (
                                      <span
                                        key={`m-${idx}-${i}`}
                                        className="resultados-ball"
                                        style={
                                          mainSet.has(n)
                                            ? {
                                                fontWeight: 700,
                                                border: '2px solid rgba(255,255,255,0.9)',
                                                opacity: 1,
                                              }
                                            : { opacity: 0.45 }
                                        }
                                      >
                                        {String(n).padStart(2, '0')}
                                      </span>
                                    ))}
                                  </div>
                                  {slug === 'euromillones' ? (
                                    <div className="resultados-balls">
                                      {starsOrClave.map((n, i) => (
                                        <span
                                          key={`s-${idx}-${i}`}
                                          className="resultados-ball-star-wrap"
                                          title="Estrella"
                                          style={starSet.has(n) ? { opacity: 1 } : { opacity: 0.45 }}
                                        >
                                          <img
                                            src="/images/start.svg"
                                            alt=""
                                            className="resultados-star-img"
                                            aria-hidden
                                          />
                                          <span
                                            className="resultados-star-num"
                                            style={starSet.has(n) ? { fontWeight: 700 } : undefined}
                                          >
                                            {String(n).padStart(2, '0')}
                                          </span>
                                        </span>
                                      ))}
                                    </div>
                                  ) : (
                                    starsOrClave.length > 0 && (
                                      <div className="resultados-balls">
                                        {starsOrClave.map((n, i) => (
                                          <span
                                            key={`c-${idx}-${i}`}
                                            className="resultados-ball"
                                            style={
                                              starSet.has(n)
                                                ? {
                                                    fontWeight: 700,
                                                    border: '2px solid rgba(255,255,255,0.9)',
                                                    opacity: 1,
                                                  }
                                                : { opacity: 0.45 }
                                            }
                                          >
                                            {String(n).padStart(2, '0')}
                                          </span>
                                        ))}
                                      </div>
                                    )
                                  )}
                                </div>
                              </td>
                              <td>
                                {hitsMain}+{hitsStar}
                                {prizePerTicket
                                  ? ` · ${prizePerTicket.toFixed(2)} €`
                                  : ''}
                              </td>
                              <td>{costAcc.toFixed(2)} €</td>
                              <td>{`${runningPrize.toFixed(2)} €`}</td>
                              <td>
                                <span
                                  style={{
                                    color:
                                      earning > 0
                                        ? 'var(--color-success, green)'
                                        : earning < 0
                                        ? 'var(--color-error, #c00)'
                                        : 'inherit',
                                  }}
                                >
                                  {earning === 0
                                    ? '0.00 €'
                                    : `${earning > 0 ? '+' : ''}${earning.toFixed(2)} €`}
                                </span>
                              </td>
                            </tr>
                          );
                        });
                        const totalCost = limit * TICKET_BUDGET_EUR;
                        const totalPrize = runningPrize;
                        const totalEarning = totalPrize - totalCost;
                        return (
                          <div style={{ marginTop: 'var(--space-md)' }}>
                            <div className="resultados-features-table-wrap" style={{ marginBottom: 'var(--space-md)' }}>
                              <table className="resultados-features-table">
                                <thead>
                                  <tr>
                                    <th>Aciertos / premio</th>
                                    <th>Count</th>
                                    <th>Earning</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {Array.from(prizeByHits.entries())
                                    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
                                    .map(([key, agg]) => (
                                      <tr key={key}>
                                        <td>{agg.label}</td>
                                        <td>{agg.count}</td>
                                        <td>{agg.totalPrize.toFixed(2)} €</td>
                                      </tr>
                                    ))}
                                </tbody>
                              </table>
                            </div>

                            {showComparePoolTable && (
                              <div className="resultados-features-table-wrap">
                                <table className="resultados-features-table">
                                  <thead>
                                    <tr>
                                      <th style={{ width: 60 }}>#</th>
                                      <th>Boletos (pool)</th>
                                      <th style={{ width: 180 }}>Aciertos / premio</th>
                                      <th style={{ width: 150 }}>Coste acumulado</th>
                                      <th style={{ width: 150 }}>Premio acumulado</th>
                                      <th style={{ width: 150 }}>Ganancia</th>
                                    </tr>
                                  </thead>
                                  <tbody>{rows}</tbody>
                                </table>
                              </div>
                            )}
                          </div>
                        );
                      })()}
                    </div>
                  )}
                </section>

                <section className="euromillones-compare-card-col">
                  {compareDraw && (
                    <>
                      <div className="euromillones-train-current-draw-card euromillones-compare-result-card">
                        <div
                          style={{
                            fontSize: '0.75rem',
                            color: 'var(--color-text-muted)',
                            marginBottom: 'var(--space-xs)',
                            textTransform: 'uppercase',
                            letterSpacing: '0.04em',
                          }}
                        >
                          Resultado real (sorteo {compareDraw.id_sorteo})
                        </div>
                        {compareDraw.fecha_sorteo && (
                          <div
                            style={{
                              fontSize: '0.9rem',
                              fontWeight: 600,
                              marginBottom: 'var(--space-sm)',
                            }}
                          >
                            {compareDraw.fecha_sorteo}
                          </div>
                        )}
                        <div
                          style={{
                            display: 'flex',
                            flexWrap: 'wrap',
                            gap: 6,
                            alignItems: 'center',
                            marginBottom: 'var(--space-xs)',
                          }}
                        >
                          {(compareDraw.main.length ? compareDraw.main : []).map((n) => (
                            <span
                              key={n}
                              className="resultados-ball resultados-train-draw-ball"
                              style={{ width: 28, height: 28, fontSize: '0.8rem' }}
                            >
                              {String(n).padStart(2, '0')}
                            </span>
                          ))}
                        </div>
                        {slug === 'euromillones' ? (
                          <div
                            style={{
                              display: 'flex',
                              flexWrap: 'wrap',
                              gap: 6,
                              alignItems: 'center',
                            }}
                          >
                            {(compareDraw.stars.length ? compareDraw.stars : []).map((n) => (
                              <span
                                key={n}
                                className="resultados-ball-star-wrap"
                                style={{ display: 'inline-flex', alignItems: 'center', gap: 2 }}
                              >
                                <img
                                  src="/images/start.svg"
                                  alt=""
                                  className="resultados-star-img"
                                  aria-hidden
                                  style={{ width: 18, height: 18 }}
                                />
                                <span
                                  className="resultados-ball resultados-train-draw-ball"
                                  style={{ width: 28, height: 28, fontSize: '0.8rem' }}
                                >
                                  {String(n).padStart(2, '0')}
                                </span>
                              </span>
                            ))}
                          </div>
                        ) : (
                          <div
                            style={{
                              display: 'flex',
                              flexWrap: 'wrap',
                              gap: 6,
                              alignItems: 'center',
                            }}
                          >
                            {(compareDraw.stars.length ? compareDraw.stars : []).map((n) => (
                              <span
                                key={n}
                                className="resultados-ball resultados-train-draw-ball"
                                style={{ width: 28, height: 28, fontSize: '0.8rem' }}
                              >
                                {String(n).padStart(2, '0')}
                              </span>
                            ))}
                          </div>
                        )}
                      </div>

                      {compareProgress && (
                        (() => {
                          const pool = compareProgress.candidate_pool ?? [];
                          const limit = Math.min(comparePoolLimit, pool.length);
                          const mainSet = new Set(compareDraw.main.map(Number));
                          const starSet = new Set(compareDraw.stars.map(Number));
                          const escrutinio = Array.isArray(compareDraw.escrutinio)
                            ? (compareDraw.escrutinio as any[])
                            : [];
                          const prizeLookup = new Map<string, number>();
                          escrutinio.forEach((row: any) => {
                            const aciertos = String(
                              row.tipo ?? row.aciertos ?? row.categoria ?? '',
                            ).trim();
                            const m = aciertos.match(/(\d+)\s*\+\s*(\d+)/);
                            if (!m) return;
                            const hm = Number(m[1]);
                            const hs = Number(m[2]);
                            if (!Number.isFinite(hm) || !Number.isFinite(hs)) return;
                            const key = `${hm}-${hs}`;
                            const premio = parseEuroPremio(row.premio);
                            if (premio > 0) {
                              prizeLookup.set(key, premio);
                            }
                          });
                          let runningPrize = 0;
                          pool.slice(0, limit).forEach((t) => {
                            const anyTicket = t as any;
                            const mains = (anyTicket.mains ?? []).map(Number);
                            const starsOrClave = Array.isArray(anyTicket.stars)
                              ? anyTicket.stars.map(Number)
                              : anyTicket.clave != null
                              ? [Number(anyTicket.clave)]
                              : [];
                            const hitsMain = mains.filter((n: number) => mainSet.has(n)).length;
                            const hitsStar = starsOrClave.filter((n: number) => starSet.has(n)).length;
                            const prizePerTicket =
                              prizeLookup.get(`${hitsMain}-${hitsStar}`) ?? 0;
                            runningPrize += prizePerTicket;
                          });
                          const totalCost = limit * TICKET_BUDGET_EUR;
                          const totalPrize = runningPrize;
                          const totalEarning = totalPrize - totalCost;
                          return (
                            <div
                              className="euromillones-train-current-draw-card"
                              style={{ marginTop: 'var(--space-md)', padding: 'var(--space-md)' }}
                            >
                              <div
                                style={{
                                  fontSize: '0.75rem',
                                  color: 'var(--color-text-muted)',
                                  marginBottom: 'var(--space-xs)',
                                  textTransform: 'uppercase',
                                  letterSpacing: '0.04em',
                                }}
                              >
                                Resumen de {limit} boletos
                              </div>
                              <div style={{ fontSize: '0.9rem', display: 'grid', rowGap: 4 }}>
                                <div>
                                  <strong>Boletos seleccionados</strong> {limit}
                                </div>
                                <div>
                                  <strong>Coste total</strong> {totalCost.toFixed(2)} €
                                </div>
                                <div>
                                  <strong>Premio total</strong> {totalPrize.toFixed(2)} €
                                </div>
                                <div>
                                  <strong>Ganancia</strong>{' '}
                                  <span
                                    style={{
                                      color:
                                        totalEarning > 0
                                          ? 'var(--color-success, green)'
                                          : totalEarning < 0
                                          ? 'var(--color-error, #c00)'
                                          : 'inherit',
                                    }}
                                  >
                                    {totalEarning === 0
                                      ? '0.00 €'
                                      : `${totalEarning > 0 ? '+' : ''}${totalEarning.toFixed(
                                          2,
                                        )} €`}
                                  </span>
                                </div>
                              </div>
                            </div>
                          );
                        })()
                      )}
                    </>
                  )}
                </section>
              </div>
            )}
          </>
        )}

        {slug === 'euromillones' && view === 'pred' && (
          <section
            className="card resultados-features-card resultados-theme-euromillones"
            style={{ marginTop: 'var(--space-lg)', width: '100%' }}
          >
            <h3 style={{ marginTop: 0, marginBottom: '0.75rem' }}>
              Euromillones Feature Model
            </h3>
            {featureRowsLoading && <p style={{ margin: 0 }}>Cargando datos de euromillones_feature…</p>}
            {!featureRowsLoading && featureRowsError && (
              <p style={{ margin: 0, color: 'var(--color-error)' }}>{featureRowsError}</p>
            )}
            {!featureRowsLoading && !featureRowsError && featureRows.length === 0 && (
              <p style={{ margin: 0 }}>No hay filas en euromillones_feature.</p>
            )}
            {!featureRowsLoading && !featureRowsError && featureRows.length > 0 && (
              <div className="resultados-features-table-wrap">
                <table className="resultados-features-table">
                  <thead>
                    <tr>
                      <th>id_sorteo</th>
                      <th>pre_id_sorteo</th>
                      <th>fecha_sorteo</th>
                      <th>dia_semana</th>
                      <th>main_number</th>
                      <th>star_number</th>
                      <th>frequency (62)</th>
                      <th>gap (62)</th>
                      <th>presence_mask (62)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {featureRows.map((row) => (
                      <tr key={row.id_sorteo}>
                        <td>{row.id_sorteo}</td>
                        <td>{row.pre_id_sorteo ?? '—'}</td>
                        <td>{row.fecha_sorteo ?? '—'}</td>
                        <td>{row.dia_semana ?? '—'}</td>
                        <td>{(row.main_number ?? []).join(' - ')}</td>
                        <td>{(row.star_number ?? []).join(' - ')}</td>
                        <td>
                          <code style={{ fontSize: '0.72rem' }}>
                            {(row.frequency ?? []).slice(0, 20).join(',')}
                            {(row.frequency ?? []).length > 20 ? ', ...' : ''}
                          </code>
                        </td>
                        <td>
                          <code style={{ fontSize: '0.72rem' }}>
                            {(row.gap ?? []).slice(0, 20).map((v) => (v == null ? 'null' : String(v))).join(',')}
                            {(row.gap ?? []).length > 20 ? ', ...' : ''}
                          </code>
                        </td>
                        <td>
                          <code style={{ fontSize: '0.72rem' }}>
                            {(row.presence_mask ?? []).slice(0, 20).join(',')}
                            {(row.presence_mask ?? []).length > 20 ? ', ...' : ''}
                          </code>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}

        {view === 'sim' && (
          <>
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
                    Ejecuta los tres modelos para este sorteo y actualiza las probabilidades.
                  </p>
                </div>
                <button
                  type="button"
                  className="primary"
                  disabled={simLoading}
                  onClick={runAllSimulation}
                  style={{
                    minWidth: '9rem',
                  }}
                >
                  {simLoading ? 'Ejecutando…' : 'Simulación'}
                </button>
              </div>
              {simError && (
                <p style={{ color: 'var(--color-error)', marginTop: '0.5rem' }}>{simError}</p>
              )}
              {simResult && (
                <div className="resultados-features-table-wrap" style={{ marginTop: 'var(--space-md)' }}>
                  <div
                    style={{
                      display: 'grid',
                      gridTemplateColumns: '1fr 1fr',
                      gap: 'var(--space-lg)',
                    }}
                  >
                    <div>
                      <h4 style={{ marginTop: 0 }}>Todos los números principales</h4>
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
                            const getVal = (x: {
                              number: number;
                              freq?: number;
                              gap?: number;
                              hot?: number;
                            }) => {
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
                            <td>{m.freq != null ? (m.freq * 100).toFixed(1) + '%' : '—'}</td>
                            <td>{m.gap != null ? (m.gap * 100).toFixed(1) + '%' : '—'}</td>
                            <td>{m.hot != null ? (m.hot * 100).toFixed(1) + '%' : '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                      </table>
                    </div>
                    <div>
                      <h4 style={{ marginTop: 0 }}>Todas las estrellas</h4>
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
                            Estrella
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
                            const getVal = (x: {
                              number: number;
                              freq?: number;
                              gap?: number;
                              hot?: number;
                            }) => {
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
                            <td>{s.freq != null ? (s.freq * 100).toFixed(1) + '%' : '—'}</td>
                            <td>{s.gap != null ? (s.gap * 100).toFixed(1) + '%' : '—'}</td>
                            <td>{s.hot != null ? (s.hot * 100).toFixed(1) + '%' : '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}
            </section>
          </>
        )}

        {slug === 'euromillones' && view === 'pool' && (
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
                    Pool de candidatos
                  </h3>
                  <p style={{ margin: 0, fontSize: '0.9rem', color: '#4b5563' }}>
                    Ajusta el tamaño del pool y los pesos de cada modelo que se usarán en el
                    generador de combinaciones.
                  </p>
                </div>
                <button
                  type="button"
                  className="primary"
                  disabled={poolLoading}
                  onClick={buildCandidatePool}
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
                      Pool estrellas
                    </div>
                    <div
                      style={{
                        marginTop: '0.25rem',
                        fontSize: '0.9rem',
                        fontWeight: 500,
                        textAlign: 'right',
                      }}
                    >
                      Pool stars ({candidatePool.k_star}):{' '}
                      {(candidatePool.star_pool || []).join(' ')}
                    </div>
                  </div>
                  <button
                    type="button"
                    aria-label={poolExpanded ? 'Ocultar tabla de parámetros' : 'Mostrar tabla de parámetros'}
                    title={poolExpanded ? 'Ocultar tabla de parámetros' : 'Mostrar tabla de parámetros'}
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
                      <th>Números estrella</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>Tamaño del pool</td>
                      <td>
                        <input
                          type="number"
                          min={1}
                          max={50}
                          value={kMain}
                          onChange={handleIntChange(setKMain)}
                          className="form-input"
                        />
                      </td>
                      <td>
                        <input
                          type="number"
                          min={1}
                          max={12}
                          value={kStar}
                          onChange={handleIntChange(setKStar)}
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
                          value={starFreqW}
                          onChange={handleFloatChange(setStarFreqW)}
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
                          value={starGapW}
                          onChange={handleFloatChange(setStarGapW)}
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
                          value={starHotW}
                          onChange={handleFloatChange(setStarHotW)}
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

        {slug === 'euromillones' && view === 'wheel' && (
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
                <h3 style={{ marginTop: 0, marginBottom: '0.25rem' }}>Sistema Wheeling</h3>
                <p style={{ margin: 0, fontSize: '0.9rem', color: '#4b5563' }}>
                  Genera boletos de Euromillones a partir del pool de candidatos guardado.
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
              <div className="resultados-features-table-wrap" style={{ marginTop: 'var(--space-md)' }}>
                <table className="resultados-features-table">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>Números principales</th>
                      <th>Estrellas</th>
                    </tr>
                  </thead>
                  <tbody>
                    {wheelTickets.slice(0, wheelCount).map((t, idx) => (
                      <tr key={`${idx}-${t.mains.join('-')}-${t.stars.join('-')}`}>
                        <td>{idx + 1}</td>
                        <td>{t.mains.join(' ')}</td>
                        <td>{t.stars.join(' ')}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}
      </div>

      {slug === 'euromillones' && view !== 'compare' && (
        <aside
          className="resultados-page-sidebar resultados-theme-euromillones"
          style={{
            position: 'sticky',
            top: 'var(--space-md)',
          }}
        >
          {compareResult && (
            <>
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
                  Estrellas
                </p>
                <div className="resultados-balls">
                  {(compareResult.result_star_numbers || []).map((n: number, i: number) => (
                    <span key={i} className="resultados-ball-star-wrap" title="Estrella">
                      <img src="/images/start.svg" alt="" className="resultados-star-img" aria-hidden />
                      <span className="resultados-star-num">{String(n).padStart(2, '0')}</span>
                    </span>
                  ))}
                </div>
              </div>
              {(() => {
                const totalTickets = Number(compareResult.total_tickets) || 0;
                const totalReturn =
                  typeof compareResult.total_return === 'number'
                    ? compareResult.total_return
                    : 0;
                const cost = totalTickets * TICKET_BUDGET_EUR;
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
                      <strong style={{ color: earning >= 0 ? 'var(--color-success, green)' : 'var(--color-error, #c00)' }}>
                        {earning >= 0 ? '+' : ''}{earning.toFixed(2)} €
                      </strong>
                    </p>
                  </div>
                );
              })()}
            </>
          )}

          {/* La calidad de predicción (freq / gap / hot) ahora se muestra en la pestaña
              "Predicción vs resultado" en lugar de en la barra lateral. */}
        </aside>
      )}

      <Drawer
        title="Comparación Wheeling — gráfico por nº de boletos"
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

