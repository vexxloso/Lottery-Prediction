import { useCallback, useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { InputNumber, Modal, Spin, Tooltip, Pagination } from 'antd';

import { BuyQueueExportModal } from './BuyQueueExportModal';
import {
  buildExportTxtLines,
  downloadCsv,
  downloadTxt,
  exportFilenameBase,
  flattenLaPrimitivaQueue,
  openModernPrintView,
} from './buyQueueExport';
import {
  collectWheelPositionsFromBettingState,
  countWheelPositionsInRange,
  wheelPositionsInRange,
} from './rangeEnqueueExclude';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

const CANDIDATE_COUNT_OPTIONS = [100, 200, 300, 400, 500, 1000, 2000, 3000] as const;
const BUCKET_MAX = 8;
const REINTEGRO_OPTIONS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

function shuffleArray<T>(arr: T[]): T[] {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

/** Ticket with only mains (for candidate selection and bucket). */
type LaPrimitivaMains = { mains: number[]; position?: number };
/** Full ticket with reintegro (for saved/bought and API). */
type LaPrimitivaTicket = { mains: number[]; reintegro: number; position?: number };

function mainsKey(t: LaPrimitivaMains): string {
  return (t.mains ?? []).slice().sort((a, b) => a - b).join(',');
}

function RealPlatformIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6" />
      <polyline points="15 3 21 3 21 9" />
      <line x1="10" y1="14" x2="21" y2="3" />
    </svg>
  );
}

function DeleteIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
      <path d="M10 11v6M14 11v6M8 6v12M16 6v12" />
    </svg>
  );
}

function RepairIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M21 12a9 9 0 1 1-2.64-6.36" />
      <polyline points="21 3 21 9 15 9" />
    </svg>
  );
}

function ShuffleIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l5 5" />
    </svg>
  );
}

function QueueStatusIcon({ status }: { status: string }) {
  const s = status ?? '';
  if (s === 'waiting') {
    return (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
        <title>En cola</title>
        <circle cx="12" cy="12" r="10" />
        <path d="M12 6v6l4 2" />
      </svg>
    );
  }
  if (s === 'in_progress') {
    return (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
        <title>Comprando</title>
        <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83" />
      </svg>
    );
  }
  if (s === 'bought') {
    return (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
        <title>Comprado</title>
        <path d="M20 6L9 17l-5-5" />
      </svg>
    );
  }
  if (s === 'failed') {
    return (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
        <title>Error</title>
        <circle cx="12" cy="12" r="10" />
        <path d="M15 9l-6 6M9 9l6 6" />
      </svg>
    );
  }
  return null;
}

function TicketCardMains({
  ticket,
  onClick,
  onRemove,
  disabled,
  styleAnimationDelay,
  title: titleProp,
}: {
  ticket: LaPrimitivaMains;
  onClick?: () => void;
  onRemove?: () => void;
  disabled?: boolean;
  styleAnimationDelay?: number;
  title?: string;
}) {
  const mains = ticket.mains ?? [];
  return (
    <div className={`el-gordo-betting-ticket-card-wrap ${onRemove ? 'has-remove' : ''}`}>
      {onRemove && (
        <button
          type="button"
          className="el-gordo-betting-ticket-card-remove"
          onClick={(e) => { e.stopPropagation(); onRemove(); }}
          aria-label="Quitar"
        >
          ×
        </button>
      )}
      <div
        role={onClick ? 'button' : undefined}
        tabIndex={onClick ? 0 : undefined}
        className={`el-gordo-betting-ticket-card ${disabled ? 'el-gordo-betting-ticket-card--disabled' : ''}`}
        style={styleAnimationDelay != null ? { animationDelay: `${styleAnimationDelay}ms` } : undefined}
        onClick={onClick && !disabled ? onClick : undefined}
        onKeyDown={onClick && !disabled ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick(); } } : undefined}
        title={titleProp}
      >
        <div className="resultados-balls">
          {mains.map((n, i) => (
            <span key={i} className="resultados-ball">{n}</span>
          ))}
        </div>
      </div>
    </div>
  );
}

function TicketCardFull({
  ticket,
  onRemove,
}: {
  ticket: LaPrimitivaTicket;
  onRemove?: () => void;
}) {
  const mains = ticket.mains ?? [];
  return (
    <div className={`el-gordo-betting-ticket-card-wrap ${onRemove ? 'has-remove' : ''}`}>
      {onRemove && (
        <button
          type="button"
          className="el-gordo-betting-ticket-card-remove"
          onClick={(e) => { e.stopPropagation(); onRemove(); }}
          aria-label="Quitar"
        >
          ×
        </button>
      )}
      <div className="el-gordo-betting-ticket-card">
        <div className="resultados-balls">
          {mains.map((n, i) => (
            <span key={i} className="resultados-ball">{n}</span>
          ))}
          <span className="resultados-ball reintegro">{ticket.reintegro}</span>
        </div>
      </div>
    </div>
  );
}

export function LaPrimitivaBettingPanel() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [candidatePool, setCandidatePool] = useState<LaPrimitivaTicket[]>([]);
  const [bucket, setBucket] = useState<LaPrimitivaMains[]>([]);
  const [realPool, setRealPool] = useState<LaPrimitivaTicket[]>([]);
  const [candidateCount, setCandidateCount] = useState(100);
  const [totalTickets, setTotalTickets] = useState(0);
  const [page, setPage] = useState(1);
  const [searchParams] = useSearchParams();
  const drawDate = searchParams.get('draw_date') ?? '';
  const cutoffDrawId = searchParams.get('cutoff_draw_id') ?? '';

  const fetchBettingPool = useCallback(async (showLoading = true) => {
    if (showLoading) {
      setError('');
    }
    try {
      const params = new URLSearchParams();
      if (drawDate) params.set('draw_date', drawDate);
      else if (cutoffDrawId) params.set('cutoff_draw_id', cutoffDrawId);
      const url = params.toString()
        ? `${API_URL}/api/la-primitiva/betting/pool?${params.toString()}`
        : `${API_URL}/api/la-primitiva/betting/pool`;
      const res = await fetch(url, { cache: 'no-store' });
      const data = await res.json();
      if (!res.ok) {
        if (showLoading) {
          setError(data.detail ?? res.statusText ?? 'Error al cargar pool');
        }
        return;
      }
      const rawBought = Array.isArray(data.bought_tickets) ? data.bought_tickets : [];
      setRealPool(
        rawBought.map((t: { mains?: unknown; reintegro?: unknown; position?: unknown }) => {
          const row: LaPrimitivaTicket = {
            mains: Array.isArray(t.mains) ? t.mains.map(Number) : [],
            reintegro: typeof t.reintegro === 'number' ? t.reintegro : Number(t.reintegro) || 0,
          };
          const p = t.position;
          if (typeof p === 'number' && Number.isFinite(p) && p >= 1) row.position = Math.floor(p);
          return row;
        }),
      );
    } catch (e) {
      if (showLoading) {
        const msg = e instanceof Error ? e.message : 'Error al cargar pool';
        setError(
          msg.includes('fetch') || msg.includes('Failed')
            ? 'Error de conexión con el servidor. Comprueba que el backend esté en marcha (puerto 8000).'
            : msg,
        );
      }
    }
  }, [drawDate, cutoffDrawId]);

  const fetchCandidatePage = useCallback(async (showLoading = true) => {
    if (showLoading) {
      setLoading(true);
      setError('');
    }
    try {
      const params = new URLSearchParams();
      if (drawDate) params.set('draw_date', drawDate);
      else if (cutoffDrawId) params.set('cutoff_draw_id', cutoffDrawId);
      const skip = (page - 1) * candidateCount;
      params.set('skip', String(skip));
      params.set('limit', String(candidateCount));
      const url = `${API_URL}/api/la-primitiva/betting/pool-from-file?${params.toString()}`;
      const res = await fetch(url, { cache: 'no-store' });
      const data = await res.json();
      if (!res.ok) {
        if (showLoading) {
          setError(
            data.detail ?? res.statusText ?? 'Error al cargar pool de candidatos',
          );
          setCandidatePool([]);
          setTotalTickets(0);
        }
        return;
      }
      const tickets = Array.isArray(data.tickets) ? data.tickets : [];
      setCandidatePool(
        tickets.map((t: { mains?: unknown; position?: unknown }) => {
          const row: LaPrimitivaTicket = {
            mains: Array.isArray(t.mains) ? t.mains.map(Number) : [],
            reintegro: 0,
          };
          const p = t.position;
          if (typeof p === 'number' && Number.isFinite(p) && p >= 1) row.position = Math.floor(p);
          return row;
        }),
      );
      setTotalTickets(typeof data.total === 'number' ? data.total : 0);
    } catch (e) {
      if (showLoading) {
        const msg = e instanceof Error ? e.message : 'Error al cargar pool de candidatos';
        setError(msg);
        setCandidatePool([]);
        setTotalTickets(0);
      }
    } finally {
      if (showLoading) setLoading(false);
    }
  }, [drawDate, cutoffDrawId, page, candidateCount]);

  useEffect(() => {
    const run = async () => {
      setLoading(true);
      setError('');
      await fetchBettingPool(false);
      await fetchCandidatePage(false);
      setLoading(false);
    };
    run();
  }, [fetchBettingPool, fetchCandidatePage]);

  const addToBucket = (ticket: LaPrimitivaTicket) => {
    if (bucket.length >= BUCKET_MAX) return;
    const key = mainsKey(ticket);
    setBucket((prev) => {
      if (prev.some((t) => mainsKey(t) === key)) return prev; // already in bucket
      const next: LaPrimitivaMains = { mains: [...(ticket.mains ?? [])] };
      if (typeof ticket.position === 'number' && Number.isFinite(ticket.position) && ticket.position >= 1) {
        next.position = Math.floor(ticket.position);
      }
      return [...prev, next];
    });
  };

  const removeFromBucket = (index: number) => {
    setBucket((prev) => prev.filter((_, i) => i !== index));
  };

  const [enqueueLoading, setEnqueueLoading] = useState(false);
  const [reintegroModalOpen, setReintegroModalOpen] = useState(false);
  const [buyQueue, setBuyQueue] = useState<
    { id: string; status: string; tickets_count: number; tickets?: { mains?: number[]; reintegro?: number; position?: number }[]; draw_date?: string; error?: string }[]
  >([]);
  const [countModalOpen, setCountModalOpen] = useState(false);
  const [countInput, setCountInput] = useState<number>(100);
  const [enqueueByCountLoading, setEnqueueByCountLoading] = useState(false);

  const [rangeModalOpen, setRangeModalOpen] = useState(false);
  const [rangeStart, setRangeStart] = useState<number>(1);
  const [rangeEnd, setRangeEnd] = useState<number>(2);
  const [enqueueByRangeLoading, setEnqueueByRangeLoading] = useState(false);
  const [exportModalOpen, setExportModalOpen] = useState(false);
  const [deleteAllWaitingLoading, setDeleteAllWaitingLoading] = useState(false);
  const [queueLastDrawDate, setQueueLastDrawDate] = useState('');

  const fetchBuyQueue = useCallback(async () => {
    try {
      // Backend validates limit (see api_la_primitiva_betting_buy_queue),
      // so we request a large value to show all queued items.
      const res = await fetch(`${API_URL}/api/la-primitiva/betting/buy-queue?limit=5000`, { cache: 'no-store' });
      const data = await res.json();
      setBuyQueue(Array.isArray(data.items) ? data.items : []);
      setQueueLastDrawDate(typeof data.last_draw_date === 'string' ? data.last_draw_date.slice(0, 10) : '');
    } catch {
      setBuyQueue([]);
      setQueueLastDrawDate('');
    }
  }, []);

  const visibleBuyQueue = buyQueue;

  const queueTicketsFlatCount = useMemo(
    () => visibleBuyQueue.reduce((n, q) => n + (Array.isArray(q.tickets) ? q.tickets.length : 0), 0),
    [visibleBuyQueue],
  );

  const waitingQueueBatchCount = useMemo(
    () => visibleBuyQueue.filter((q) => q?.status === 'waiting').length,
    [visibleBuyQueue],
  );

  const wheelPositionsOccupied = useMemo(
    () => collectWheelPositionsFromBettingState(visibleBuyQueue, bucket, realPool),
    [visibleBuyQueue, bucket, realPool],
  );

  const rangeSkippedByOccupiedCount = useMemo(
    () => countWheelPositionsInRange(wheelPositionsOccupied, rangeStart, rangeEnd),
    [wheelPositionsOccupied, rangeStart, rangeEnd],
  );

  const queueSliceByTicketCount = useCallback((selection: { queueCount: number }) => {
    const qCount = Math.max(0, Math.floor(selection.queueCount));
    return visibleBuyQueue.slice(0, qCount);
  }, [visibleBuyQueue]);

  const handleExportLaPrimitivaCsv = useCallback((selection: { queueCount: number; requestedTickets: number; selectedTickets: number }) => {
    const queueSlice = queueSliceByTicketCount(selection);
    const { headers, rows } = flattenLaPrimitivaQueue(queueSlice);
    if (rows.length === 0) {
      setError('No hay boletos en la cola para exportar.');
      return;
    }
    downloadCsv(`${exportFilenameBase('la-primitiva')}.csv`, headers, rows);
  }, [queueSliceByTicketCount]);

  const handleExportLaPrimitivaTxt = useCallback((selection: { queueCount: number; requestedTickets: number; selectedTickets: number }) => {
    const queueSlice = queueSliceByTicketCount(selection);
    const { headers, rows } = flattenLaPrimitivaQueue(queueSlice);
    if (rows.length === 0) {
      setError('No hay boletos en la cola para exportar.');
      return;
    }
    downloadTxt(
      `${exportFilenameBase('la-primitiva')}.txt`,
      buildExportTxtLines('La Primitiva — Cola de compra', headers, rows),
    );
  }, [queueSliceByTicketCount]);

  const handleExportLaPrimitivaPdf = useCallback(async (printTab: Window | null, selection: { queueCount: number; requestedTickets: number; selectedTickets: number }) => {
    const queueSlice = queueSliceByTicketCount(selection);
    const { headers, rows } = flattenLaPrimitivaQueue(queueSlice);
    if (rows.length === 0) {
      printTab?.close();
      setError('No hay boletos en la cola para exportar.');
      return;
    }
    setError('');
    try {
      const queueIds = queueSlice.map((q) => q?.id).filter((id): id is string => typeof id === 'string' && id !== '');
      const res = await fetch(`${API_URL}/api/la-primitiva/betting/save-queue-after-print`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ queue_ids: queueIds }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        printTab?.close();
        setError(typeof data.detail === 'string' ? data.detail : 'No se pudo guardar en boletos comprados.');
        return;
      }
      await fetchBuyQueue();
      fetchBettingPool(false);
      requestAnimationFrame(() => {
        document.getElementById('la-primitiva-boletos-guardados')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
    } catch (e) {
      printTab?.close();
      setError(e instanceof Error ? e.message : 'Error de red al guardar.');
      return;
    }
    const ok = openModernPrintView(
      {
        title: 'La Primitiva — Cola de compra',
        subtitle: `Generado: ${new Date().toLocaleString('es-ES')}`,
        columns: headers,
        rows,
      },
      printTab,
    );
    if (!ok) setError('No se pudo abrir la ventana. Permite ventanas emergentes.');
  }, [queueSliceByTicketCount, fetchBuyQueue, fetchBettingPool]);

  const saveBoughtFromQueue = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/api/la-primitiva/betting/save-bought-from-queue`, { method: 'POST' });
      if (res.ok) {
        const data = await res.json().catch(() => ({}));
        if ((data.saved_count ?? 0) > 0) fetchBettingPool(false);
      }
    } catch {
      // ignore
    }
  }, [fetchBettingPool]);

  useEffect(() => {
    fetchBuyQueue();
    saveBoughtFromQueue();
    const intervalMs = 8000;
    const t = setInterval(() => {
      // Just refresh queue status periodically so the user sees progress.
      // Saving bought tickets and refreshing the bought pool is done on demand
      // (on mount and after an explicit save) to avoid constant background writes.
      fetchBuyQueue();
    }, intervalMs);
    return () => clearInterval(t);
  }, [fetchBuyQueue, fetchBettingPool, saveBoughtFromQueue]);

  const enqueueBuy = async (reintegro: number) => {
    if (bucket.length === 0) return;
    setReintegroModalOpen(false);
    setEnqueueLoading(true);
    setError('');
    try {
      const body: {
        tickets: { mains: number[]; reintegro: number; position?: number }[];
        draw_date?: string;
        cutoff_draw_id?: string;
      } = {
        tickets: bucket.map((m) => {
          const row: { mains: number[]; reintegro: number; position?: number } = { mains: m.mains, reintegro };
          if (typeof m.position === 'number' && Number.isFinite(m.position) && m.position >= 1) {
            row.position = Math.floor(m.position);
          }
          return row;
        }),
      };
      if (drawDate) body.draw_date = drawDate;
      else if (cutoffDrawId) body.cutoff_draw_id = cutoffDrawId;
      const res = await fetch(`${API_URL}/api/la-primitiva/betting/enqueue`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.detail ?? res.statusText ?? 'Error al encolar');
      } else {
        setBucket([]);
        fetchBuyQueue();
        fetchBettingPool(false);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al encolar');
    } finally {
      setEnqueueLoading(false);
    }
  };

  const openEnqueueReintegroModal = () => {
    if (bucket.length === 0) return;
    setReintegroModalOpen(true);
  };

  const enqueueByCount = async () => {
    const count = Number(countInput);
    if (!Number.isInteger(count) || count < 1) return;
    setEnqueueByCountLoading(true);
    setError('');
    try {
      const body: { count: number; draw_date?: string; cutoff_draw_id?: string } = { count };
      if (drawDate) body.draw_date = drawDate;
      if (cutoffDrawId) body.cutoff_draw_id = cutoffDrawId;
      const res = await fetch(`${API_URL}/api/la-primitiva/betting/enqueue-by-count`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError(data.detail ?? res.statusText ?? 'Error al encolar por cantidad');
        return;
      }
      setCountModalOpen(false);
      await fetchBuyQueue();
      setError('');
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al encolar por cantidad');
    } finally {
      setEnqueueByCountLoading(false);
    }
  };

  const enqueueByRange = async () => {
    if (totalTickets <= 0) return;

    if (!Number.isInteger(rangeStart) || !Number.isInteger(rangeEnd)) {
      setError('start y end deben ser números enteros');
      return;
    }
    if (rangeStart < 1) {
      setError('start_position debe ser >= 1');
      return;
    }
    if (rangeEnd < rangeStart) {
      setError('end_position debe ser >= start_position');
      return;
    }
    if (rangeStart > totalTickets) {
      setError(`start_position debe ser <= total tickets (${totalTickets})`);
      return;
    }
    if (rangeEnd > totalTickets) {
      setError(`end_position debe ser <= total tickets (${totalTickets})`);
      return;
    }

    setEnqueueByRangeLoading(true);
    setError('');
    try {
      const body: {
        start_position: number;
        end_position: number;
        draw_date?: string;
        cutoff_draw_id?: string;
        exclude_positions: number[];
      } = {
        start_position: rangeStart,
        end_position: rangeEnd,
        exclude_positions: wheelPositionsInRange(wheelPositionsOccupied, rangeStart, rangeEnd),
      };
      if (drawDate) body.draw_date = drawDate;
      else if (cutoffDrawId) body.cutoff_draw_id = cutoffDrawId;

      const res = await fetch(`${API_URL}/api/la-primitiva/betting/enqueue-by-range`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError(data.detail ?? res.statusText ?? 'Error al encolar por rango');
        return;
      }

      setRangeModalOpen(false);
      await fetchBuyQueue();
      fetchBettingPool(false);
      setError('');
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al encolar por rango');
    } finally {
      setEnqueueByRangeLoading(false);
    }
  };

  const bucketFull = bucket.length >= BUCKET_MAX;
  const savedMainsKeys = new Set(realPool.map((t) => mainsKey(t)));
  const bucketMainsKeys = new Set(bucket.map(mainsKey));
  const inQueue = new Set<string>();
  for (const q of visibleBuyQueue) {
    const tickets = q?.tickets;
    if (Array.isArray(tickets)) for (const t of tickets) inQueue.add(mainsKey({ mains: t.mains ?? [] }));
  }
  const inBucketOrReal = new Set([...bucketMainsKeys, ...savedMainsKeys]);
  const availableCandidates = candidatePool.filter((t) => !inBucketOrReal.has(mainsKey(t)) && !inQueue.has(mainsKey(t)));
  const addRandomToBucket = () => {
    const need = Math.min(BUCKET_MAX - bucket.length, availableCandidates.length);
    if (need <= 0) return;
    const picked = shuffleArray(availableCandidates).slice(0, need).map((t) => {
      const row: LaPrimitivaMains = { mains: [...(t.mains ?? [])] };
      if (typeof t.position === 'number' && Number.isFinite(t.position) && t.position >= 1) row.position = Math.floor(t.position);
      return row;
    });
    setBucket((prev) => [...prev, ...picked]);
  };
  const disabledReason = (t: LaPrimitivaMains): string => {
    const key = mainsKey(t);
    if (bucketMainsKeys.has(key)) return 'En cesta';
    if (savedMainsKeys.has(key)) return 'Guardado';
    if (inQueue.has(key)) return 'En cola';
    return '';
  };
  const displayedCandidates = candidatePool;

  if (loading) {
    return (
      <section className="card resultados-features-card resultados-theme-la-primitiva el-gordo-betting">
        <Spin size="small" />
      </section>
    );
  }

  return (
    <section className="card resultados-features-card resultados-theme-la-primitiva el-gordo-betting">
      <h2 style={{ marginTop: 0, marginBottom: 'var(--space-sm)', fontSize: '1rem' }}>
        La Primitiva — Apuestas
      </h2>
      {error && (
        <p style={{ color: 'var(--color-error)', marginBottom: 'var(--space-md)' }}>{error}</p>
      )}

      <Modal
        title="Comprar por cantidad"
        open={countModalOpen}
        onCancel={() => !enqueueByCountLoading && setCountModalOpen(false)}
        onOk={() => enqueueByCount()}
        okText="Confirmar"
        cancelText="Cancelar"
        confirmLoading={enqueueByCountLoading}
        destroyOnClose
      >
        <p style={{ marginBottom: 8 }}>Número de boletos a encolar (se crearán colas de {BUCKET_MAX} boletos cada una):</p>
        <InputNumber
          min={1}
          max={1000000}
          value={countInput}
          onChange={(v) => setCountInput(v ?? 1)}
          style={{ width: '100%' }}
        />
        <p style={{ marginTop: 12, fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>
          Se crearán {Math.ceil((countInput ?? 0) / BUCKET_MAX)} colas de compra (máx. {BUCKET_MAX} boletos por cola). Cada cola tendrá un reintegro aleatorio (0–9).
        </p>
      </Modal>

      <Modal
        title="Comprar por rango"
        open={rangeModalOpen}
        onCancel={() => !enqueueByRangeLoading && setRangeModalOpen(false)}
        onOk={() => enqueueByRange()}
        okText="Confirmar"
        cancelText="Cancelar"
        confirmLoading={enqueueByRangeLoading}
        destroyOnClose
      >
        <p style={{ marginBottom: 8, fontSize: '0.9rem' }}>
          Se encolarán boletos desde <strong>start_position</strong> hasta <strong>end_position</strong> (ambos inclusive).
        </p>
        <p style={{ marginBottom: 10, fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>
          No se incluyen líneas ya en <strong>cesta</strong>, <strong>cola</strong> o <strong>guardados</strong> (por posición del
          bombo). En este rango hay <strong>{rangeSkippedByOccupiedCount}</strong> posición(es) ocupada(s) que se omiten.
        </p>
        <p style={{ marginBottom: 6, color: 'var(--color-text-muted)' }}>start_position</p>
        <InputNumber
          min={1}
          max={Math.max(totalTickets, 1)}
          value={rangeStart}
          onChange={(v) => setRangeStart(v ?? 1)}
          style={{ width: '100%', marginBottom: 10 }}
        />
        <p style={{ marginBottom: 6, color: 'var(--color-text-muted)' }}>end_position</p>
        <InputNumber
          min={1}
          max={Math.max(totalTickets, 1)}
          value={rangeEnd}
          onChange={(v) => setRangeEnd(v ?? 1)}
          style={{ width: '100%' }}
        />
        <p style={{ marginTop: 12, fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>
          Se crearán colas por el total de boletos disponibles en el rango (ya excluye los que están en cola o comprados).
        </p>
      </Modal>

      {reintegroModalOpen && (
        <div className="el-gordo-betting-reintegro-overlay" role="dialog" aria-modal="true" aria-label="Elegir reintegro">
          <div className="el-gordo-betting-reintegro-modal">
            <h3>Elige reintegro (0-9)</h3>
            <p style={{ fontSize: '0.9rem', color: 'var(--color-text-muted)', marginBottom: 12 }}>
              Los {bucket.length} boletos de la cesta tendrán el mismo reintegro. Luego se añadirán a la cola de compra.
            </p>
            <div className="el-gordo-betting-reintegro-buttons">
              {REINTEGRO_OPTIONS.map((r) => (
                <button
                  key={r}
                  type="button"
                  className="resultados-ball reintegro el-gordo-betting-reintegro-btn"
                  onClick={() => enqueueBuy(r)}
                  disabled={enqueueLoading}
                >
                  {r}
                </button>
              ))}
            </div>
            <button
              type="button"
              className="resultados-features-iconbtn"
              onClick={() => setReintegroModalOpen(false)}
              style={{ marginTop: 12 }}
              disabled={enqueueLoading}
            >
              Cancelar
            </button>
          </div>
        </div>
      )}

      <div className="el-gordo-betting-split">
        <div className="el-gordo-betting-left">
          <div className="el-gordo-betting-panel-card" style={{ flex: 1, minHeight: 280 }}>
            <div className="el-gordo-betting-candidate-header">
              <h3>Candidatos — clic para añadir a la cesta (solo números principales)</h3>
              <div className="el-gordo-betting-candidate-actions">
                <button
                  type="button"
                  className="el-gordo-betting-random-btn"
                  onClick={() => setCountModalOpen(true)}
                  title="Crear colas por número de boletos (se dividen en colas de 8)"
                >
                  Comprar por cantidad
                </button>
                <button
                  type="button"
                  className="el-gordo-betting-random-btn"
                  onClick={() => setRangeModalOpen(true)}
                  disabled={totalTickets <= 0}
                  title="Comprar por rango de posiciones (start y end inclusive)"
                >
                  Comprar por rango
                </button>
                <button
                  type="button"
                  className="el-gordo-betting-random-btn"
                  onClick={addRandomToBucket}
                  disabled={bucketFull || availableCandidates.length === 0}
                  title="Add random tickets from full candidate pool (no duplicates)"
                >
                  <ShuffleIcon />
                  <span>Selección aleatoria</span>
                </button>
                <label className="el-gordo-betting-count-select">
                  <span>Mostrar</span>
                  <select
                    value={candidateCount}
                    onChange={(e) => {
                      setCandidateCount(Number(e.target.value));
                      setPage(1);
                    }}
                    aria-label="Número de candidatos por página"
                  >
                    {CANDIDATE_COUNT_OPTIONS.map((n) => (
                      <option key={n} value={n}>{n}</option>
                    ))}
                  </select>
                </label>
              </div>
            </div>
            <p style={{ margin: '0 0 var(--space-sm)', fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
              {totalTickets} boletos en full wheel · página {page} · {availableCandidates.length} disponibles (clic para añadir) · máx. {BUCKET_MAX} en la cesta
            </p>
            <Pagination
              size="small"
              current={page}
              pageSize={candidateCount}
              total={totalTickets}
              onChange={(p) => setPage(p)}
              showSizeChanger={false}
              style={{ marginBottom: 'var(--space-sm)' }}
            />
            <div className="el-gordo-betting-gallery">
              {displayedCandidates.length === 0 ? (
                <p style={{ margin: 'auto', fontSize: '0.85rem', color: 'var(--color-text-muted)', gridColumn: '1 / -1' }}>
                  {!drawDate && !cutoffDrawId
                    ? 'Se abre con la fecha del último sorteo (draw_date). Si no hay datos, ejecuta en Predicción el paso de pool de candidatos.'
                    : 'No hay pool. En Predicción ejecuta todos los pasos hasta generar el pool de candidatos para este sorteo.'}
                </p>
              ) : (
                displayedCandidates.map((t, i) => {
                  const reason = disabledReason(t);
                  const disabled = bucketFull || !!reason;
                  return (
                    <TicketCardMains
                      key={`c-${i}-${mainsKey(t)}`}
                      ticket={t}
                      onClick={() => addToBucket(t)}
                      disabled={disabled}
                      styleAnimationDelay={i < 40 ? i * 25 : undefined}
                      title={reason || undefined}
                    />
                  );
                })
              )}
            </div>
          </div>
        </div>

        <div className="el-gordo-betting-right">
          {visibleBuyQueue.length > 0 && (
            <div style={{ marginBottom: 'var(--space-sm)', fontSize: '0.8rem', maxHeight: 220, overflowY: 'auto' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, flexWrap: 'wrap' }}>
                <strong>
                  Cola de compra
                  {queueLastDrawDate ? ` (${queueLastDrawDate})` : ''}
                </strong>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                  <button
                    type="button"
                    className="el-gordo-betting-btn-text"
                    disabled={queueTicketsFlatCount === 0}
                    onClick={() => setExportModalOpen(true)}
                    title="Exportar cola (CSV o PDF / imprimir)"
                  >
                    Exportar cola
                  </button>
                  <button
                    type="button"
                    className="el-gordo-betting-btn-icon"
                    disabled={waitingQueueBatchCount === 0 || deleteAllWaitingLoading}
                    onClick={async () => {
                      setDeleteAllWaitingLoading(true);
                      try {
                        const drawDateQuery = queueLastDrawDate ? `?draw_date=${encodeURIComponent(queueLastDrawDate)}` : '';
                        const res = await fetch(`${API_URL}/api/la-primitiva/betting/buy-queue/waiting${drawDateQuery}`, {
                          method: 'DELETE',
                        });
                        if (res.ok) fetchBuyQueue();
                        else {
                          const data = await res.json().catch(() => ({}));
                          setError(data.detail ?? 'Error al vaciar la cola (en espera)');
                        }
                      } catch (e) {
                        setError(e instanceof Error ? e.message : 'Error al vaciar la cola (en espera)');
                      } finally {
                        setDeleteAllWaitingLoading(false);
                      }
                    }}
                    aria-label="Quitar todos los que están en cola"
                    title="Quitar todos los que están «En cola» (no borra comprados, en curso ni errores)"
                  >
                    <DeleteIcon />
                  </button>
                </div>
              </div>
              <ul style={{ margin: '4px 0 0', paddingLeft: '1.2rem', listStyle: 'none' }}>
                {visibleBuyQueue.filter((q) => q != null).map((q, idx) => (
                  <li key={q?.id ?? `q-${idx}`} style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 2 }}>
                    <Tooltip
                      title={
                        Array.isArray(q?.tickets) && q.tickets.length > 0 ? (
                          <div style={{ padding: '4px 0', lineHeight: 1.5 }}>
                            {q.tickets.map((t, i) => (
                              <div key={i} style={{ marginBottom: i < (q?.tickets?.length ?? 0) - 1 ? 4 : 0 }}>
                                Boleto {i + 1}: {(t.mains ?? []).join(', ')}
                                {typeof (t as { reintegro?: number }).reintegro === 'number' ? ` — Reintegro ${(t as { reintegro?: number }).reintegro}` : ''}
                              </div>
                            ))}
                          </div>
                        ) : (
                          <span>{q?.tickets_count ?? 0} boleto(s)</span>
                        )
                      }
                      placement="topLeft"
                    >
                      <span style={{ display: 'flex', alignItems: 'center', gap: 4, flex: 1, minWidth: 0, cursor: 'default' }}>
                        <span style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 }} aria-hidden>
                          <QueueStatusIcon status={q?.status ?? ''} />
                        </span>
                        <span style={{ flex: 1, minWidth: 0, color: q?.status === 'failed' ? 'var(--color-error)' : undefined }}>
                          {q?.tickets_count ?? 0} boleto{(q?.tickets_count ?? 0) !== 1 ? 's' : ''} — {q?.status === 'waiting' ? 'En cola' : q?.status === 'in_progress' ? 'Comprando…' : q?.status === 'bought' ? 'Comprado' : 'Error'}
                        </span>
                      </span>
                    </Tooltip>
                    {(q?.status === 'in_progress' || q?.status === 'failed') && q?.id ? (
                      <button
                        type="button"
                        onClick={async () => {
                          try {
                            const res = await fetch(`${API_URL}/api/la-primitiva/betting/buy-queue/${encodeURIComponent(q.id)}/repair`, { method: 'POST' });
                            if (res.ok) fetchBuyQueue();
                            else {
                              const data = await res.json().catch(() => ({}));
                              setError(data.detail ?? 'Error al reparar');
                            }
                          } catch (e) {
                            setError(e instanceof Error ? e.message : 'Error al reparar');
                          }
                        }}
                        aria-label="Reparar cola"
                        title="Reparar: volver a En cola para reintentar"
                        style={{ padding: 2, border: 'none', background: 'transparent', cursor: 'pointer', color: 'var(--color-text-muted)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                      >
                        <RepairIcon />
                      </button>
                    ) : null}
                    {(q?.status === 'waiting' || q?.status === 'failed') && q?.id ? (
                      <button
                        type="button"
                        onClick={async () => {
                          try {
                            const res = await fetch(`${API_URL}/api/la-primitiva/betting/buy-queue/${encodeURIComponent(q.id)}`, { method: 'DELETE' });
                            if (res.ok) fetchBuyQueue();
                            else {
                              const data = await res.json().catch(() => ({}));
                              setError(data.detail ?? 'Error al eliminar');
                            }
                          } catch (e) {
                            setError(e instanceof Error ? e.message : 'Error al eliminar');
                          }
                        }}
                        aria-label="Quitar de la cola"
                        title="Quitar de la cola"
                        style={{ padding: 2, border: 'none', background: 'transparent', cursor: 'pointer', color: 'var(--color-text-muted)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                      >
                        <DeleteIcon />
                      </button>
                    ) : null}
                  </li>
                ))}
              </ul>
            </div>
          )}
          <div className="el-gordo-betting-panel-card">
            <div className="el-gordo-betting-bucket-header">
              <h3>Cesta ({bucket.length}/{BUCKET_MAX})</h3>
              <div className="el-gordo-betting-bucket-toolbar">
                <button
                  type="button"
                  className="el-gordo-betting-btn-icon"
                  disabled={bucket.length === 0 || enqueueLoading}
                  onClick={openEnqueueReintegroModal}
                  aria-label="Comprar en Loterías — elegir reintegro y añadir a la cola"
                  title="Comprar en Loterías — elegir reintegro y añadir a la cola"
                >
                  <RealPlatformIcon />
                </button>
                <button
                  type="button"
                  className="el-gordo-betting-btn-icon"
                  disabled={bucket.length === 0}
                  onClick={() => setBucket([])}
                  aria-label="Vaciar cesta"
                  title="Vaciar cesta"
                >
                  <DeleteIcon />
                </button>
              </div>
            </div>
            <div className="el-gordo-betting-gallery el-gordo-betting-gallery--compact">
              {bucket.length === 0 ? (
                <p style={{ margin: 'auto', fontSize: '0.8rem', color: 'var(--color-text-muted)', gridColumn: '1 / -1' }}>
                  Vacío. Haz clic en un candidato para añadir. Haz clic en un boleto de la cesta para quitarlo.
                </p>
              ) : (
                bucket.map((m, i) => (
                  <TicketCardMains
                    key={`b-${i}-${mainsKey(m)}`}
                    ticket={m}
                    onClick={() => removeFromBucket(i)}
                    onRemove={() => removeFromBucket(i)}
                  />
                ))
              )}
            </div>
          </div>

          <div
            id="la-primitiva-boletos-guardados"
            className="el-gordo-betting-panel-card"
            style={{ flex: 1, minHeight: 180 }}
          >
            <h3>Boletos guardados</h3>
            <p style={{ margin: '0 0 var(--space-sm)', fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
              {realPool.length} boleto{realPool.length !== 1 ? 's' : ''} guardado{realPool.length !== 1 ? 's' : ''}
            </p>
            <div className="el-gordo-betting-gallery el-gordo-betting-gallery--compact el-gordo-betting-gallery--start">
              {realPool.length === 0 ? (
                <p style={{ margin: 'auto', fontSize: '0.8rem', color: 'var(--color-text-muted)', gridColumn: '1 / -1' }}>
                  Vacío.
                </p>
              ) : (
                realPool.map((t, i) => (
                  <TicketCardFull key={`r-${i}`} ticket={t} />
                ))
              )}
            </div>
          </div>
        </div>
      </div>
      <BuyQueueExportModal
        open={exportModalOpen}
        onCancel={() => setExportModalOpen(false)}
        lotteryTitle="La Primitiva"
        disabled={queueTicketsFlatCount === 0}
        queueTicketCounts={visibleBuyQueue.map((q) => (Array.isArray(q?.tickets) ? q.tickets.length : (q?.tickets_count ?? 0)))}
        onExportCsv={handleExportLaPrimitivaCsv}
        onExportTxt={handleExportLaPrimitivaTxt}
        onExportPdf={handleExportLaPrimitivaPdf}
      />
    </section>
  );
}
