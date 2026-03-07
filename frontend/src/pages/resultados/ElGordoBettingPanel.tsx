import { useCallback, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Spin } from 'antd';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

const CANDIDATE_COUNT_OPTIONS = [100, 200, 300, 400, 500, 1000, 2000, 3000] as const;
const BUCKET_MAX = 6;

function shuffleArray<T>(arr: T[]): T[] {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

type ElGordoTicket = { mains: number[]; clave: number };

function ticketKey(t: ElGordoTicket): string {
  return `${(t.mains ?? []).join(',')}|${t.clave}`;
}

function DeleteIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
      <path d="M10 11v6M14 11v6M8 6v12M16 6v12" />
    </svg>
  );
}

function BuyIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M20 6L9 17l-5-5" />
    </svg>
  );
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

function ShuffleIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l5 5" />
    </svg>
  );
}

function TicketCard({
  ticket,
  onClick,
  onRemove,
  disabled,
  styleAnimationDelay,
}: {
  ticket: ElGordoTicket;
  onClick?: () => void;
  onRemove?: () => void;
  disabled?: boolean;
  styleAnimationDelay?: number;
}) {
  const mains = ticket.mains ?? [];
  return (
    <div className={`el-gordo-betting-ticket-card-wrap ${onRemove ? 'has-remove' : ''}`}>
      {onRemove && (
        <button
          type="button"
          className="el-gordo-betting-ticket-card-remove"
          onClick={(e) => {
            e.stopPropagation();
            onRemove();
          }}
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
        onKeyDown={
          onClick && !disabled
            ? (e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  onClick();
                }
              }
            : undefined
        }
      >
        <div className="resultados-balls">
          {mains.map((n, i) => (
            <span key={i} className="resultados-ball">
              {n}
            </span>
          ))}
          <span className="resultados-ball clave">{ticket.clave}</span>
        </div>
      </div>
    </div>
  );
}

export function ElGordoBettingPanel() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [_lastDrawDate, setLastDrawDate] = useState<string | null>(null);
  const [candidatePool, setCandidatePool] = useState<ElGordoTicket[]>([]);
  const [bucket, setBucket] = useState<ElGordoTicket[]>([]);
  const [realPool, setRealPool] = useState<ElGordoTicket[]>([]);
  const [candidateCount, setCandidateCount] = useState(100);
  const [searchParams] = useSearchParams();
  const drawDate = searchParams.get('draw_date') ?? '';
  const cutoffDrawId = searchParams.get('cutoff_draw_id') ?? '';

  const fetchBettingPool = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const params = new URLSearchParams();
      if (drawDate) params.set('draw_date', drawDate);
      else if (cutoffDrawId) params.set('cutoff_draw_id', cutoffDrawId);
      const url = params.toString()
        ? `${API_URL}/api/el-gordo/betting/pool?${params.toString()}`
        : `${API_URL}/api/el-gordo/betting/pool`;
      const res = await fetch(url, { cache: 'no-store' });
      const data = await res.json();
      if (!res.ok) {
        setError(data.detail ?? res.statusText ?? 'Error al cargar pool');
        setCandidatePool([]);
        setLastDrawDate(null);
        return;
      }
      setLastDrawDate(data.last_draw_date ?? null);
      setCandidatePool(Array.isArray(data.candidate_pool) ? data.candidate_pool : []);
      setRealPool(Array.isArray(data.bought_tickets) ? data.bought_tickets : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al cargar pool');
      setCandidatePool([]);
      setLastDrawDate(null);
    } finally {
      setLoading(false);
    }
  }, [drawDate, cutoffDrawId]);

  useEffect(() => {
    fetchBettingPool();
  }, [fetchBettingPool]);

  const addToBucket = (ticket: ElGordoTicket) => {
    if (bucket.length >= BUCKET_MAX) return;
    const key = ticketKey(ticket);
    setBucket((prev) => {
      if (prev.some((t) => ticketKey(t) === key)) return prev; // already in bucket
      return [...prev, { mains: [...(ticket.mains ?? [])], clave: ticket.clave }];
    });
  };

  const removeFromBucket = (index: number) => {
    setBucket((prev) => prev.filter((_, i) => i !== index));
  };

  const [openRealPlatformLoading, setOpenRealPlatformLoading] = useState(false);
  const [botProgress, setBotProgress] = useState<{ status: string; step: string; error?: string; has_pending_confirm?: boolean }>({ status: 'idle', step: '' });
  const [confirmBotBoughtLoading, setConfirmBotBoughtLoading] = useState(false);
  const botPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const openRealPlatform = async () => {
    if (bucket.length === 0) return;
    setOpenRealPlatformLoading(true);
    setError('');
    try {
      const body: { tickets: ElGordoTicket[]; draw_date?: string; cutoff_draw_id?: string } = { tickets: bucket };
      if (drawDate) body.draw_date = drawDate;
      else if (cutoffDrawId) body.cutoff_draw_id = cutoffDrawId;
      const res = await fetch(`${API_URL}/api/el-gordo/betting/open-real-platform`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.detail ?? res.statusText ?? 'Error al abrir la plataforma');
      } else {
        setBotProgress({ status: 'running', step: 'Iniciando...' });
        if (botPollRef.current) clearInterval(botPollRef.current);
        botPollRef.current = setInterval(async () => {
          try {
            const pr = await fetch(`${API_URL}/api/el-gordo/betting/bot-progress`, { cache: 'no-store' });
            const data = await pr.json();
            setBotProgress({
              status: data.status ?? 'idle',
              step: data.step ?? '',
              error: data.error,
              has_pending_confirm: data.has_pending_confirm,
            });
            if (data.status === 'success' || data.status === 'error') {
              if (botPollRef.current) {
                clearInterval(botPollRef.current);
                botPollRef.current = null;
              }
              if (data.status === 'error') setError(data.error ?? 'Error en el bot');
            }
          } catch {
            // ignore poll errors
          }
        }, 2000);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al abrir la plataforma');
    } finally {
      setOpenRealPlatformLoading(false);
    }
  };

  useEffect(() => () => {
    if (botPollRef.current) clearInterval(botPollRef.current);
  }, []);

  const confirmBotBought = async () => {
    setConfirmBotBoughtLoading(true);
    try {
      const res = await fetch(`${API_URL}/api/el-gordo/betting/confirm-bot-bought`, { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.detail ?? res.statusText ?? 'Error al confirmar');
      } else {
        setBucket([]);
        setBotProgress({ status: 'idle', step: '' });
        fetchBettingPool();
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al confirmar');
    } finally {
      setConfirmBotBoughtLoading(false);
    }
  };

  const buyBucket = async () => {
    if (bucket.length === 0) return;
    const combined = [...realPool, ...bucket];
    const seen = new Set<string>();
    const newRealPool = combined.filter((t) => {
      const k = ticketKey(t);
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
    setRealPool(newRealPool);
    setBucket([]);
    try {
      const body: { tickets: ElGordoTicket[]; draw_date?: string; cutoff_draw_id?: string } = { tickets: newRealPool };
      if (drawDate) body.draw_date = drawDate;
      else if (cutoffDrawId) body.cutoff_draw_id = cutoffDrawId;
      const res = await fetch(`${API_URL}/api/el-gordo/betting/bought`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.detail ?? res.statusText ?? 'Error al guardar boletos');
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al guardar boletos');
    }
  };

  const bucketFull = bucket.length >= BUCKET_MAX;
  const addRandomToBucket = () => {
    const need = Math.min(BUCKET_MAX - bucket.length, availableCandidates.length);
    if (need <= 0) return;
    const picked = shuffleArray(availableCandidates).slice(0, need).map((t) => ({ mains: [...(t.mains ?? [])], clave: t.clave }));
    setBucket((prev) => [...prev, ...picked]);
  };
  const inBucketOrReal = new Set([
    ...bucket.map(ticketKey),
    ...realPool.map(ticketKey),
  ]);
  const availableCandidates = candidatePool.filter((t) => !inBucketOrReal.has(ticketKey(t)));
  const displayedCandidates = availableCandidates.slice(0, candidateCount);

  if (loading) {
    return (
      <section className="card resultados-features-card resultados-theme-el-gordo el-gordo-betting">
        <Spin size="small" />
      </section>
    );
  }

  const botRunning = botProgress.status === 'running';

  return (
    <section className="card resultados-features-card resultados-theme-el-gordo el-gordo-betting" style={{ position: 'relative' }}>
      <h2 style={{ marginTop: 0, marginBottom: 'var(--space-sm)', fontSize: '1rem' }}>
        El Gordo — Apuestas
      </h2>
      {error && (
        <p style={{ color: 'var(--color-error)', marginBottom: 'var(--space-md)' }}>{error}</p>
      )}

      {/* While bot is running, block all interaction and show only progress */}
      {botRunning && (
        <div
          className="el-gordo-betting-bot-overlay"
          role="alert"
          aria-busy="true"
          style={{
            position: 'absolute',
            inset: 0,
            zIndex: 20,
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 'var(--space-md)',
            background: 'rgba(0,0,0,0.65)',
            borderRadius: 'var(--radius)',
            color: 'var(--color-text)',
            pointerEvents: 'auto',
            padding: 'var(--space-lg)',
          }}
        >
          <Spin size="large" />
          <span style={{ fontWeight: 600, fontSize: '1rem' }}>Bot en curso</span>
          <span style={{ fontSize: '0.9rem', color: 'var(--color-text-muted)', textAlign: 'center' }}>
            {botProgress.step || 'Rellenando boletos en Loterías…'}
          </span>
          <span style={{ fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
            No se puede usar la página hasta que termine.
          </span>
        </div>
      )}

      <div className="el-gordo-betting-split" style={botRunning ? { pointerEvents: 'none', userSelect: 'none', opacity: 0.7 } : undefined}>
        {/* Left 16: candidate pool as gallery — click card → add to bucket */}
        <div className="el-gordo-betting-left">
          <div className="el-gordo-betting-panel-card" style={{ flex: 1, minHeight: 280 }}>
            <div className="el-gordo-betting-candidate-header">
              <h3>Candidatos — clic para añadir a la cesta</h3>
              <div className="el-gordo-betting-candidate-actions">
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
                  onChange={(e) => setCandidateCount(Number(e.target.value))}
                  aria-label="Número de candidatos a mostrar"
                >
                  {CANDIDATE_COUNT_OPTIONS.map((n) => (
                    <option key={n} value={n}>{n}</option>
                  ))}
                </select>
                </label>
              </div>
            </div>
            <p style={{ margin: '0 0 var(--space-sm)', fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
              {candidatePool.length} boletos · {availableCandidates.length} disponibles · mostrando {displayedCandidates.length} · máx. {BUCKET_MAX} en la cesta
            </p>
            <div className="el-gordo-betting-gallery">
              {displayedCandidates.length === 0 ? (
                <p style={{ margin: 'auto', fontSize: '0.85rem', color: 'var(--color-text-muted)', gridColumn: '1 / -1' }}>
                  {candidatePool.length === 0
                    ? 'No hay pool. Ejecuta el pipeline de predicción para el sorteo.'
                    : 'No hay más candidatos disponibles (todos están en la cesta o en boletos guardados).'}
                </p>
              ) : (
                displayedCandidates.map((t, i) => (
                  <TicketCard
                    key={`c-${i}-${(t.mains ?? []).join('-')}-${t.clave}`}
                    ticket={t}
                    onClick={() => addToBucket(t)}
                    disabled={bucketFull}
                    styleAnimationDelay={i < 40 ? i * 25 : undefined}
                  />
                ))
              )}
            </div>
          </div>
        </div>

        {/* Right 8: top = bucket gallery, bottom = real pool */}
        <div className="el-gordo-betting-right">
          {botProgress.status !== 'idle' && (
            <div
              className="el-gordo-betting-bot-progress"
              role="status"
              aria-live="polite"
              style={{
                marginBottom: 'var(--space-sm)',
                padding: 'var(--space-sm) var(--space-md)',
                borderRadius: 'var(--radius)',
                background: botProgress.status === 'error' ? 'rgba(200,60,60,0.12)' : botProgress.status === 'success' ? 'rgba(40,160,80,0.12)' : 'rgba(0,0,0,0.04)',
                border: `1px solid ${botProgress.status === 'error' ? 'rgba(200,60,60,0.4)' : botProgress.status === 'success' ? 'rgba(40,160,80,0.4)' : 'var(--color-border)'}`,
                fontSize: '0.8rem',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
                {botProgress.status === 'running' && <Spin size="small" />}
                <span style={{ fontWeight: 600 }}>
                  {botProgress.status === 'running' && 'Bot en curso: '}
                  {botProgress.status === 'success' && 'Completado: '}
                  {botProgress.status === 'error' && 'Error: '}
                </span>
                <span>{botProgress.status === 'error' ? (botProgress.error ?? botProgress.step) : botProgress.step}</span>
                {botProgress.status === 'success' && botProgress.has_pending_confirm && (
                  <button
                    type="button"
                    style={{
                      marginLeft: 8,
                      padding: '4px 10px',
                      fontSize: '0.75rem',
                      fontWeight: 600,
                      borderRadius: 'var(--radius)',
                      border: '1px solid var(--color-border)',
                      background: 'var(--color-surface)',
                      cursor: confirmBotBoughtLoading ? 'wait' : 'pointer',
                    }}
                    disabled={confirmBotBoughtLoading}
                    onClick={confirmBotBought}
                  >
                    {confirmBotBoughtLoading ? '...' : 'Añadir a guardados'}
                  </button>
                )}
              </div>
            </div>
          )}
          <div className="el-gordo-betting-panel-card">
            <div className="el-gordo-betting-bucket-header">
              <h3>Cesta ({bucket.length}/{BUCKET_MAX})</h3>
              <div className="el-gordo-betting-bucket-toolbar">
                <button
                  type="button"
                  className="el-gordo-betting-btn-icon"
                  disabled={bucket.length === 0 || openRealPlatformLoading}
                  onClick={openRealPlatform}
                  aria-label="Comprar en Loterías — abre Chrome y rellena los boletos"
                  title="Comprar en Loterías — abre Chrome y rellena los boletos"
                >
                  <RealPlatformIcon />
                </button>
                <button
                  type="button"
                  className="el-gordo-betting-btn-icon"
                  disabled={bucket.length === 0}
                  onClick={buyBucket}
                  aria-label="Confirmar — pasar a boletos guardados"
                  title="Confirmar — pasar a boletos guardados"
                >
                  <BuyIcon />
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
                bucket.map((t, i) => (
                  <TicketCard
                    key={`b-${i}`}
                    ticket={t}
                    onClick={() => removeFromBucket(i)}
                    onRemove={() => removeFromBucket(i)}
                  />
                ))
              )}
            </div>
          </div>

          <div className="el-gordo-betting-panel-card" style={{ flex: 1, minHeight: 180 }}>
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
                  <TicketCard key={`r-${i}`} ticket={t} />
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
