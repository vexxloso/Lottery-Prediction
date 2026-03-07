import { useCallback, useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Spin } from 'antd';

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
type LaPrimitivaMains = { mains: number[] };
/** Full ticket with reintegro (for saved/bought and API). */
type LaPrimitivaTicket = { mains: number[]; reintegro: number };

function mainsKey(t: LaPrimitivaMains): string {
  return (t.mains ?? []).slice().sort((a, b) => a - b).join(',');
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

function ShuffleIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l5 5" />
    </svg>
  );
}

function TicketCardMains({
  ticket,
  onClick,
  onRemove,
  disabled,
  styleAnimationDelay,
}: {
  ticket: LaPrimitivaMains;
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
  const [reintegroModalOpen, setReintegroModalOpen] = useState(false);
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
        ? `${API_URL}/api/la-primitiva/betting/pool?${params.toString()}`
        : `${API_URL}/api/la-primitiva/betting/pool`;
      const res = await fetch(url, { cache: 'no-store' });
      const data = await res.json();
      if (!res.ok) {
        setError(data.detail ?? res.statusText ?? 'Error al cargar pool');
        setCandidatePool([]);
        return;
      }
      const rawPool = Array.isArray(data.candidate_pool) ? data.candidate_pool : [];
      const rawBought = Array.isArray(data.bought_tickets) ? data.bought_tickets : [];
      setCandidatePool(
        rawPool.map((t: { mains?: unknown; reintegro?: unknown }) => ({
          mains: Array.isArray(t.mains) ? t.mains.map(Number) : [],
          reintegro: typeof t.reintegro === 'number' ? t.reintegro : Number(t.reintegro) || 0,
        }))
      );
      setRealPool(
        rawBought.map((t: { mains?: unknown; reintegro?: unknown }) => ({
          mains: Array.isArray(t.mains) ? t.mains.map(Number) : [],
          reintegro: typeof t.reintegro === 'number' ? t.reintegro : Number(t.reintegro) || 0,
        }))
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Error al cargar pool';
      setError(msg.includes('fetch') || msg.includes('Failed') ? 'Error de conexión con el servidor. Comprueba que el backend esté en marcha (puerto 8000).' : msg);
      setCandidatePool([]);
    } finally {
      setLoading(false);
    }
  }, [drawDate, cutoffDrawId]);

  useEffect(() => { fetchBettingPool(); }, [fetchBettingPool]);

  const addToBucket = (ticket: LaPrimitivaTicket) => {
    if (bucket.length >= BUCKET_MAX) return;
    const key = mainsKey(ticket);
    setBucket((prev) => {
      if (prev.some((t) => mainsKey(t) === key)) return prev; // already in bucket
      return [...prev, { mains: [...(ticket.mains ?? [])] }];
    });
  };

  const removeFromBucket = (index: number) => {
    setBucket((prev) => prev.filter((_, i) => i !== index));
  };

  const openBuyReintegro = () => {
    if (bucket.length === 0) return;
    setReintegroModalOpen(true);
  };

  const confirmBuyWithReintegro = async (reintegro: number) => {
    setReintegroModalOpen(false);
    const newTickets: LaPrimitivaTicket[] = bucket.map((m) => ({ mains: [...m.mains], reintegro }));
    const combined = [...realPool, ...newTickets];
    const seen = new Set<string>();
    const newRealPool = combined.filter((t) => {
      const k = mainsKey(t);
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
    setRealPool(newRealPool);
    setBucket([]);
    try {
      const body: { tickets: LaPrimitivaTicket[]; draw_date?: string; cutoff_draw_id?: string } = { tickets: newRealPool };
      if (drawDate) body.draw_date = drawDate;
      else if (cutoffDrawId) body.cutoff_draw_id = cutoffDrawId;
      const res = await fetch(`${API_URL}/api/la-primitiva/betting/bought`, {
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
  const savedMainsKeys = new Set(realPool.map((t) => mainsKey(t)));
  const bucketMainsKeys = new Set(bucket.map(mainsKey));
  const inBucketOrReal = new Set([...bucketMainsKeys, ...savedMainsKeys]);
  const availableCandidates = candidatePool.filter((t) => !inBucketOrReal.has(mainsKey(t)));
  const addRandomToBucket = () => {
    const need = Math.min(BUCKET_MAX - bucket.length, availableCandidates.length);
    if (need <= 0) return;
    const picked = shuffleArray(availableCandidates).slice(0, need).map((t) => ({ mains: [...(t.mains ?? [])] }));
    setBucket((prev) => [...prev, ...picked]);
  };
  const displayedCandidates = availableCandidates.slice(0, candidateCount);

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

      {reintegroModalOpen && (
        <div className="el-gordo-betting-reintegro-overlay" role="dialog" aria-modal="true" aria-label="Elegir reintegro">
          <div className="el-gordo-betting-reintegro-modal">
            <h3>Elige reintegro (0-9)</h3>
            <p style={{ fontSize: '0.9rem', color: 'var(--color-text-muted)', marginBottom: 12 }}>
              Los {bucket.length} boletos tendrán el mismo reintegro.
            </p>
            <div className="el-gordo-betting-reintegro-buttons">
              {REINTEGRO_OPTIONS.map((r) => (
                <button
                  key={r}
                  type="button"
                  className="resultados-ball reintegro el-gordo-betting-reintegro-btn"
                  onClick={() => confirmBuyWithReintegro(r)}
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
                    ? (!drawDate && !cutoffDrawId
                        ? 'Se abre con la fecha del último sorteo (draw_date). Si no hay datos, ejecuta en Predicción el paso de pool de candidatos.'
                        : 'No hay pool. En Predicción ejecuta todos los pasos hasta generar el pool de candidatos para este sorteo.')
                    : 'No hay más candidatos disponibles (todos están en la cesta o en boletos guardados).'}
                </p>
              ) : (
                displayedCandidates.map((t, i) => (
                  <TicketCardMains
                    key={`c-${i}-${mainsKey(t)}`}
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

        <div className="el-gordo-betting-right">
          <div className="el-gordo-betting-panel-card">
            <div className="el-gordo-betting-bucket-header">
              <h3>Cesta ({bucket.length}/{BUCKET_MAX})</h3>
              <div className="el-gordo-betting-bucket-toolbar">
                <button
                  type="button"
                  className="el-gordo-betting-btn-icon"
                  disabled={bucket.length === 0}
                  onClick={openBuyReintegro}
                  aria-label="Confirmar — elegir reintegro y guardar"
                  title="Confirmar — elegir reintegro y guardar"
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
                  Vacío. Haz clic en un candidato para añadir. Al confirmar elegirás un reintegro para los 8 boletos.
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
                  <TicketCardFull key={`r-${i}`} ticket={t} />
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
