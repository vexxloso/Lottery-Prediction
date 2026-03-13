import { useCallback, useEffect, useState } from 'react';
import { Card, Descriptions, Drawer, notification, Spin, Steps, Table, Tag } from 'antd';
import { useSearchParams } from 'react-router-dom';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

const PIPELINE_FETCH_MS = 5 * 60 * 1000;
async function fetchWithTimeout(
  url: string,
  options: RequestInit & { timeoutMs?: number } = {},
): Promise<Response> {
  const { timeoutMs = PIPELINE_FETCH_MS, ...fetchOpts } = options;
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...fetchOpts, signal: ctrl.signal });
    clearTimeout(id);
    return res;
  } catch (e) {
    clearTimeout(id);
    if (e instanceof Error && e.name === 'AbortError') {
      throw new Error(
        'La petición tardó demasiado (timeout). En un VPS, aumenta el timeout del proxy (nginx) o ejecuta el backend sin proxy.',
      );
    }
    throw e;
  }
}

interface ProbRow {
  number: number;
  p: number;
}

interface TrainProgress {
  cutoff_draw_id: string;
  dataset_prepared: boolean;
  dataset_prepared_at?: string;
  main_rows?: number;
  star_rows?: number;
  models_trained: boolean;
  trained_at?: string;
  main_accuracy?: number;
  star_accuracy?: number;
  probs_computed?: boolean;
  probs_computed_at?: string;
  mains_probs?: ProbRow[];
  stars_probs?: ProbRow[];
  probs_draw_id?: string;
  probs_fecha_sorteo?: string;
  rules_applied?: boolean;
  rules_applied_at?: string;
  filtered_mains_probs?: ProbRow[];
  filtered_stars_probs?: ProbRow[];
  rule_flags?: {
    rules_used?: string[];
    excluded?: { mains?: { number: number; reason: string }[]; stars?: unknown[] };
    stats?: {
      mains?: { count: number; sum: number; even: number; odd: number };
      stars?: { count: number; sum: number; even: number; odd: number };
    };
    snapshot_mains?: number[];
    snapshot_stars?: number[];
  };
  candidate_pool?: { mains: number[]; stars: number[] }[];
  candidate_pool_at?: string;
  candidate_pool_count?: number;
  full_wheel_draw_date?: string;
  full_wheel_file_path?: string;
  full_wheel_total_tickets?: number;
  full_wheel_good_tickets?: number;
  full_wheel_bad_tickets?: number;
  full_wheel_generated_at?: string;
   full_wheel_started_at?: string;
  full_wheel_status?: 'waiting' | 'done' | 'error';
  full_wheel_error?: string;
  full_wheel_elapsed_seconds?: number;
}

interface FullWheelPreviewTicket {
  position: number;
  mains: number[];
  stars: number[];
}

export function EuromillonesPredictionPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const cutoffDrawId = searchParams.get('cutoff_draw_id');
  const [progress, setProgress] = useState<TrainProgress | null>(null);
  const [progressLoading, setProgressLoading] = useState(false);
  const [runAllLoading, setRunAllLoading] = useState(false);
  const [runningStep, setRunningStep] = useState<number>(0);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerWidth, setDrawerWidth] = useState(420);
  const [currentDraw, setCurrentDraw] = useState<{ date: string; mains: number[]; stars: number[] } | null>(null);
  const [fullWheelPreview, setFullWheelPreview] = useState<FullWheelPreviewTicket[] | null>(null);

  useEffect(() => {
    if (!cutoffDrawId) {
      setCurrentDraw(null);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(
          `${API_URL}/api/euromillones/feature-model?draw_id=${encodeURIComponent(cutoffDrawId)}`
        );
        const data = await res.json();
        if (cancelled || !data.features?.length) {
          if (!cancelled) setCurrentDraw(null);
          return;
        }
        const f = data.features[0];
        const fecha = (f.fecha_sorteo || '').toString().trim();
        const date = fecha.split(' ')[0] || fecha;
        const mains = Array.isArray(f.main_number) ? f.main_number.map(Number).filter((n: number) => !isNaN(n)) : [];
        const stars = Array.isArray(f.star_number) ? f.star_number.map(Number).filter((n: number) => !isNaN(n)) : [];
        if (!cancelled) setCurrentDraw({ date, mains, stars });
      } catch {
        if (!cancelled) setCurrentDraw(null);
      }
    })();
    return () => { cancelled = true; };
  }, [cutoffDrawId]);

  useEffect(() => {
    const mq = window.matchMedia('(max-width: 768px)');
    const update = () => setDrawerWidth(mq.matches ? window.innerWidth : 420);
    update();
    mq.addEventListener('change', update);
    return () => mq.removeEventListener('change', update);
  }, []);

  const fetchProgress = useCallback(async (cacheBust = false) => {
    if (!cutoffDrawId) {
      setProgress(null);
      return;
    }
    setProgressLoading(true);
    try {
      const url = `${API_URL}/api/euromillones/train/progress?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}${cacheBust ? `&_t=${Date.now()}` : ''}`;
      const res = await fetch(url, { cache: 'no-store' });
      const data = await res.json();
      setProgress((data.progress as TrainProgress) ?? null);
    } catch {
      setProgress(null);
    } finally {
      setProgressLoading(false);
    }
  }, [cutoffDrawId]);

  useEffect(() => {
    fetchProgress();
  }, [fetchProgress]);

  // Poll backend every 5 minutes while full wheel is running.
  useEffect(() => {
    if (!cutoffDrawId) return undefined;
    if (progress?.full_wheel_status !== 'waiting') return undefined;
    const id = window.setInterval(() => {
      fetchProgress(true);
    }, 5 * 60 * 1000);
    return () => window.clearInterval(id);
  }, [cutoffDrawId, progress?.full_wheel_status, fetchProgress]);

  // Load a small preview of the full-wheel tickets when available
  useEffect(() => {
    const loadPreview = async () => {
      if (!cutoffDrawId) {
        setFullWheelPreview(null);
        return;
      }
      if ((progress?.full_wheel_total_tickets ?? 0) <= 0) {
        setFullWheelPreview(null);
        return;
      }
      try {
        const params = new URLSearchParams({ cutoff_draw_id: cutoffDrawId, limit: '20' });
        const res = await fetch(`${API_URL}/api/euromillones/train/full-wheel-preview?${params.toString()}`, {
          cache: 'no-store',
        });
        const data = await res.json();
        if (!res.ok) return;
        setFullWheelPreview((data.tickets as FullWheelPreviewTicket[]) ?? null);
      } catch {
        setFullWheelPreview(null);
      }
    };
    loadPreview();
  }, [cutoffDrawId, progress?.full_wheel_total_tickets]);

  // Current step from progress (so user sees "now I am in step 3") — only 4 steps for "Generar todo"
  const currentStep = progress == null
    ? 0
    : !progress.dataset_prepared
      ? 0
      : !progress.models_trained
        ? 1
        : !progress.probs_computed
          ? 2
          : 3;

  const handleBackToTable = () => {
    const params = new URLSearchParams(searchParams);
    params.delete('cutoff_draw_id');
    setSearchParams(params, { replace: true });
  };

  const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

  const runAllPipeline = async () => {
    if (!cutoffDrawId) return;
    setRunAllLoading(true);
    setRunningStep(0);
    try {
      const qs = `?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}`;
      // Step 1: prepare dataset
      let res = await fetchWithTimeout(`${API_URL}/api/euromillones/train/prepare-dataset${qs}`, { method: 'POST' });
      let data = await res.json();
      if (!res.ok || data.status !== 'ok') throw new Error((data as any).detail ?? 'Error preparando dataset');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(1);
      // Step 2: train models
      res = await fetchWithTimeout(`${API_URL}/api/euromillones/train/models${qs}`, { method: 'POST' });
      data = await res.json();
      if (!res.ok || data.status !== 'ok') throw new Error((data as any).detail ?? 'Error entrenando modelos');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(2);
      // Step 3: compute probs (can be slow on VPS; long timeout)
      res = await fetchWithTimeout(`${API_URL}/api/euromillones/prediction/ml${qs}`, { method: 'GET' });
      data = await res.json();
      if (!res.ok || data.status !== 'ok') throw new Error((data as any).detail ?? 'Error calculando probabilidades');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(3);
      // Step 4: rule filters
      res = await fetchWithTimeout(`${API_URL}/api/euromillones/train/rule-filters?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}`, { method: 'POST' });
      data = await res.json();
      if (!res.ok) throw new Error((data as { detail?: string }).detail ?? 'Error aplicando filtros');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(4);
      notification.success({
        message: 'Pipeline completado',
        description: 'Pasos 1 a 4 ejecutados. Pool de números generado y guardado en la base de datos.',
        placement: 'topRight',
        duration: 4,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Error en el pipeline';
      notification.error({ message: 'Error', description: msg, placement: 'topRight', duration: 5 });
    } finally {
      setRunAllLoading(false);
    }
  };

  const handleGenerateFullWheel = async () => {
    if (!cutoffDrawId) return;
    try {
      const params = new URLSearchParams({ cutoff_draw_id: cutoffDrawId });
      if (currentDraw?.date) {
        params.set('draw_date', currentDraw.date);
      }
      const res = await fetch(`${API_URL}/api/euromillones/train/full-wheel?${params.toString()}`, {
        method: 'POST',
      });
      const data = await res.json();
      if (!res.ok || data.status !== 'started') {
        throw new Error((data as { detail?: string }).detail ?? 'Error iniciando generación de tickets (full wheeling)');
      }
      notification.success({
        message: 'Generación iniciada (full wheeling)',
        description: 'La generación del archivo de tickets se está ejecutando en segundo plano. Este proceso puede tardar varios minutos.',
        placement: 'topRight',
        duration: 6,
      });
      await fetchProgress(true);
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Error iniciando generación de tickets (full wheeling)';
      notification.error({ message: 'Error', description: msg, placement: 'topRight', duration: 6 });
    }
  };

  const displayStep = runAllLoading ? runningStep : currentStep;
  const stepItems = [
    {
      title: 'Preparar dataset',
      status: runAllLoading
        ? (runningStep > 0 ? ('finish' as const) : ('process' as const))
        : progress?.dataset_prepared
          ? ('finish' as const)
          : currentStep === 0
            ? ('process' as const)
            : ('wait' as const),
    },
    {
      title: 'Entrenar modelos',
      status: runAllLoading
        ? (runningStep > 1 ? ('finish' as const) : runningStep === 1 ? ('process' as const) : ('wait' as const))
        : progress?.models_trained
          ? ('finish' as const)
          : currentStep === 1
            ? ('process' as const)
            : ('wait' as const),
    },
    {
      title: 'Probabilidades',
      status: runAllLoading
        ? (runningStep > 2 ? ('finish' as const) : runningStep === 2 ? ('process' as const) : ('wait' as const))
        : progress?.probs_computed
          ? ('finish' as const)
          : currentStep === 2
            ? ('process' as const)
            : ('wait' as const),
    },
    {
      title: 'Generar pool',
      status: runAllLoading
        ? (runningStep > 3 ? ('finish' as const) : runningStep === 3 ? ('process' as const) : ('wait' as const))
        : progress?.rules_applied
          ? ('finish' as const)
          : currentStep === 3
            ? ('process' as const)
            : ('wait' as const),
    },
  ];

  return (
    <section className="resultados-features-card resultados-theme-euromillones euromillones-train-split">
      <div
        className="euromillones-train-layout"
        style={{
          display: 'flex',
          flexDirection: 'row',
          gap: 'var(--space-md)',
          alignItems: 'flex-start',
        }}
      >
        {/* Left: filtered pool + preview table */}
        <div style={{ flex: '17 1 0%', minWidth: 0, display: 'flex', flexDirection: 'column', gap: 12 }} className="euromillones-train-table-col">
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 8, flexWrap: 'wrap' }}>
              <button
                type="button"
                className="resultados-features-iconbtn"
                onClick={handleBackToTable}
                aria-label="Volver a la tabla de features"
                title="Volver a la tabla de features"
              >
                ← Volver
              </button>
              <h4 className="resultados-features-chart-title" style={{ margin: 0 }}>
                Pool filtrado de números
              </h4>
            </div>
            {/* Intentionally no detailed text here; pool info is used internally. */}
          </div>

          <div>
            <h4 className="resultados-features-chart-title" style={{ margin: '8px 0' }}>
              Pool de candidatos (preview)
            </h4>
            <Table
              size="small"
              dataSource={(fullWheelPreview ?? []).map((t) => ({
                key: t.position,
                index: t.position,
                mainsStr: (t.mains ?? []).join(' '),
                starsStr: (t.stars ?? []).join(' '),
              }))}
              columns={[
                { title: '#', dataIndex: 'index', key: 'index', width: 56 },
                { title: 'Mains', dataIndex: 'mainsStr', key: 'mains' },
                { title: 'Stars', dataIndex: 'starsStr', key: 'stars', width: 80 },
              ]}
              pagination={false}
              scroll={{ x: 320 }}
              locale={{
                emptyText:
                  progress?.full_wheel_status === 'waiting'
                    ? 'Generando full wheel...'
                    : 'Sin datos. Pulsa "Generar tickets (full wheel)" para generar el pool completo.',
              }}
            />
          </div>
        </div>

        {/* Right: step card — 7 parts (own card) */}
        <div style={{ flex: '7 0 0%', minWidth: 260, maxWidth: 420, display: 'flex', flexDirection: 'column' }} className="euromillones-train-step-card-col">
          {progressLoading ? (
            <Spin size="small" />
          ) : (
            <Card size="small" className="euromillones-train-steps-card euromillones-train-steps-card-fixed" bodyStyle={{ padding: 'var(--space-md)', display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, overflow: 'auto' }}>
              <Steps
                direction="vertical"
                current={displayStep}
                items={stepItems}
                className="euromillones-train-steps"
              />
              <div style={{ marginTop: 'var(--space-md)', display: 'flex', flexDirection: 'column', gap: 'var(--space-sm)' }}>
                <button
                  type="button"
                  className="resultados-features-iconbtn"
                  disabled={runAllLoading || !cutoffDrawId || progress?.full_wheel_status === 'waiting'}
                  onClick={runAllPipeline}
                  style={{
                    padding: '10px 18px',
                    borderRadius: 999,
                    border: 'none',
                    background: 'linear-gradient(135deg, var(--resultados-primary) 0%, var(--resultados-primary-dark) 100%)',
                    color: '#fff',
                    fontSize: '0.95rem',
                    fontWeight: 600,
                    boxShadow: '0 4px 10px rgba(0,0,0,0.18)',
                  }}
                >
                  {runAllLoading ? (
                    <>
                      <Spin size="small" style={{ marginRight: 8 }} />
                      Ejecutando… (paso {displayStep + 1}/4)
                    </>
                  ) : (
                    'Generar todo'
                  )}
                </button>
                <button
                  type="button"
                  className="resultados-features-iconbtn"
                  disabled={
                    !cutoffDrawId ||
                    !progress?.rules_applied ||
                    progress.full_wheel_status === 'waiting' ||
                    progress.full_wheel_status === 'done'
                  }
                  onClick={handleGenerateFullWheel}
                  style={{
                    padding: '8px 16px',
                    borderRadius: 999,
                    border: '1px solid var(--color-border)',
                    background: 'transparent',
                    color: 'var(--color-text)',
                    fontSize: '0.9rem',
                  }}
                >
                  {progress?.full_wheel_status === 'waiting'
                    ? 'Generando tickets…'
                    : 'Generar tickets (full wheel)'}
                </button>
                <button
                  type="button"
                  className="resultados-features-iconbtn"
                  onClick={() => { fetchProgress(true); setDrawerOpen(true); }}
                  style={{
                    padding: '8px 16px',
                    borderRadius: 999,
                    border: '1px solid var(--color-border)',
                    background: 'transparent',
                    color: 'var(--color-text)',
                    fontSize: '0.9rem',
                  }}
                >
                  Ver progreso
                </button>
              </div>
            </Card>
          )}
          {currentDraw && (
            <Card size="small" className="euromillones-train-current-draw-card" bodyStyle={{ padding: 'var(--space-md)' }} style={{ marginTop: 'var(--space-md)' }}>
              <div style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', marginBottom: 'var(--space-xs)', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Sorteo actual
              </div>
              <div style={{ fontSize: '0.9rem', fontWeight: 600, marginBottom: 'var(--space-sm)' }}>
                {currentDraw.date}
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, alignItems: 'center', marginBottom: 'var(--space-xs)' }}>
                <span style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', marginRight: 4 }}>Mains:</span>
                {(currentDraw.mains.length ? currentDraw.mains : []).map((n) => (
                  <span key={n} className="resultados-ball resultados-train-draw-ball" style={{ width: 26, height: 26, fontSize: '0.75rem' }}>{n}</span>
                ))}
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, alignItems: 'center' }}>
                <span style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', marginRight: 4 }}>Stars:</span>
                {(currentDraw.stars.length ? currentDraw.stars : []).map((n) => (
                  <span key={n} className="resultados-ball resultados-ball star resultados-train-draw-ball" style={{ width: 26, height: 26, fontSize: '0.75rem' }}>{n}</span>
                ))}
              </div>
            </Card>
          )}
        </div>
      </div>

      <Drawer
        className="euromillones-progress-drawer"
        title="Progreso del pipeline"
        placement="right"
        width={drawerWidth}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
      >
        {progress ? (
          <>
            <Descriptions column={1} size="small" bordered>
              <Descriptions.Item label="Sorteo (cutoff)">{progress.cutoff_draw_id}</Descriptions.Item>
              <Descriptions.Item label="1. Preparar dataset">
                {progress.dataset_prepared ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.main_rows != null && ` · Mains: ${progress.main_rows} filas`}
                {progress.star_rows != null && ` · Stars: ${progress.star_rows} filas`}
              </Descriptions.Item>
              <Descriptions.Item label="2. Entrenar modelos">
                {progress.models_trained ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.main_accuracy != null && ` · Accuracy mains: ${(progress.main_accuracy * 100).toFixed(2)}%`}
                {progress.star_accuracy != null && ` · stars: ${(progress.star_accuracy * 100).toFixed(2)}%`}
              </Descriptions.Item>
              <Descriptions.Item label="3. Probabilidades">
                {progress.probs_computed ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.probs_fecha_sorteo && ` · ${progress.probs_fecha_sorteo}`}
              </Descriptions.Item>
              <Descriptions.Item label="4. Generar pool">
                {progress.rules_applied ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.rule_flags?.rules_used?.length ? ` · ${progress.rule_flags.rules_used.join(', ')}` : ''}
              </Descriptions.Item>
              <Descriptions.Item label="5. Pool de candidatos (full wheel)">
                {progress.full_wheel_status === 'waiting' ? (
                  <Tag color="processing">En curso</Tag>
                ) : (progress.full_wheel_total_tickets ?? 0) > 0 ? (
                  <Tag color="success">Hecho</Tag>
                ) : progress.full_wheel_status === 'error' ? (
                  <Tag color="error">Error</Tag>
                ) : (
                  <Tag>Pendiente</Tag>
                )}
                {progress.full_wheel_total_tickets != null &&
                  ` · ${progress.full_wheel_total_tickets} tickets (buenos: ${progress.full_wheel_good_tickets ?? 0}, penalizados: ${progress.full_wheel_bad_tickets ?? 0})`}
              </Descriptions.Item>
            </Descriptions>
            {(progress.filtered_mains_probs?.length || progress.filtered_stars_probs?.length) ? (
              <div style={{ marginTop: 16 }}>
                <div style={{ marginBottom: 8, fontWeight: 600 }}>Pool filtrado</div>
                <p style={{ margin: 0, fontSize: '0.9rem' }}>
                  Mains ({progress.filtered_mains_probs?.length ?? 0}):{' '}
                  {progress.filtered_mains_probs?.map((x) => x.number).join(' ') || '—'}
                </p>
                <p style={{ margin: '4px 0 0', fontSize: '0.9rem' }}>
                  Stars ({progress.filtered_stars_probs?.length ?? 0}):{' '}
                  {progress.filtered_stars_probs?.map((x) => x.number).join(' ') || '—'}
                </p>
              </div>
            ) : null}
            {(progress.candidate_pool?.length ?? 0) > 0 && (
              <div style={{ marginTop: 16 }}>
                <div style={{ marginBottom: 8, fontWeight: 600 }}>Muestra del pool ({Math.min(10, progress.candidate_pool!.length)} primeros)</div>
                <Table
                  size="small"
                  dataSource={progress.candidate_pool!.slice(0, 10).map((t, i) => ({ key: i, mains: (t.mains ?? []).join(' '), stars: (t.stars ?? []).join(' ') }))}
                  columns={[{ title: 'Mains', dataIndex: 'mains' }, { title: 'Stars', dataIndex: 'stars', width: 80 }]}
                  pagination={false}
                />
              </div>
            )}
          </>
        ) : (
          <Spin size="small" />
        )}
      </Drawer>
    </section>
  );
}
