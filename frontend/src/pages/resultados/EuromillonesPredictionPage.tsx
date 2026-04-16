import { useCallback, useEffect, useRef, useState } from 'react';
import { Card, notification, Spin, Steps, Table } from 'antd';
import { useSearchParams } from 'react-router-dom';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

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
  pipeline_status?: 'idle' | 'running' | 'done' | 'error';
  pipeline_error?: string | null;
  pipeline_started_at?: string;
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
  const pipelineNotifiedRef = useRef<'done' | 'error' | null>(null);
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

  const fetchProgress = useCallback(async (cacheBust = false, silent = false) => {
    if (!cutoffDrawId) {
      setProgress(null);
      return;
    }
    if (!silent) {
      setProgressLoading(true);
    }
    try {
      const url = `${API_URL}/api/euromillones/train/progress?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}${cacheBust ? `&_t=${Date.now()}` : ''}`;
      const res = await fetch(url, { cache: 'no-store', method: 'POST' });
      const data = await res.json();
      setProgress((data.progress as TrainProgress) ?? null);
    } catch {
      setProgress(null);
    } finally {
      if (!silent) {
        setProgressLoading(false);
      }
    }
  }, [cutoffDrawId]);

  useEffect(() => {
    fetchProgress();
  }, [fetchProgress]);

  // Poll when full wheel or pipeline is running — silent so the card does not refresh/flash
  useEffect(() => {
    if (!cutoffDrawId) return;
    const needPoll =
      progress?.full_wheel_status === 'waiting' || progress?.pipeline_status === 'running';
    if (!needPoll) return;
    let cancelled = false;
    const interval = window.setInterval(() => {
      if (cancelled) return;
      void fetchProgress(true, true);
    }, 4000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [cutoffDrawId, progress?.full_wheel_status, progress?.pipeline_status, fetchProgress]);

  useEffect(() => {
    if (!progress) return;
    if (progress.pipeline_status === 'running') pipelineNotifiedRef.current = null;
    if (progress.pipeline_status === 'done' && pipelineNotifiedRef.current !== 'done') {
      pipelineNotifiedRef.current = 'done';
      setRunAllLoading(false);
      notification.success({
        message: 'Pipeline completado',
        description: 'Pasos 1 a 4 ejecutados. Pool de números generado y guardado en la base de datos.',
        placement: 'topRight',
        duration: 4,
      });
    } else if (progress.pipeline_status === 'error' && pipelineNotifiedRef.current !== 'error') {
      pipelineNotifiedRef.current = 'error';
      setRunAllLoading(false);
      notification.error({
        message: 'Error en el pipeline',
        description: progress.pipeline_error ?? 'Error desconocido',
        placement: 'topRight',
        duration: 6,
      });
    }
  }, [progress?.pipeline_status, progress?.pipeline_error]);

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

  const currentStep =
    progress == null
      ? 0
      : !progress.dataset_prepared
        ? 0
        : !progress.models_trained
          ? 1
          : !progress.probs_computed
            ? 2
            : !progress.rules_applied
              ? 3
              : 4;

  const handleBackToTable = () => {
    const params = new URLSearchParams(searchParams);
    params.delete('cutoff_draw_id');
    setSearchParams(params, { replace: true });
  };

  const pipelineRunning = progress?.pipeline_status === 'running' || runAllLoading;
  const runAllPipeline = async () => {
    if (!cutoffDrawId) return;
    if (pipelineRunning) return;
    setRunAllLoading(true);
    try {
      const res = await fetch(
        `${API_URL}/api/euromillones/train/run-pipeline?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}`,
        { method: 'POST' },
      );
      const data = await res.json();
      if (!res.ok) {
        setRunAllLoading(false);
        throw new Error((data as { detail?: string }).detail ?? 'Error iniciando pipeline');
      }
      if (data.status === 'done') setRunAllLoading(false);
      await fetchProgress(true);
    } catch (e) {
      setRunAllLoading(false);
      const msg = e instanceof Error ? e.message : 'Error en el pipeline';
      notification.error({ message: 'Error', description: msg, placement: 'topRight', duration: 5 });
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

  const displayStep = currentStep;
  const stepItems = [
    {
      title: 'Preparar dataset',
      status: progress?.dataset_prepared ? ('finish' as const) : currentStep === 0 ? ('process' as const) : ('wait' as const),
    },
    {
      title: 'Entrenar modelos',
      status: progress?.models_trained ? ('finish' as const) : currentStep === 1 ? ('process' as const) : ('wait' as const),
    },
    {
      title: 'Probabilidades',
      status: progress?.probs_computed ? ('finish' as const) : currentStep === 2 ? ('process' as const) : ('wait' as const),
    },
    {
      title: 'Generar pool',
      status: progress?.rules_applied ? ('finish' as const) : currentStep === 3 ? ('process' as const) : ('wait' as const),
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
                  disabled={pipelineRunning || !cutoffDrawId || progress?.full_wheel_status === 'waiting'}
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
                  {pipelineRunning ? (
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
    </section>
  );
}
