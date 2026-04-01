import { useCallback, useEffect, useRef, useState } from 'react';
import { Card, notification, Spin, Steps, Table } from 'antd';
import { useSearchParams } from 'react-router-dom';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

interface ProbRow {
  number: number;
  p: number;
}

interface TrainProgressLaPrimitiva {
  cutoff_draw_id: string;
  dataset_prepared: boolean;
  dataset_prepared_at?: string;
  main_rows?: number;
  reintegro_rows?: number;
  models_trained: boolean;
  trained_at?: string;
  main_accuracy?: number;
  reintegro_accuracy?: number;
  probs_computed?: boolean;
  probs_computed_at?: string;
  mains_probs?: ProbRow[];
  reintegro_probs?: ProbRow[];
  probs_draw_id?: string;
  probs_fecha_sorteo?: string;
  rules_applied?: boolean;
  rules_applied_at?: string;
  filtered_mains_probs?: ProbRow[];
  filtered_reintegro_probs?: ProbRow[];
  rule_flags?: {
    rules_used?: string[];
    snapshot_mains?: number[];
    snapshot_reintegro?: number[];
  };
  candidate_pool?: { mains: number[]; reintegro: number }[];
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
  full_wheel_error?: string | null;
  full_wheel_elapsed_seconds?: number;
  pipeline_status?: 'idle' | 'running' | 'done' | 'error';
  pipeline_error?: string | null;
  pipeline_started_at?: string;
}

export function LaPrimitivaPredictionPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const cutoffDrawId = searchParams.get('cutoff_draw_id');
  const [progress, setProgress] = useState<TrainProgressLaPrimitiva | null>(null);
  const [progressLoading, setProgressLoading] = useState(false);
  const [runAllLoading, setRunAllLoading] = useState(false);
  const [currentDraw, setCurrentDraw] = useState<{
    date: string;
    mains: number[];
    complementario: number | null;
    reintegro: number | null;
  } | null>(null);
  const [candidateDisplayCount] = useState(20);
  const [fullWheelPreview, setFullWheelPreview] = useState<
    { position: number; mains: number[] }[] | null
  >(null);
  // Local flag so the step 5 button is disabled immediately after click.
  const [startingFullWheel, setStartingFullWheel] = useState(false);
  const pipelineNotifiedRef = useRef<'done' | 'error' | null>(null);

  // Load feature-model row for the selected cutoff draw
  useEffect(() => {
    if (!cutoffDrawId) {
      setCurrentDraw(null);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(
          `${API_URL}/api/la-primitiva/feature-model?draw_id=${encodeURIComponent(
            cutoffDrawId,
          )}`,
        );
        const data = await res.json();
        if (cancelled || !data.features?.length) {
          if (!cancelled) setCurrentDraw(null);
          return;
        }
        const f = data.features[0];
        const fecha = (f.fecha_sorteo || '').toString().trim();
        const date = fecha.split(' ')[0] || fecha;
        const mains = Array.isArray(f.main_number)
          ? f.main_number.map(Number).filter((n: number) => !Number.isNaN(n))
          : [];
        const complementario =
          typeof f.complementario === 'number' ? Number(f.complementario) : null;
        const reintegro = typeof f.reintegro === 'number' ? Number(f.reintegro) : null;
        if (!cancelled) setCurrentDraw({ date, mains, complementario, reintegro });
      } catch {
        if (!cancelled) setCurrentDraw(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [cutoffDrawId]);

  const fetchProgress = useCallback(
    async (cacheBust = false, silent = false) => {
      if (!cutoffDrawId) {
        setProgress(null);
        return;
      }
      if (!silent) {
        setProgressLoading(true);
      }
      try {
        const url = `${API_URL}/api/la-primitiva/train/progress?cutoff_draw_id=${encodeURIComponent(
          cutoffDrawId,
        )}${cacheBust ? `&_t=${Date.now()}` : ''}`;
        const res = await fetch(url, { cache: 'no-store', method: 'POST' });
        const data = await res.json();
        setProgress((data.progress as TrainProgressLaPrimitiva) ?? null);
      } catch {
        setProgress(null);
      } finally {
        if (!silent) {
          setProgressLoading(false);
        }
      }
    },
    [cutoffDrawId],
  );

  useEffect(() => {
    fetchProgress();
  }, [fetchProgress]);

  // Load a small preview of the full-wheel tickets when available.
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
        const res = await fetch(
          `${API_URL}/api/la-primitiva/train/full-wheel-preview?${params.toString()}`,
          { cache: 'no-store' },
        );
        const data = await res.json();
        if (!res.ok || data.detail) {
          setFullWheelPreview(null);
          return;
        }
        setFullWheelPreview(
          Array.isArray(data.tickets)
            ? (data.tickets as any[]).map((t) => ({
                position: Number(t.position),
                mains: Array.isArray(t.mains) ? t.mains.map(Number) : [],
              }))
            : null,
        );
      } catch {
        setFullWheelPreview(null);
      }
    };
    void loadPreview();
  }, [cutoffDrawId, progress?.full_wheel_total_tickets]);

  // Poll when full wheel is running or when steps 1–4 pipeline is running (backend-run).
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

  // When pipeline finishes (done/error), stop loading and show notification once.
  useEffect(() => {
    if (!progress) return;
    if (progress.pipeline_status === 'running') {
      pipelineNotifiedRef.current = null;
    }
    if (progress.pipeline_status === 'done' && pipelineNotifiedRef.current !== 'done') {
      pipelineNotifiedRef.current = 'done';
      setRunAllLoading(false);
      notification.success({
        message: 'Pipeline La Primitiva completado',
        description: 'Pasos 1 a 4 ejecutados. Pool de números generado y guardado en la base de datos.',
        placement: 'topRight',
        duration: 5,
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

  // Steps 1‑4: generate number pool (dataset, models, probs, rule filters).
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
        `${API_URL}/api/la-primitiva/train/run-pipeline?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}`,
        { method: 'POST' },
      );
      const data = await res.json();
      if (!res.ok) {
        setRunAllLoading(false);
        throw new Error((data as { detail?: string }).detail ?? 'Error iniciando pipeline');
      }
      if (data.status === 'done') {
        setRunAllLoading(false);
      }
      await fetchProgress(true);
    } catch (e) {
      setRunAllLoading(false);
      const msg = e instanceof Error ? e.message : 'Error en el pipeline de La Primitiva';
      notification.error({ message: 'Error', description: msg, placement: 'topRight', duration: 5 });
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

  const handleGenerateCandidatePool = async () => {
    if (!cutoffDrawId) return;
    try {
      setStartingFullWheel(true);
      const params = new URLSearchParams({ cutoff_draw_id: cutoffDrawId });
      if (currentDraw?.date) {
        params.set('draw_date', currentDraw.date);
      }
      const res = await fetch(
        `${API_URL}/api/la-primitiva/train/full-wheel?${params.toString()}`,
        {
          method: 'POST',
        },
      );
      const data = await res.json();
      if (!res.ok) {
        throw new Error(
          (data as { detail?: string }).detail ??
            'Error iniciando generación de boletos (full wheeling)',
        );
      }
      await fetchProgress(true);
      notification.success({
        message: 'Generación iniciada (full wheeling)',
        description:
          'La generación del archivo de boletos de La Primitiva se está ejecutando en segundo plano (full wheel).',
        placement: 'topRight',
        duration: 4,
      });
    } catch (e) {
      const msg =
        e instanceof Error
          ? e.message
          : 'Error iniciando generación de boletos (full wheeling) para La Primitiva';
      notification.error({
        message: 'Error',
        description: msg,
        placement: 'topRight',
        duration: 5,
      });
    } finally {
      // Once progress is refreshed, startingFullWheel no longer matters; the button will stay
      // disabled while progress.full_wheel_status === 'waiting'.
      setStartingFullWheel(false);
    }
  };

  return (
    <section className="resultados-features-card resultados-theme-la-primitiva euromillones-train-split">
      <div
        className="euromillones-train-layout"
        style={{
          display: 'flex',
          flexDirection: 'row',
          gap: 'var(--space-md)',
          alignItems: 'flex-start',
        }}
      >
        {/* Left: placeholder table (no candidate pool yet) */}
        <div
          style={{
            flex: '17 1 0%',
            minWidth: 0,
            display: 'flex',
            flexDirection: 'column',
          }}
          className="euromillones-train-table-col"
        >
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: 12,
              marginBottom: 8,
              flexWrap: 'wrap',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
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
                Pool de boletos La Primitiva (full wheel – vista previa)
              </h4>
            </div>
            {/* Removed page-size select; always show first 20 tickets for preview. */}
          </div>
          <Table
            size="small"
            dataSource={
              (fullWheelPreview ?? [])
                .slice(0, candidateDisplayCount)
                .map((t, i) => ({
                  key: t.position ?? i,
                  index: t.position ?? i + 1,
                  mainsStr: (t.mains ?? []).join(' '),
                }))
            }
            columns={[
              { title: '#', dataIndex: 'index', key: 'index', width: 56 },
              { title: 'Mains', dataIndex: 'mainsStr', key: 'mains' },
            ]}
            pagination={false}
            scroll={{ x: 320 }}
            locale={{
              emptyText:
                'Todavía no hay archivo full wheel para La Primitiva. Genera el pool de boletos (paso 5) para verlo aquí.',
            }}
          />
        </div>

        {/* Right: steps + current draw */}
        <div
          style={{
            flex: '7 0 0%',
            minWidth: 260,
            maxWidth: 420,
            display: 'flex',
            flexDirection: 'column',
          }}
          className="euromillones-train-step-card-col"
        >
          {progressLoading ? (
            <Spin size="small" />
          ) : (
            <Card
              size="small"
              className="euromillones-train-steps-card euromillones-train-steps-card-fixed"
              bodyStyle={{
                padding: 'var(--space-md)',
                display: 'flex',
                flexDirection: 'column',
                flex: 1,
                minHeight: 0,
                overflow: 'auto',
              }}
            >
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
                  disabled={pipelineRunning || !cutoffDrawId}
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
                    'Generar pool de números (pasos 1‑4)'
                  )}
                </button>
                <button
                  type="button"
                  className="resultados-features-iconbtn"
                  disabled={
                    pipelineRunning ||
                    !cutoffDrawId ||
                    startingFullWheel ||
                    progress?.full_wheel_status === 'waiting' ||
                    (progress?.full_wheel_status === 'done' &&
                      !!progress?.full_wheel_file_path)
                  }
                  onClick={handleGenerateCandidatePool}
                  style={{
                    padding: '8px 16px',
                    borderRadius: 999,
                    border: '1px solid var(--color-border)',
                    background: 'transparent',
                    color: 'var(--color-text)',
                    fontSize: '0.9rem',
                  }}
                >
                  Generar pool de boletos (paso 5)
                </button>
                {(startingFullWheel || progress?.full_wheel_status === 'waiting') && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: '0.85rem' }}>
                    <Spin size="small" />
                    <span>Generando boletos (full wheel)…</span>
                  </div>
                )}
              </div>
            </Card>
          )}

          <Card
            size="small"
            className="euromillones-train-current-draw-card"
            style={{ marginTop: 'var(--space-md)' }}
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
              Sorteo de referencia
            </div>
            {currentDraw ? (
              <>
                <div
                  style={{
                    fontSize: '0.9rem',
                    marginBottom: 'var(--space-sm)',
                  }}
                >
                  {currentDraw.date}
                </div>
                <div
                  style={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 6,
                    alignItems: 'center',
                    marginBottom: 'var(--space-xs)',
                  }}
                >
                  {(currentDraw.mains ?? []).map((n) => (
                    <span
                      key={n}
                      className="resultados-ball resultados-train-draw-ball"
                      style={{ width: 28, height: 28, fontSize: '0.8rem' }}
                    >
                      {String(n).padStart(2, '0')}
                    </span>
                  ))}
                </div>
                {(currentDraw.complementario != null ||
                  currentDraw.reintegro != null) && (
                  <div
                    style={{
                      display: 'flex',
                      flexWrap: 'wrap',
                      gap: 8,
                      alignItems: 'center',
                      fontSize: '0.85rem',
                    }}
                  >
                    {currentDraw.complementario != null && (
                      <span>
                        C:{' '}
                        <strong>
                          {String(currentDraw.complementario).padStart(2, '0')}
                        </strong>
                      </span>
                    )}
                    {currentDraw.reintegro != null && (
                      <span>
                        R: <strong>{currentDraw.reintegro}</strong>
                      </span>
                    )}
                  </div>
                )}
              </>
            ) : (
              <div style={{ fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>
                No se ha podido cargar el sorteo de referencia.
              </div>
            )}
          </Card>
        </div>
      </div>
    </section>
  );
}

