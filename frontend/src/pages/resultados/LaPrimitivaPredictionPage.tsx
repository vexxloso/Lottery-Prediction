import { useCallback, useEffect, useState } from 'react';
import { Card, Descriptions, Drawer, notification, Select, Spin, Steps, Table, Tag } from 'antd';
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
}

export function LaPrimitivaPredictionPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const cutoffDrawId = searchParams.get('cutoff_draw_id');
  const [progress, setProgress] = useState<TrainProgressLaPrimitiva | null>(null);
  const [progressLoading, setProgressLoading] = useState(false);
  const [runAllLoading, setRunAllLoading] = useState(false);
  const [runningStep, setRunningStep] = useState<number>(0);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerWidth, setDrawerWidth] = useState(420);
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

  // Responsive drawer width
  useEffect(() => {
    const mq = window.matchMedia('(max-width: 768px)');
    const update = () => setDrawerWidth(mq.matches ? window.innerWidth : 420);
    update();
    mq.addEventListener('change', update);
    return () => mq.removeEventListener('change', update);
  }, []);

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

  // While full wheel generation is running (status === 'waiting'), poll backend
  // for updated progress so the UI refreshes automatically when it finishes.
  useEffect(() => {
    if (!cutoffDrawId) return;
    if (progress?.full_wheel_status !== 'waiting') return;
    let cancelled = false;
    const interval = window.setInterval(() => {
      if (cancelled) return;
      void fetchProgress(true, true);
    }, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [cutoffDrawId, progress?.full_wheel_status, fetchProgress]);

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

  const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

  const runAllPipeline = async () => {
    if (!cutoffDrawId) return;
    setRunAllLoading(true);
    setRunningStep(0);
    try {
      const qs = `?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}`;
      // Step 1: prepare dataset
      let res = await fetch(
        `${API_URL}/api/la-primitiva/train/prepare-dataset${qs}`,
        { method: 'POST' },
      );
      let data = await res.json();
      if (!res.ok || data.status !== 'ok')
        throw new Error((data as any).detail ?? 'Error preparando dataset');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(1);

      // Step 2: train models
      res = await fetch(`${API_URL}/api/la-primitiva/train/models${qs}`, {
        method: 'POST',
      });
      data = await res.json();
      if (!res.ok || data.status !== 'ok')
        throw new Error((data as any).detail ?? 'Error entrenando modelos');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(2);

      // Step 3: compute probabilities
      res = await fetch(`${API_URL}/api/la-primitiva/prediction/ml${qs}`, {
        method: 'GET',
      });
      data = await res.json();
      if (!res.ok || data.status !== 'ok')
        throw new Error(
          (data as any).detail ?? 'Error calculando probabilidades',
        );
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(3);

      // Step 4: rule filters (number pool)
      res = await fetch(
        `${API_URL}/api/la-primitiva/train/rule-filters?cutoff_draw_id=${encodeURIComponent(
          cutoffDrawId,
        )}`,
        { method: 'POST' },
      );
      data = await res.json();
      if (!res.ok)
        throw new Error(
          (data as { detail?: string }).detail ??
            'Error generando pool de números',
        );
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(4);
      notification.success({
        message: 'Pipeline La Primitiva completado',
        description:
          'Pasos 1 a 4 ejecutados. Pool de números generado y guardado en la base de datos.',
        placement: 'topRight',
        duration: 5,
      });
    } catch (e) {
      const msg =
        e instanceof Error ? e.message : 'Error en el pipeline de La Primitiva';
      notification.error({
        message: 'Error',
        description: msg,
        placement: 'topRight',
        duration: 5,
      });
    } finally {
      setRunAllLoading(false);
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
                  disabled={runAllLoading || !cutoffDrawId}
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
                    'Generar pool de números (pasos 1‑4)'
                  )}
                </button>
                <button
                  type="button"
                  className="resultados-features-iconbtn"
                  disabled={
                    runAllLoading ||
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
            {cutoffDrawId && (
              <div
                style={{
                  fontSize: '0.9rem',
                  fontWeight: 600,
                  marginBottom: 'var(--space-xs)',
                }}
              >
                id_sorteo {cutoffDrawId}
              </div>
            )}
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

      <Drawer
        className="euromillones-progress-drawer"
        title="Progreso del pipeline"
        placement="right"
        width={drawerWidth}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        bodyStyle={{ padding: 'var(--space-md)' }}
        rootClassName="resultados-features-drawer"
      >
        {progress ? (
          <>
            <Descriptions column={1} size="small" bordered>
              <Descriptions.Item label="Cutoff id_sorteo">
                <Tag color="blue">{progress.cutoff_draw_id}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="1. Dataset">
                {progress.dataset_prepared ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.dataset_prepared_at && ` · ${progress.dataset_prepared_at}`}
                {progress.main_rows != null && ` · mains: ${progress.main_rows}`}
                {progress.reintegro_rows != null &&
                  ` · reintegro: ${progress.reintegro_rows}`}
              </Descriptions.Item>
              <Descriptions.Item label="2. Modelos">
                {progress.models_trained ? (
                  <Tag color="success">Hecho</Tag>
                ) : (
                  <Tag>Pendiente</Tag>
                )}
                {progress.trained_at && ` · ${progress.trained_at}`}
                {progress.main_accuracy != null &&
                  ` · mains: ${(progress.main_accuracy * 100).toFixed(2)}%`}
                {progress.reintegro_accuracy != null &&
                  ` · reintegro: ${(progress.reintegro_accuracy * 100).toFixed(2)}%`}
              </Descriptions.Item>
              <Descriptions.Item label="3. Probabilidades">
                {progress.probs_computed ? (
                  <Tag color="success">Hecho</Tag>
                ) : (
                  <Tag>Pendiente</Tag>
                )}
                {progress.probs_fecha_sorteo && ` · ${progress.probs_fecha_sorteo}`}
              </Descriptions.Item>
              <Descriptions.Item label="4. Generar pool">
                {progress.rules_applied ? (
                  <Tag color="success">Hecho</Tag>
                ) : (
                  <Tag>Pendiente</Tag>
                )}
                {(progress.filtered_mains_probs?.length ?? 0) > 0 ||
                (progress.filtered_reintegro_probs?.length ?? 0) > 0
                  ? ` · main: ${progress.filtered_mains_probs?.length ?? 0} pool, reintegro: ${progress.filtered_reintegro_probs?.length ?? 0} pool`
                  : ''}
              </Descriptions.Item>
              <Descriptions.Item label="5. Pool de candidatos">
                {(progress.candidate_pool_count ?? 0) > 0 ? (
                  <Tag color="success">Hecho</Tag>
                ) : (
                  <Tag>Pendiente</Tag>
                )}
                {progress.candidate_pool_count != null &&
                  ` · ${progress.candidate_pool_count} boletos`}
              </Descriptions.Item>
            </Descriptions>
            {(progress.filtered_mains_probs?.length ||
              progress.filtered_reintegro_probs?.length) && (
              <div style={{ marginTop: 16 }}>
                <div style={{ marginBottom: 8, fontWeight: 600 }}>Pool filtrado</div>
                <p style={{ margin: 0, fontSize: '0.9rem' }}>
                  Mains ({progress.filtered_mains_probs?.length ?? 0}):{' '}
                  {progress.filtered_mains_probs
                    ?.map((x) => x.number)
                    .join(' ') || '—'}
                </p>
                <p style={{ margin: '4px 0 0', fontSize: '0.9rem' }}>
                  Reintegro ({progress.filtered_reintegro_probs?.length ?? 0}):{' '}
                  {progress.filtered_reintegro_probs
                    ?.map((x) => x.number)
                    .join(' ') || '—'}
                </p>
              </div>
            )}
            {(progress.candidate_pool?.length ?? 0) > 0 && (
              <div style={{ marginTop: 16 }}>
                <div style={{ marginBottom: 8, fontWeight: 600 }}>
                  Muestra del pool (
                  {Math.min(10, progress.candidate_pool!.length)} primeros)
                </div>
                <Table
                  size="small"
                  dataSource={progress.candidate_pool!
                    .slice(0, 10)
                    .map((t, i) => ({
                      key: i,
                      mains: (t.mains ?? []).join(' '),
                      reintegro: t.reintegro,
                    }))}
                  columns={[
                    { title: 'Mains', dataIndex: 'mains' },
                    { title: 'Reintegro', dataIndex: 'reintegro', width: 80 },
                  ]}
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

