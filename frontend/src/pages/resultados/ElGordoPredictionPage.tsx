import { useCallback, useEffect, useState } from 'react';
import { Card, Descriptions, Drawer, notification, Select, Spin, Steps, Table, Tag } from 'antd';
import { useSearchParams } from 'react-router-dom';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

interface ProbRow {
  number: number;
  p: number;
}

interface TrainProgressElGordo {
  cutoff_draw_id: string;
  dataset_prepared: boolean;
  dataset_prepared_at?: string;
  main_rows?: number;
  clave_rows?: number;
  models_trained: boolean;
  trained_at?: string;
  main_accuracy?: number;
  clave_accuracy?: number;
  probs_computed?: boolean;
  probs_computed_at?: string;
  mains_probs?: ProbRow[];
  clave_probs?: ProbRow[];
  probs_draw_id?: string;
  probs_fecha_sorteo?: string;
  rules_applied?: boolean;
  rules_applied_at?: string;
  filtered_mains_probs?: ProbRow[];
  filtered_clave_probs?: ProbRow[];
  rule_flags?: {
    rules_used?: string[];
    snapshot_mains?: number[];
    snapshot_clave?: number[];
  };
  candidate_pool?: { mains: number[]; clave: number }[];
  candidate_pool_at?: string;
  candidate_pool_count?: number;
}

export function ElGordoPredictionPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const cutoffDrawId = searchParams.get('cutoff_draw_id');
  const [progress, setProgress] = useState<TrainProgressElGordo | null>(null);
  const [progressLoading, setProgressLoading] = useState(false);
  const [runAllLoading, setRunAllLoading] = useState(false);
  const [runningStep, setRunningStep] = useState<number>(0);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerWidth, setDrawerWidth] = useState(420);
  const [currentDraw, setCurrentDraw] = useState<{ date: string; mains: number[]; clave: number | null } | null>(null);
  const [candidateDisplayCount, setCandidateDisplayCount] = useState(20);
  const CANDIDATE_COUNT_OPTIONS = [10, 20, 30, 50, 100, 300, 500, 1000, 2000, 3000];

  useEffect(() => {
    if (!cutoffDrawId) {
      setCurrentDraw(null);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(
          `${API_URL}/api/el-gordo/feature-model?draw_id=${encodeURIComponent(cutoffDrawId)}`
        );
        const data = await res.json();
        if (cancelled || !data.features?.length) {
          if (!cancelled) setCurrentDraw(null);
          return;
        }
        const f = data.features[0];
        const fecha = (f.fecha_sorteo || '').toString().trim();
        const date = fecha.split(' ')[0] || fecha;
        const mains = Array.isArray(f.main_number) ? f.main_number.map(Number).filter((n: number) => !Number.isNaN(n)) : [];
        const clave = typeof f.clave === 'number' ? f.clave : null;
        if (!cancelled) setCurrentDraw({ date, mains, clave });
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
      const url = `${API_URL}/api/el-gordo/train/progress?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}${cacheBust ? `&_t=${Date.now()}` : ''}`;
      const res = await fetch(url, { cache: 'no-store' });
      const data = await res.json();
      setProgress((data.progress as TrainProgressElGordo) ?? null);
    } catch {
      setProgress(null);
    } finally {
      setProgressLoading(false);
    }
  }, [cutoffDrawId]);

  useEffect(() => {
    fetchProgress();
  }, [fetchProgress]);

  const currentStep = progress == null
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
      // These endpoints will be implemented for El Gordo; for now we still wire the flow
      let res = await fetch(`${API_URL}/api/el-gordo/train/prepare-dataset${qs}`, { method: 'POST' });
      let data = await res.json();
      if (!res.ok || data.status !== 'ok') throw new Error((data as any).detail ?? 'Error preparando dataset');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(1);

      res = await fetch(`${API_URL}/api/el-gordo/train/models${qs}`, { method: 'POST' });
      data = await res.json();
      if (!res.ok || data.status !== 'ok') throw new Error((data as any).detail ?? 'Error entrenando modelos');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(2);

      res = await fetch(`${API_URL}/api/el-gordo/prediction/ml${qs}`, { method: 'GET' });
      data = await res.json();
      if (!res.ok || data.status !== 'ok') throw new Error((data as any).detail ?? 'Error calculando probabilidades');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(3);

      res = await fetch(`${API_URL}/api/el-gordo/train/rule-filters?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}`, { method: 'POST' });
      data = await res.json();
      if (!res.ok) throw new Error((data as { detail?: string }).detail ?? 'Error generando pool de números');
      await fetchProgress(true);
      await delay(3000);
      setRunningStep(4);

      res = await fetch(`${API_URL}/api/el-gordo/train/candidate-pool?cutoff_draw_id=${encodeURIComponent(cutoffDrawId)}&num_tickets=3000`, { method: 'POST' });
      data = await res.json();
      if (!res.ok) throw new Error((data as { detail?: string }).detail ?? 'Error generando pool de boletos');
      await fetchProgress(true);
      notification.success({ message: 'Pipeline El Gordo completado', description: 'Pasos 1 a 5 ejecutados para El Gordo. Progreso guardado en la base de datos.', placement: 'topRight', duration: 4 });
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Error en el pipeline de El Gordo';
      notification.error({ message: 'Error', description: msg, placement: 'topRight', duration: 5 });
    } finally {
      setRunAllLoading(false);
    }
  };

  const displayStep = runAllLoading ? runningStep : currentStep;
  const stepItems = [
    { title: 'Preparar dataset', status: displayStep === 0 && !progress?.dataset_prepared ? 'process' as const : progress?.dataset_prepared ? 'finish' as const : 'wait' as const },
    { title: 'Entrenar modelos', status: displayStep === 1 && !progress?.models_trained ? 'process' as const : progress?.models_trained ? 'finish' as const : 'wait' as const },
    { title: 'Probabilidades', status: displayStep === 2 && !progress?.probs_computed ? 'process' as const : progress?.probs_computed ? 'finish' as const : 'wait' as const },
    { title: 'Generar pool', status: displayStep === 3 && !progress?.rules_applied ? 'process' as const : progress?.rules_applied ? 'finish' as const : 'wait' as const },
    { title: 'Pool de candidatos', status: (progress?.candidate_pool_count ?? 0) > 0 ? 'finish' as const : displayStep === 4 ? 'process' as const : 'wait' as const },
  ];

  return (
    <section className="resultados-features-card resultados-theme-el-gordo euromillones-train-split">
      <div
        className="euromillones-train-layout"
        style={{
          display: 'flex',
          flexDirection: 'row',
          gap: 'var(--space-md)',
          alignItems: 'flex-start',
        }}
      >
        {/* Left: ticket pool preview */}
        <div style={{ flex: '17 1 0%', minWidth: 0, display: 'flex', flexDirection: 'column' }} className="euromillones-train-table-col">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, marginBottom: 8, flexWrap: 'wrap' }}>
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
                Pool de candidatos El Gordo{progress?.candidate_pool_count != null ? `: ${progress.candidate_pool_count} boletos` : ''}
              </h4>
            </div>
            <span style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>
              Mostrar:
              <Select
                value={candidateDisplayCount}
                onChange={setCandidateDisplayCount}
                options={CANDIDATE_COUNT_OPTIONS.map((n) => ({ label: String(n), value: n }))}
                style={{ width: 90 }}
                size="small"
              />
            </span>
          </div>
          <Table
            size="small"
            dataSource={
              (progress?.candidate_pool ?? [])
                .slice(0, candidateDisplayCount)
                .map((t, i) => ({
                  key: i,
                  index: i + 1,
                  mainsStr: (t.mains ?? []).join(' '),
                  claveStr: typeof t.clave === 'number' ? String(t.clave) : '',
                }))
            }
            columns={[
              { title: '#', dataIndex: 'index', key: 'index', width: 56 },
              { title: 'Mains', dataIndex: 'mainsStr', key: 'mains' },
              { title: 'Clave', dataIndex: 'claveStr', key: 'clave', width: 80 },
            ]}
            pagination={false}
            scroll={{ x: 320 }}
            locale={{ emptyText: 'Sin datos. Pulsa "Generar todo" para ejecutar el pipeline de El Gordo.' }}
          />
        </div>

        {/* Right: steps + current draw */}
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
                      Ejecutando… (paso {displayStep + 1}/5)
                    </>
                  ) : (
                    'Generar todo'
                  )}
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
                <span style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', marginRight: 4 }}>Clave:</span>
                {currentDraw.clave != null && (
                  <span className="resultados-ball resultados-train-draw-ball" style={{ width: 26, height: 26, fontSize: '0.75rem' }}>{currentDraw.clave}</span>
                )}
              </div>
            </Card>
          )}
        </div>
      </div>

      <Drawer
        className="euromillones-progress-drawer"
        title="Progreso del pipeline El Gordo"
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
                {progress.clave_rows != null && ` · Clave: ${progress.clave_rows} filas`}
              </Descriptions.Item>
              <Descriptions.Item label="2. Entrenar modelos">
                {progress.models_trained ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.main_accuracy != null && ` · Accuracy mains: ${(progress.main_accuracy * 100).toFixed(2)}%`}
                {progress.clave_accuracy != null && ` · clave: ${(progress.clave_accuracy * 100).toFixed(2)}%`}
              </Descriptions.Item>
              <Descriptions.Item label="3. Probabilidades">
                {progress.probs_computed ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.probs_fecha_sorteo && ` · ${progress.probs_fecha_sorteo}`}
              </Descriptions.Item>
              <Descriptions.Item label="4. Generar pool">
                {progress.rules_applied ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.rule_flags?.rules_used?.length ? ` · ${progress.rule_flags.rules_used.join(', ')}` : ''}
              </Descriptions.Item>
              <Descriptions.Item label="5. Pool de candidatos">
                {(progress.candidate_pool_count ?? 0) > 0 ? <Tag color="success">Hecho</Tag> : <Tag>Pendiente</Tag>}
                {progress.candidate_pool_count != null && ` · ${progress.candidate_pool_count} boletos`}
              </Descriptions.Item>
            </Descriptions>
            {(progress.rule_flags?.snapshot_mains?.length || progress.rule_flags?.snapshot_clave?.length) && (
              <div style={{ marginTop: 16 }}>
                <div style={{ marginBottom: 8, fontWeight: 600 }}>Snapshot Step 4 (debug)</div>
                <p style={{ margin: 0, fontSize: '0.9rem' }}>
                  Mains ({progress.rule_flags?.snapshot_mains?.length ?? 0}):{' '}
                  {progress.rule_flags?.snapshot_mains?.join(' ') || '—'}
                  {progress.rule_flags?.snapshot_clave != null && (
                    <>
                      {' · '}
                      <span style={{ opacity: 0.8 }}>
                        Clave:{' '}
                        {progress.rule_flags.snapshot_clave.join
                          ? (progress.rule_flags.snapshot_clave as number[]).join(' ')
                          : String(progress.rule_flags.snapshot_clave)}
                      </span>
                    </>
                  )}
                </p>
              </div>
            )}
            {(progress.filtered_mains_probs?.length || progress.filtered_clave_probs?.length) ? (
              <div style={{ marginTop: 16 }}>
                <div style={{ marginBottom: 8, fontWeight: 600 }}>Pool filtrado</div>
                <p style={{ margin: 0, fontSize: '0.9rem' }}>
                  Mains ({progress.filtered_mains_probs?.length ?? 0}):{' '}
                  {progress.filtered_mains_probs?.map((x) => x.number).join(' ') || '—'}
                </p>
                <p style={{ margin: '4px 0 0', fontSize: '0.9rem' }}>
                  Clave ({progress.filtered_clave_probs?.length ?? 0}):{' '}
                  {progress.filtered_clave_probs?.map((x) => x.number).join(' ') || '—'}
                </p>
              </div>
            ) : null}
            {(progress.candidate_pool?.length ?? 0) > 0 && (
              <div style={{ marginTop: 16 }}>
                <div style={{ marginBottom: 8, fontWeight: 600 }}>Muestra del pool ({Math.min(10, progress.candidate_pool!.length)} primeros)</div>
                <Table
                  size="small"
                  dataSource={progress.candidate_pool!.slice(0, 10).map((t, i) => ({ key: i, mains: (t.mains ?? []).join(' '), clave: t.clave }))}
                  columns={[{ title: 'Mains', dataIndex: 'mains' }, { title: 'Clave', dataIndex: 'clave', width: 80 }]}
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

