import { useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ResultadosPage } from './ResultadosPage';
import { ElGordoApuestasPanel } from './ElGordoApuestasPanel';
import { ElGordoBettingPanel } from './ElGordoBettingPanel';
import { ElGordoFeatureModelPanel } from './ElGordoFeatureModelPanel';
import { ElGordoPredictionPage } from './ElGordoPredictionPage';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

type ElGordoTab = 'results' | 'prediction' | 'grafico' | 'betting';

export function ResultadosElGordo() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = (searchParams.get('tab') as ElGordoTab | null) ?? 'results';
  const activeTab: ElGordoTab =
    tabParam === 'prediction' || tabParam === 'grafico' || tabParam === 'betting' ? tabParam : 'results';
  const hasCutoffDraw = !!searchParams.get('cutoff_draw_id');

  const setActiveTab = useCallback(
    (tab: ElGordoTab) => {
      const params = new URLSearchParams(searchParams);
      params.set('tab', tab);
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams]
  );

  const setActiveTabApuestas = useCallback(async () => {
    const params = new URLSearchParams(searchParams);
    let drawDate = params.get('draw_date');
    if (!drawDate) {
      try {
        const res = await fetch(`${API_URL}/api/el-gordo/betting/last-draw-date`, { cache: 'no-store' });
        const data = await res.json();
        if (data?.last_draw_date) drawDate = data.last_draw_date;
      } catch {
        /* ignore */
      }
    }
    params.set('tab', 'betting');
    if (drawDate) params.set('draw_date', drawDate);
    setSearchParams(params, { replace: true });
  }, [searchParams, setSearchParams]);

  return (
    <div className="resultados-euromillones-layout">
      <div className="resultados-tabs" role="tablist" aria-label="El Gordo">
        <button
          type="button"
          className={`resultados-tab ${activeTab === 'results' ? 'resultados-tab--active' : ''}`}
          role="tab"
          aria-selected={activeTab === 'results'}
          onClick={() => setActiveTab('results')}
        >
          Resultados
        </button>
        <button
          type="button"
          className={`resultados-tab ${activeTab === 'prediction' ? 'resultados-tab--active' : ''}`}
          role="tab"
          aria-selected={activeTab === 'prediction'}
          onClick={() => setActiveTab('prediction')}
        >
          Predicción
        </button>
        <button
          type="button"
          className={`resultados-tab ${activeTab === 'grafico' ? 'resultados-tab--active' : ''}`}
          role="tab"
          aria-selected={activeTab === 'grafico'}
          onClick={() => setActiveTab('grafico')}
        >
          Gráfico
        </button>
        <button
          type="button"
          className={`resultados-tab ${activeTab === 'betting' ? 'resultados-tab--active' : ''}`}
          role="tab"
          aria-selected={activeTab === 'betting'}
          onClick={setActiveTabApuestas}
        >
          Apuestas
        </button>
      </div>

      <div className="resultados-tab-content">
        {activeTab === 'results' && <ResultadosPage lottery="el-gordo" />}
        {activeTab === 'prediction' && (
          <div className="resultados-euromillones-features">
            {hasCutoffDraw ? <ElGordoPredictionPage /> : <ElGordoFeatureModelPanel />}
          </div>
        )}
        {activeTab === 'grafico' && (
          <div className="resultados-euromillones-features">
            <ElGordoApuestasPanel />
          </div>
        )}
        {activeTab === 'betting' && (
          <div className="resultados-euromillones-features">
            <ElGordoBettingPanel />
          </div>
        )}
      </div>
    </div>
  );
}
