import { useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ResultadosPage } from './ResultadosPage';
import { EuromillonesFeaturesPanel } from './EuromillonesFeaturesPanel';
import { EuromillonesApuestasPanel } from './EuromillonesApuestasPanel';
import { EuromillonesPredictionPage } from './EuromillonesPredictionPage';
import { EuromillonesBettingPanel } from './EuromillonesBettingPanel';
import { EuromillonesAnalysisPage } from './EuromillonesAnalysisPage';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

 type EuromillonesTab = 'results' | 'prediction' | 'grafico' | 'apuestas' | 'analisis';

export function ResultadosEuromillones() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = (searchParams.get('tab') as EuromillonesTab | null) ?? 'results';
  const activeTab: EuromillonesTab =
    tabParam === 'prediction' || tabParam === 'grafico' || tabParam === 'apuestas' || tabParam === 'analisis'
      ? tabParam
      : 'results';
  const hasCutoffDraw = !!searchParams.get('cutoff_draw_id');

  const setActiveTab = useCallback(
    (tab: EuromillonesTab) => {
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
        const res = await fetch(`${API_URL}/api/euromillones/betting/last-draw-date`, { cache: 'no-store' });
        const data = await res.json();
        if (data?.last_draw_date) drawDate = data.last_draw_date;
      } catch {
        /* ignore */
      }
    }
    params.set('tab', 'apuestas');
    if (drawDate) params.set('draw_date', drawDate);
    setSearchParams(params, { replace: true });
  }, [searchParams, setSearchParams]);

  return (
    <div className="resultados-euromillones-layout">
      <div className="resultados-tabs" role="tablist" aria-label="Euromillones">
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
          className={`resultados-tab ${activeTab === 'apuestas' ? 'resultados-tab--active' : ''}`}
          role="tab"
          aria-selected={activeTab === 'apuestas'}
          onClick={setActiveTabApuestas}
        >
          Apuestas
        </button>
        <button
          type="button"
          className={`resultados-tab ${activeTab === 'analisis' ? 'resultados-tab--active' : ''}`}
          role="tab"
          aria-selected={activeTab === 'analisis'}
          onClick={() => setActiveTab('analisis')}
        >
          Análisis
        </button>
      </div>

      <div className="resultados-tab-content">
        {activeTab === 'results' && <ResultadosPage lottery="euromillones" />}
        {activeTab === 'prediction' && (
          <div className="resultados-euromillones-features">
            {hasCutoffDraw ? <EuromillonesPredictionPage /> : <EuromillonesFeaturesPanel />}
          </div>
        )}
        {activeTab === 'grafico' && (
          <div className="resultados-euromillones-features">
            <EuromillonesApuestasPanel />
          </div>
        )}
        {activeTab === 'apuestas' && (
          <div className="resultados-euromillones-features">
            <EuromillonesBettingPanel />
          </div>
        )}
        {activeTab === 'analisis' && (
          <div className="resultados-euromillones-features">
            <EuromillonesAnalysisPage />
          </div>
        )}
      </div>
    </div>
  );
}
