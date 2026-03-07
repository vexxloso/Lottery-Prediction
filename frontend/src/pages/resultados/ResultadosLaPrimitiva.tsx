import { useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ResultadosPage } from './ResultadosPage';
import { LaPrimitivaApuestasPanel } from './LaPrimitivaApuestasPanel';
import { LaPrimitivaFeatureModelPanel } from './LaPrimitivaFeatureModelPanel';
import { LaPrimitivaPredictionPage } from './LaPrimitivaPredictionPage';
import { LaPrimitivaBettingPanel } from './LaPrimitivaBettingPanel';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

type LaPrimitivaTab = 'results' | 'prediction' | 'grafico' | 'apuestas';

export function ResultadosLaPrimitiva() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = (searchParams.get('tab') as LaPrimitivaTab | null) ?? 'results';
  const activeTab: LaPrimitivaTab =
    tabParam === 'prediction' || tabParam === 'grafico' || tabParam === 'apuestas' ? tabParam : 'results';

  const setActiveTab = useCallback(
    (tab: LaPrimitivaTab) => {
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
        const res = await fetch(`${API_URL}/api/la-primitiva/betting/last-draw-date`, { cache: 'no-store' });
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
      <div className="resultados-tabs" role="tablist" aria-label="La Primitiva">
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
      </div>

      <div className="resultados-tab-content">
        {activeTab === 'results' && <ResultadosPage lottery="la-primitiva" />}
        {activeTab === 'prediction' && (
          <div className="resultados-euromillones-features">
            {searchParams.get('cutoff_draw_id') ? (
              <LaPrimitivaPredictionPage />
            ) : (
              <LaPrimitivaFeatureModelPanel />
            )}
          </div>
        )}
        {activeTab === 'grafico' && (
          <div className="resultados-euromillones-features">
            <LaPrimitivaApuestasPanel />
          </div>
        )}
        {activeTab === 'apuestas' && (
          <div className="resultados-euromillones-features">
            <LaPrimitivaBettingPanel />
          </div>
        )}
      </div>
    </div>
  );
}
