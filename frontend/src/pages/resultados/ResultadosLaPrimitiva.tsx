import { useSearchParams } from 'react-router-dom';
import { ResultadosPage } from './ResultadosPage';
import { LaPrimitivaApuestasPanel } from './LaPrimitivaApuestasPanel';
import { LaPrimitivaFeatureModelPanel } from './LaPrimitivaFeatureModelPanel';
import { LaPrimitivaPredictionPage } from './LaPrimitivaPredictionPage';

type LaPrimitivaTab = 'results' | 'prediction' | 'grafico';

export function ResultadosLaPrimitiva() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = (searchParams.get('tab') as LaPrimitivaTab | null) ?? 'results';
  const activeTab: LaPrimitivaTab =
    tabParam === 'prediction' || tabParam === 'grafico' ? tabParam : 'results';

  const setActiveTab = (tab: LaPrimitivaTab) => {
    const params = new URLSearchParams(searchParams);
    params.set('tab', tab);
    setSearchParams(params, { replace: true });
  };

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
      </div>
    </div>
  );
}
