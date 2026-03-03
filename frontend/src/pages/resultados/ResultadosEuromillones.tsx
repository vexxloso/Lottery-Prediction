import { useSearchParams } from 'react-router-dom';
import { ResultadosPage } from './ResultadosPage';
import { EuromillonesFeaturesPanel } from './EuromillonesFeaturesPanel';
import { EuromillonesApuestasPanel } from './EuromillonesApuestasPanel';

type EuromillonesTab = 'results' | 'prediction' | 'grafico';

export function ResultadosEuromillones() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = (searchParams.get('tab') as EuromillonesTab | null) ?? 'results';
  const activeTab: EuromillonesTab =
    tabParam === 'prediction' || tabParam === 'grafico' ? tabParam : 'results';

  const setActiveTab = (tab: EuromillonesTab) => {
    const params = new URLSearchParams(searchParams);
    params.set('tab', tab);
    setSearchParams(params, { replace: true });
  };

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
      </div>

      <div className="resultados-tab-content">
        {activeTab === 'results' && <ResultadosPage lottery="euromillones" />}
        {activeTab === 'prediction' && (
          <div className="resultados-euromillones-features">
            <EuromillonesFeaturesPanel />
          </div>
        )}
        {activeTab === 'grafico' && (
          <div className="resultados-euromillones-features">
            <EuromillonesApuestasPanel />
          </div>
        )}
      </div>
    </div>
  );
}
