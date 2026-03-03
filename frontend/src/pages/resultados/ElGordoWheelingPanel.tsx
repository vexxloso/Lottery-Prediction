import { useState } from 'react';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

interface ElGordoTicket {
  mains: number[];
  clave: number;
}

export function ElGordoWheelingPanel() {
  const [cutoffDrawId, setCutoffDrawId] = useState('');
  const [kMain, setKMain] = useState(20);
  const [kClave, setKClave] = useState(6);
  const [mainFreqW, setMainFreqW] = useState(0.4);
  const [mainGapW, setMainGapW] = useState(0.3);
  const [mainHotW, setMainHotW] = useState(0.3);
  const [claveFreqW, setClaveFreqW] = useState(0.4);
  const [claveGapW, setClaveGapW] = useState(0.3);
  const [claveHotW, setClaveHotW] = useState(0.3);

  const [poolLoading, setPoolLoading] = useState(false);
  const [poolError, setPoolError] = useState('');
  const [candidatePool, setCandidatePool] = useState<any | null>(null);

  const [wheelLoading, setWheelLoading] = useState(false);
  const [wheelError, setWheelError] = useState('');
  const [wheelTickets, setWheelTickets] = useState<ElGordoTicket[] | null>(null);
  const [wheelCount, setWheelCount] = useState(20);

  const handleFloat = (setter: (v: number) => void) => (e: React.ChangeEvent<HTMLInputElement>) => {
    setter(Number(e.target.value) || 0);
  };

  const handleInt = (setter: (v: number) => void) => (e: React.ChangeEvent<HTMLInputElement>) => {
    setter(Math.max(1, Number(e.target.value) || 1));
  };

  const runPool = async () => {
    if (!cutoffDrawId) {
      setPoolError('Introduce un id_sorteo como cutoff_draw_id.');
      return;
    }
    try {
      setPoolLoading(true);
      setPoolError('');
      setCandidatePool(null);
      const params = new URLSearchParams();
      params.set('cutoff_draw_id', cutoffDrawId);
      params.set('k_main', String(kMain));
      params.set('k_clave', String(kClave));
      params.set('w_freq_main', String(mainFreqW));
      params.set('w_gap_main', String(mainGapW));
      params.set('w_hot_main', String(mainHotW));
      params.set('w_freq_clave', String(claveFreqW));
      params.set('w_gap_clave', String(claveGapW));
      params.set('w_hot_clave', String(claveHotW));
      const res = await fetch(`${API_URL}/api/el-gordo/simulation/candidate-pool?${params.toString()}`);
      const data = await res.json();
      if (!res.ok) {
        setPoolError(data.detail ?? res.statusText);
        return;
      }
      setCandidatePool(data);
    } catch (e) {
      setPoolError(
        e instanceof Error ? e.message : 'Error al generar el pool de candidatos de El Gordo',
      );
    } finally {
      setPoolLoading(false);
    }
  };

  const runWheeling = async () => {
    if (!cutoffDrawId) {
      setWheelError('Introduce un id_sorteo como cutoff_draw_id.');
      return;
    }
    try {
      setWheelLoading(true);
      setWheelError('');
      setWheelTickets(null);
      const params = new URLSearchParams();
      params.set('cutoff_draw_id', cutoffDrawId);
      params.set('n_tickets', String(wheelCount));
      const res = await fetch(`${API_URL}/api/el-gordo/simulation/wheeling?${params.toString()}`, {
        method: 'POST',
      });
      const data = await res.json();
      if (!res.ok) {
        setWheelError(data.detail ?? res.statusText);
        return;
      }
      const tickets = (data.tickets ?? []) as { mains: number[]; clave: number }[];
      setWheelTickets(tickets);
    } catch (e) {
      setWheelError(
        e instanceof Error ? e.message : 'Error al generar boletos de wheeling de El Gordo',
      );
    } finally {
      setWheelLoading(false);
    }
  };

  return (
    <div style={{ marginTop: 'var(--space-lg)', width: '100%' }}>
      <section className="card resultados-features-card" style={{ width: '100%' }}>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: '1rem',
            flexWrap: 'wrap',
          }}
        >
          <div>
            <h3 style={{ marginTop: 0, marginBottom: '0.25rem' }}>Pool de candidatos (El Gordo)</h3>
            <p style={{ margin: 0, fontSize: '0.9rem', color: '#4b5563' }}>
              Construye un pool de 20 números principales (1–54) y 6 números clave (0–9) a partir
              de los modelos de predicción.
            </p>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <input
              type="text"
              className="form-input"
              placeholder="cutoff_draw_id"
              value={cutoffDrawId}
              onChange={(e) => setCutoffDrawId(e.target.value)}
              style={{ minWidth: '10rem' }}
            />
            <button
              type="button"
              className="primary"
              disabled={poolLoading}
              onClick={runPool}
              style={{ minWidth: '9rem' }}
            >
              {poolLoading ? 'Generando…' : 'Generar pool'}
            </button>
          </div>
        </div>

        <table
          className="resultados-features-table"
          style={{ marginTop: 'var(--space-md)', marginBottom: 'var(--space-md)' }}
        >
          <thead>
            <tr>
              <th style={{ width: '30%' }}>Parámetro</th>
              <th>Números principales</th>
              <th>Número clave</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Tamaño del pool</td>
              <td>
                <input
                  type="number"
                  min={1}
                  max={54}
                  value={kMain}
                  onChange={handleInt(setKMain)}
                  className="form-input"
                />
              </td>
              <td>
                <input
                  type="number"
                  min={1}
                  max={10}
                  value={kClave}
                  onChange={handleInt(setKClave)}
                  className="form-input"
                />
              </td>
            </tr>
            <tr>
              <td>Peso frecuencia</td>
              <td>
                <input
                  type="number"
                  step="0.05"
                  min={0}
                  max={1}
                  value={mainFreqW}
                  onChange={handleFloat(setMainFreqW)}
                  className="form-input"
                />
              </td>
              <td>
                <input
                  type="number"
                  step="0.05"
                  min={0}
                  max={1}
                  value={claveFreqW}
                  onChange={handleFloat(setClaveFreqW)}
                  className="form-input"
                />
              </td>
            </tr>
            <tr>
              <td>Peso gap</td>
              <td>
                <input
                  type="number"
                  step="0.05"
                  min={0}
                  max={1}
                  value={mainGapW}
                  onChange={handleFloat(setMainGapW)}
                  className="form-input"
                />
              </td>
              <td>
                <input
                  type="number"
                  step="0.05"
                  min={0}
                  max={1}
                  value={claveGapW}
                  onChange={handleFloat(setClaveGapW)}
                  className="form-input"
                />
              </td>
            </tr>
            <tr>
              <td>Peso hot/cold</td>
              <td>
                <input
                  type="number"
                  step="0.05"
                  min={0}
                  max={1}
                  value={mainHotW}
                  onChange={handleFloat(setMainHotW)}
                  className="form-input"
                />
              </td>
              <td>
                <input
                  type="number"
                  step="0.05"
                  min={0}
                  max={1}
                  value={claveHotW}
                  onChange={handleFloat(setClaveHotW)}
                  className="form-input"
                />
              </td>
            </tr>
          </tbody>
        </table>

        {poolError && (
          <p style={{ color: 'var(--color-error)', marginTop: '0.5rem' }}>{poolError}</p>
        )}

        {candidatePool && (
          <p style={{ marginTop: '0.5rem', fontSize: '0.9rem' }}>
            Pool actual:{' '}
            <strong>
              {(candidatePool.main_pool || []).join(' ')} | Clave:{' '}
              {(candidatePool.clave_pool || []).join(' ')}
            </strong>
          </p>
        )}
      </section>

      <section
        className="card resultados-features-card"
        style={{ marginTop: 'var(--space-lg)', width: '100%' }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: '1rem',
          }}
        >
          <div>
            <h3 style={{ marginTop: 0, marginBottom: '0.25rem' }}>Sistema Wheeling (El Gordo)</h3>
            <p style={{ margin: 0, fontSize: '0.9rem', color: '#4b5563' }}>
              Genera boletos de El Gordo (5 números + clave) a partir del pool de candidatos
              guardado.
            </p>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <label
              className="form-label"
              style={{ margin: 0, display: 'flex', alignItems: 'center', gap: '0.5rem' }}
            >
              <span>Nº boletos</span>
              <select
                className="form-input"
                value={wheelCount}
                onChange={(e) => setWheelCount(Math.max(1, Number(e.target.value) || 1))}
                style={{ width: '7rem' }}
              >
                {[10, 20, 30, 50, 100, 1000, 3000].map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
            </label>
            <button
              type="button"
              className="primary"
              disabled={wheelLoading}
              onClick={runWheeling}
              style={{ minWidth: '9rem' }}
            >
              {wheelLoading ? 'Generando…' : 'Generar boletos'}
            </button>
          </div>
        </div>

        {wheelError && (
          <p style={{ color: 'var(--color-error)', marginTop: '0.5rem' }}>{wheelError}</p>
        )}

        {wheelTickets && wheelTickets.length > 0 && (
          <div
            className="resultados-features-table-wrap"
            style={{ marginTop: 'var(--space-md)' }}
          >
            <table className="resultados-features-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Números principales</th>
                  <th>Clave</th>
                </tr>
              </thead>
              <tbody>
                {wheelTickets.slice(0, wheelCount).map((t, idx) => (
                  <tr key={`${idx}-${t.mains.join('-')}-${t.clave}`}>
                    <td>{idx + 1}</td>
                    <td>{t.mains.join(' ')}</td>
                    <td>{t.clave}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

