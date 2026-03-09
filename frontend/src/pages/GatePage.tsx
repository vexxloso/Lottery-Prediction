import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';
const AUTH_KEY = 'platform_auth';

export function GatePage() {
  const navigate = useNavigate();
  const [showPassword, setShowPassword] = useState(false);
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [time, setTime] = useState(() => new Date());

  const revealPassword = useCallback(() => setShowPassword(true), []);

  useEffect(() => {
    if (sessionStorage.getItem(AUTH_KEY) === '1') {
      navigate('/', { replace: true });
      return;
    }
    const t = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(t);
  }, [navigate]);

  useEffect(() => {
    if (showPassword) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Enter') revealPassword();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [showPassword, revealPassword]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const res = await fetch(`${API_URL}/api/auth/verify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password }),
      });
      const data = await res.json().catch(() => ({}));
      if (res.ok && data.ok) {
        sessionStorage.setItem(AUTH_KEY, '1');
        const token = data.token;
        if (token) sessionStorage.setItem('platform_token', token);
        navigate('/', { replace: true });
        return;
      }
      setError(res.status === 401 ? 'Contraseña incorrecta' : 'Error de conexión');
    } catch {
      setError('Error de conexión');
    } finally {
      setLoading(false);
    }
  };

  const timeStr = time.toLocaleTimeString('es-ES', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  const dateStr = time.toLocaleDateString('es-ES', {
    weekday: 'long',
    day: 'numeric',
    month: 'long',
    year: 'numeric',
  });

  return (
    <div
      className={`gate-page ${showPassword ? 'gate-page--password' : ''}`}
      onClick={!showPassword ? revealPassword : undefined}
      role={!showPassword ? 'button' : undefined}
      tabIndex={!showPassword ? 0 : undefined}
      aria-label={!showPassword ? 'Pulsa Entrar o haz clic para introducir contraseña' : undefined}
    >
      {!showPassword ? (
        <div className="gate-page__clock" aria-live="polite">
          <span className="gate-page__time">{timeStr}</span>
          <span className="gate-page__date">{dateStr}</span>
          <span className="gate-page__hint">Pulsa Entrar o haz clic para continuar</span>
        </div>
      ) : (
        <form className="gate-page__form" onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()}>
          <label htmlFor="gate-password" className="gate-page__label">
            Contraseña
          </label>
          <input
            id="gate-password"
            type="password"
            className="gate-page__input"
            value={password}
            onChange={(e) => {
              setPassword(e.target.value);
              setError('');
            }}
            placeholder="Introduce la contraseña"
            autoComplete="current-password"
            autoFocus
            disabled={loading}
          />
          {error && <p className="gate-page__error" role="alert">{error}</p>}
          <button type="submit" className="gate-page__submit" disabled={loading}>
            {loading ? 'Comprobando…' : 'Entrar'}
          </button>
        </form>
      )}
    </div>
  );
}

export function getIsAuthenticated(): boolean {
  return sessionStorage.getItem(AUTH_KEY) === '1';
}

export function setAuthenticated(value: boolean): void {
  if (value) sessionStorage.setItem(AUTH_KEY, '1');
  else sessionStorage.removeItem(AUTH_KEY);
}
