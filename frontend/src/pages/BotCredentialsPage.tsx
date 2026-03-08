import { useCallback, useEffect, useState } from 'react';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

interface BotCredential {
  id: string;
  username: string;
  is_active: boolean;
  order: number;
  created_at?: string;
}

export function BotCredentialsPage() {
  const [items, setItems] = useState<BotCredential[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [addLoading, setAddLoading] = useState(false);

  const fetchList = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const res = await fetch(`${API_URL}/api/bot/credentials`, { cache: 'no-store' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail ?? res.statusText);
      setItems(Array.isArray(data.items) ? data.items : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al cargar');
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchList();
  }, [fetchList]);

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    const u = username.trim();
    const p = password.trim();
    if (!u || !p) {
      setError('Usuario y contraseña obligatorios');
      return;
    }
    setAddLoading(true);
    setError('');
    try {
      const res = await fetch(`${API_URL}/api/bot/credentials`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: u, password: p }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail ?? res.statusText);
      setUsername('');
      setPassword('');
      fetchList();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al añadir');
    } finally {
      setAddLoading(false);
    }
  };

  const setActive = async (id: string) => {
    setError('');
    try {
      const res = await fetch(`${API_URL}/api/bot/credentials/${id}`, {
        method: 'PATCH',
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail ?? res.statusText);
      }
      fetchList();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al activar');
    }
  };

  const remove = async (id: string) => {
    if (!confirm('¿Eliminar esta cuenta?')) return;
    setError('');
    try {
      const res = await fetch(`${API_URL}/api/bot/credentials/${id}`, { method: 'DELETE' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail ?? res.statusText);
      }
      fetchList();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al eliminar');
    }
  };

  return (
    <section className="card" style={{ maxWidth: 560 }}>
        <h2 style={{ marginTop: 0, marginBottom: 'var(--space-md)' }}>
          Cuentas bot (Loterías)
        </h2>
        <p style={{ fontSize: '0.9rem', color: 'var(--color-text-muted)', marginBottom: 16 }}>
          Añade varias cuentas de loteriasyapuestas.es. La cuenta <strong>activa</strong> es la que usa el bot al comprar. La primera que añadas queda activa por defecto.
        </p>

        {error && (
          <p style={{ color: 'var(--color-error)', marginBottom: 16 }} role="alert">
            {error}
          </p>
        )}

        <form onSubmit={handleAdd} style={{ marginBottom: 24, display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'flex-end' }}>
          <label style={{ flex: '1 1 120px', minWidth: 0 }}>
            <span className="sr-only">Usuario (email o NIF)</span>
            <input
              type="text"
              placeholder="Usuario (email o NIF)"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="username"
              style={{ width: '100%', padding: '8px 12px' }}
            />
          </label>
          <label style={{ flex: '1 1 120px', minWidth: 0 }}>
            <span className="sr-only">Contraseña</span>
            <input
              type="password"
              placeholder="Contraseña"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="new-password"
              style={{ width: '100%', padding: '8px 12px' }}
            />
          </label>
          <button type="submit" disabled={addLoading} style={{ padding: '8px 16px' }}>
            {addLoading ? 'Añadiendo…' : 'Añadir cuenta'}
          </button>
        </form>

        <h3 style={{ marginBottom: 8, fontSize: '1rem' }}>Cuentas</h3>
        {loading ? (
          <p>Cargando…</p>
        ) : items.length === 0 ? (
          <p style={{ color: 'var(--color-text-muted)' }}>No hay cuentas. Añade la primera arriba.</p>
        ) : (
          <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
            {items.map((item) => (
              <li
                key={item.id}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 12,
                  padding: '10px 12px',
                  border: '1px solid var(--color-border)',
                  borderRadius: 8,
                  marginBottom: 8,
                }}
              >
                <span style={{ flex: 1, fontWeight: item.is_active ? 600 : 400 }}>
                  {item.username}
                </span>
                {item.is_active ? (
                  <span style={{ fontSize: '0.85rem', color: 'var(--color-success)', fontWeight: 600 }}>
                    Activa
                  </span>
                ) : (
                  <button
                    type="button"
                    onClick={() => setActive(item.id)}
                    style={{ padding: '4px 10px', fontSize: '0.85rem' }}
                  >
                    Usar esta
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => remove(item.id)}
                  aria-label="Eliminar cuenta"
                  style={{ padding: 4, color: 'var(--color-text-muted)' }}
                >
                  🗑
                </button>
              </li>
            ))}
          </ul>
        )}
    </section>
  );
}
