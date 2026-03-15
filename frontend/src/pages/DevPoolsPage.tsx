/**
 * Dev-only page: list and delete TXT files in el_gordo_pools, euromillones_pools, la_primitiva_pools.
 * Not linked anywhere in the app — only reachable if you know the URL: /dev/pools
 */
import { useCallback, useEffect, useState } from 'react';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

type PoolFile = { name: string; size: number | null; modified: string | null };
type PoolsData = Record<string, PoolFile[]>;

const LOTTERY_LABELS: Record<string, string> = {
  el_gordo: 'El Gordo',
  euromillones: 'Euromillones',
  la_primitiva: 'La Primitiva',
};

export function DevPoolsPage() {
  const [pools, setPools] = useState<PoolsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [deleting, setDeleting] = useState<string | 'all' | null>(null);
  const [deletingFile, setDeletingFile] = useState<string | null>(null);

  const fetchPools = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const res = await fetch(`${API_URL}/api/dev/pools`, { cache: 'no-store' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail ?? res.statusText ?? 'Failed to list pools');
      setPools(data as PoolsData);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error loading pools');
      setPools(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPools();
  }, [fetchPools]);

  const handleDelete = async (lottery: string | null) => {
    if (!confirm(lottery ? `Delete all TXT files in ${LOTTERY_LABELS[lottery]} pool folder?` : 'Delete all TXT files in all three pool folders?')) return;
    setDeleting(lottery ?? 'all');
    setError('');
    try {
      const url = lottery ? `${API_URL}/api/dev/pools?lottery=${encodeURIComponent(lottery)}` : `${API_URL}/api/dev/pools`;
      const res = await fetch(url, { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail ?? res.statusText ?? 'Delete failed');
      await fetchPools();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Delete failed');
    } finally {
      setDeleting(null);
    }
  };

  const handleDeleteOne = async (lottery: string, fileName: string) => {
    if (!confirm(`Delete ${fileName}?`)) return;
    const key = `${lottery}:${fileName}`;
    setDeletingFile(key);
    setError('');
    try {
      const url = `${API_URL}/api/dev/pools?lottery=${encodeURIComponent(lottery)}&file=${encodeURIComponent(fileName)}`;
      const res = await fetch(url, { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail ?? res.statusText ?? 'Delete failed');
      await fetchPools();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Delete failed');
    } finally {
      setDeletingFile(null);
    }
  };

  if (loading && !pools) {
    return (
      <section style={{ padding: 24 }}>
        <p>Cargando…</p>
      </section>
    );
  }

  return (
    <section style={{ padding: 24, maxWidth: 900 }}>
      <h1 style={{ marginTop: 0, marginBottom: 16, fontSize: '1.25rem' }}>
        Dev: Pool TXT folders
      </h1>
      <p style={{ color: 'var(--color-text-muted)', fontSize: '0.9rem', marginBottom: 24 }}>
        Delete .txt files in <code>el_gordo_pools</code>, <code>euromillones_pools</code>, <code>la_primitiva_pools</code>. This page is not linked in the app.
      </p>
      {error && (
        <p style={{ color: 'var(--color-error)', marginBottom: 16 }}>{error}</p>
      )}
      {pools && (
        <>
          {(['el_gordo', 'euromillones', 'la_primitiva'] as const).map((lottery) => {
            const files = pools[lottery] ?? [];
            const isDeleting = deleting === lottery || deleting === 'all';
            return (
              <div
                key={lottery}
                style={{
                  border: '1px solid var(--color-border)',
                  borderRadius: 8,
                  padding: 16,
                  marginBottom: 16,
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                  <strong>{LOTTERY_LABELS[lottery]}</strong>
                  <button
                    type="button"
                    disabled={isDeleting || files.length === 0}
                    onClick={() => handleDelete(lottery)}
                    style={{
                      padding: '4px 12px',
                      fontSize: '0.85rem',
                      cursor: isDeleting || files.length === 0 ? 'not-allowed' : 'pointer',
                    }}
                  >
                    {isDeleting ? 'Deleting…' : `Delete ${files.length} file(s)`}
                  </button>
                </div>
                {files.length === 0 ? (
                  <p style={{ margin: 0, fontSize: '0.9rem', color: 'var(--color-text-muted)' }}>No .txt files</p>
                ) : (
                  <ul style={{ margin: 0, paddingLeft: 20, fontSize: '0.9rem', listStyle: 'none' }}>
                    {files.map((f) => {
                      const fileKey = `${lottery}:${f.name}`;
                      const isDeletingOne = deletingFile === fileKey;
                      return (
                        <li
                          key={f.name}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between',
                            gap: 8,
                            marginBottom: 4,
                          }}
                        >
                          <span>
                            {f.name}
                            {f.size != null && ` (${(f.size / 1024).toFixed(1)} KB)`}
                            {f.modified && ` · ${f.modified.slice(0, 10)}`}
                          </span>
                          <button
                            type="button"
                            disabled={isDeletingOne}
                            onClick={() => handleDeleteOne(lottery, f.name)}
                            style={{
                              padding: '2px 8px',
                              fontSize: '0.8rem',
                              cursor: isDeletingOne ? 'not-allowed' : 'pointer',
                              flexShrink: 0,
                            }}
                          >
                            {isDeletingOne ? '…' : 'Delete'}
                          </button>
                        </li>
                      );
                    })}
                  </ul>
                )}
              </div>
            );
          })}
          <div style={{ marginTop: 24 }}>
            <button
              type="button"
              disabled={deleting !== null}
              onClick={() => handleDelete(null)}
              style={{
                padding: '8px 16px',
                background: 'var(--color-error, #c00)',
                color: '#fff',
                border: 'none',
                borderRadius: 6,
                cursor: deleting ? 'not-allowed' : 'pointer',
              }}
            >
              {deleting === 'all' ? 'Deleting…' : 'Delete all TXT in all three folders'}
            </button>
          </div>
        </>
      )}
    </section>
  );
}
