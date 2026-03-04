import { useCallback, useEffect, useState } from 'react';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

export type ApuestasWindow = '2m' | '3m' | '6m' | '1y' | 'all';

export interface ApuestasPoint {
  draw_id: string;
  date: string;
  apuestas: number | null;
  premios: number | null;
  premio_bote: number | null;
}

interface ApiResponse {
  points: ApuestasPoint[];
}

export function useApuestasSeries(
  lottery: 'euromillones' | 'la-primitiva' | 'el-gordo',
  selectedWindow: ApuestasWindow,
) {
  const [points, setPoints] = useState<ApuestasPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const params = new URLSearchParams();
      params.set('window', selectedWindow);
      const res = await fetch(`${API_URL}/api/${lottery}/apuestas?${params.toString()}`);
      const data: ApiResponse = await res.json();
      if (!res.ok) {
        setError((data as any).detail ?? res.statusText);
        setPoints([]);
        return;
      }
      setPoints((data.points ?? []).map((p) => ({
        ...p,
        apuestas: p.apuestas ?? null,
        premios: p.premios ?? null,
        premio_bote: p.premio_bote ?? null,
      })));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al cargar histórico de apuestas');
      setPoints([]);
    } finally {
      setLoading(false);
    }
  }, [lottery, selectedWindow]);

  useEffect(() => {
    void load();
  }, [load]);

  return { points, loading, error, reload: load };
}

