import { useEffect, useState, useCallback } from 'react';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';
const PAGE_SIZE = 20;

export interface EuromillonesFeatureRow {
  id_sorteo: string;
  pre_id_sorteo?: string | null;
  fecha_sorteo?: string;
  dia_semana?: string;
  main_number?: number[];
  star_number?: number[];
  main_dx?: number[];
  star_dx?: number[];
  frequency?: Array<number | null>;
  gap?: Array<number | null>;
  presence_mask?: number[];
}

interface ApiResponse {
  features: EuromillonesFeatureRow[];
  total: number;
}

export function useEuromillonesFeatures() {
  const [rows, setRows] = useState<EuromillonesFeatureRow[]>([]);
  const [total, setTotal] = useState(0);
  const [skip, setSkip] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const fetchRows = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const params = new URLSearchParams();
      params.set('limit', String(PAGE_SIZE));
      params.set('skip', String(skip));
      const res = await fetch(`${API_URL}/api/euromillones/feature-model?${params.toString()}`);
      const data: ApiResponse = await res.json();
      if (!res.ok) {
        setError((data as any).detail ?? res.statusText);
        setRows([]);
        setTotal(0);
        return;
      }
      setRows(data.features ?? []);
      setTotal(data.total ?? 0);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Error al cargar datos de Euromillones');
      setRows([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }, [skip]);

  useEffect(() => {
    fetchRows();
  }, [fetchRows]);

  const totalPages = Math.ceil(total / PAGE_SIZE) || 1;
  const currentPage = Math.floor(skip / PAGE_SIZE) + 1;

  const nextPage = useCallback(() => {
    setSkip((s) => s + PAGE_SIZE);
  }, []);

  const prevPage = useCallback(() => {
    setSkip((s) => Math.max(0, s - PAGE_SIZE));
  }, []);

  const reload = useCallback(() => {
    setSkip(0);
  }, []);

  return {
    rows,
    total,
    loading,
    error,
    currentPage,
    totalPages,
    nextPage,
    prevPage,
    reload,
  };
}

