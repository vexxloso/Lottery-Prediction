import { useEffect, useState, useCallback } from 'react';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';
const PAGE_SIZE = 20;

export interface LaPrimitivaFeatureRow {
  draw_id: string;
  draw_date: string;
  weekday?: string;
  main_numbers: number[];
  complementario?: number | null;
  reintegro?: number | null;
  hot_main_numbers?: number[];
  cold_main_numbers?: number[];
  hot_complementario?: number[];
  cold_complementario?: number[];
  hot_reintegro?: number[];
  cold_reintegro?: number[];
  prev_draw_id?: string | null;
  prev_draw_date?: string | null;
  prev_weekday?: string | null;
  prev_main_numbers?: number[];
  prev_complementario?: number | null;
  prev_reintegro?: number | null;
  main_frequency_counts?: number[];
  complementario_frequency_counts?: number[];
  reintegro_frequency_counts?: number[];
}

interface ApiResponse {
  features: LaPrimitivaFeatureRow[];
  total: number;
}

export function useLaPrimitivaFeatures() {
  const [rows, setRows] = useState<LaPrimitivaFeatureRow[]>([]);
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
      const res = await fetch(`${API_URL}/api/la-primitiva/features?${params.toString()}`);
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
      setError(e instanceof Error ? e.message : 'Error al cargar datos de La Primitiva');
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

  const setPage = useCallback((page: number) => {
    if (!Number.isFinite(page) || page < 1) {
      setSkip(0);
      return;
    }
    setSkip((page - 1) * PAGE_SIZE);
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
    pageSize: PAGE_SIZE,
    nextPage,
    prevPage,
    setPage,
    reload,
  };
}

