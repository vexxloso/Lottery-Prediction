/** Parse jackpot rank from compare API payload (La Primitiva may use special_position). */
export function parseCompareJackpot(data: Record<string, unknown>): number | null {
  for (const key of ['jackpot_position', 'special_position'] as const) {
    const jp = data[key];
    if (typeof jp === 'number' && jp > 0) return jp;
    if (typeof jp === 'string' && jp.trim()) {
      const n = Number(jp);
      if (Number.isFinite(n) && n > 0) return n;
    }
  }
  return null;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Load full-wheel compare: GET cache → POST reorder → poll GET until jackpot is ready.
 * Handles 202 Accepted from reorder (background compare).
 */
export async function loadFullWheelCompare(opts: {
  getUrl: string;
  reorderUrl: string;
  isCancelled: () => boolean;
  pollAttempts?: number;
  pollIntervalMs?: number;
}): Promise<{ ok: true; data: Record<string, unknown> } | { ok: false; error: string }> {
  const pollAttempts = opts.pollAttempts ?? 45;
  const pollIntervalMs = opts.pollIntervalMs ?? 2000;

  const getOnce = async (): Promise<{ res: Response; data: Record<string, unknown> }> => {
    const res = await fetch(opts.getUrl, { method: 'GET', cache: 'no-store' });
    const data = (await res.json().catch(() => ({}))) as Record<string, unknown>;
    return { res, data };
  };

  const cached = await getOnce();
  if (!opts.isCancelled() && cached.res.ok && parseCompareJackpot(cached.data)) {
    return { ok: true, data: cached.data };
  }

  const reorderRes = await fetch(opts.reorderUrl, { method: 'POST', cache: 'no-store' });
  const reorderData = (await reorderRes.json().catch(() => ({}))) as Record<string, unknown>;

  if (!opts.isCancelled() && reorderRes.ok && parseCompareJackpot(reorderData)) {
    return { ok: true, data: reorderData };
  }

  if (!opts.isCancelled() && (reorderRes.status === 202 || reorderData.status === 'started')) {
    for (let attempt = 0; attempt < pollAttempts; attempt += 1) {
      await sleep(pollIntervalMs);
      if (opts.isCancelled()) {
        return { ok: false, error: 'Cancelled' };
      }
      const polled = await getOnce();
      if (polled.res.ok && parseCompareJackpot(polled.data)) {
        return { ok: true, data: polled.data };
      }
    }
    return {
      ok: false,
      error:
        'Comparación en segundo plano. Espera un momento y recarga, o revisa los logs del backend.',
    };
  }

  const detail =
    typeof reorderData.detail === 'string'
      ? reorderData.detail
      : typeof cached.data.detail === 'string'
        ? cached.data.detail
        : null;
  return {
    ok: false,
    error:
      detail ??
      'No se pudo calcular la comparación. Comprueba train progress y full wheel para el pre_id.',
  };
}
