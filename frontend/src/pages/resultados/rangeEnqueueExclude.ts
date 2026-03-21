/**
 * 1-based full-wheel line positions already used (cesta, cola, guardados).
 * Sent as exclude_positions when enqueueing by range so those lines are not queued again.
 */

function addPosition(s: Set<number>, t: { position?: number } | null | undefined) {
  if (t == null) return;
  const p = t.position;
  if (typeof p === 'number' && Number.isFinite(p) && p >= 1) {
    s.add(Math.floor(p));
  }
}

export function collectWheelPositionsFromBettingState(
  buyQueue: { tickets?: { position?: number }[] }[],
  bucket: { position?: number }[],
  realPool: { position?: number }[],
): number[] {
  const s = new Set<number>();
  for (const q of buyQueue) {
    const ts = q?.tickets;
    if (!Array.isArray(ts)) continue;
    for (const t of ts) addPosition(s, t);
  }
  for (const t of bucket) addPosition(s, t);
  for (const t of realPool) addPosition(s, t);
  return [...s].sort((a, b) => a - b);
}

/** Subset of positions that fall inside [start, end] (inclusive) for the API payload. */
export function wheelPositionsInRange(positions: number[], start: number, end: number): number[] {
  if (end < start) return [];
  return positions.filter((p) => p >= start && p <= end);
}

export function countWheelPositionsInRange(positions: number[], start: number, end: number): number {
  if (end < start) return 0;
  return positions.reduce((n, p) => n + (p >= start && p <= end ? 1 : 0), 0);
}
