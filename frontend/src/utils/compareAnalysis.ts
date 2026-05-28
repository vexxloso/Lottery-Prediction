/** Client metric: gap between Special position and previous draw's Special position. */

export function formatPosition(value: number | null | undefined): string {
  if (value == null || value <= 0) return '—';
  return Number(value).toLocaleString();
}

export function computeDifference(
  special: number | null | undefined,
  prevSpecial: number | null | undefined,
): number | null {
  if (special == null || prevSpecial == null || special <= 0 || prevSpecial <= 0) return null;
  return special - prevSpecial;
}

export function differenceFromRow(row: {
  difference_prev_special?: number | null;
  special_position?: number | null;
  prev_special_position?: number | null;
}): number | null {
  if (row.difference_prev_special != null) {
    return row.difference_prev_special;
  }
  return computeDifference(row.special_position, row.prev_special_position);
}
