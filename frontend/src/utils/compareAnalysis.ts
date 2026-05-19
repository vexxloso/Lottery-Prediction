/** Client metric: gap between top prize rank and 1st secondary tier rank. */

export function formatPosition(value: number | null | undefined): string {
  if (value == null || value <= 0) return '—';
  return Number(value).toLocaleString();
}

export function computeDifference(
  special: number | null | undefined,
  first: number | null | undefined,
): number | null {
  if (special == null || first == null || special <= 0 || first <= 0) return null;
  return special - first;
}

export function differenceFromRow(row: {
  difference_special_1st?: number | null;
  special_position?: number | null;
  pos_1th?: number | null;
}): number | null {
  if (row.difference_special_1st != null && row.difference_special_1st > 0) {
    return row.difference_special_1st;
  }
  return computeDifference(row.special_position, row.pos_1th);
}
