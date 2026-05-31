import { differenceFromRow, formatDifference } from '../utils/compareAnalysis';

type Props = {
  row: {
    difference_prev_special?: number | null;
    special_position?: number | null;
    prev_special_position?: number | null;
  };
};

/** Renders 1ª (top tier) position minus previous draw 1ª position. */
export function AnalysisDifferenceCell({ row }: Props) {
  const diff = differenceFromRow(row);
  if (diff == null) return <td>—</td>;
  const color =
    diff > 0 ? '#16a34a' : diff < 0 ? '#dc2626' : 'var(--color-text-muted)';
  return (
    <td style={{ fontWeight: 600, color }}>
      {formatDifference(diff)}
    </td>
  );
}
