import { differenceFromRow, formatPosition } from '../utils/compareAnalysis';

type Props = {
  row: {
    difference_prev_special?: number | null;
    special_position?: number | null;
    prev_special_position?: number | null;
  };
};

/** Renders current Special minus previous draw Special. */
export function AnalysisDifferenceCell({ row }: Props) {
  const diff = differenceFromRow(row);
  if (diff == null) return <td>—</td>;
  return (
    <td style={{ fontWeight: 600, color: '#7c3aed' }}>
      {formatPosition(diff)}
    </td>
  );
}
