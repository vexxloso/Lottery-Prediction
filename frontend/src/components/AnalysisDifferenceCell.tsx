import { differenceFromRow, formatPosition } from '../utils/compareAnalysis';

type Props = {
  row: {
    difference_special_1st?: number | null;
    special_position?: number | null;
    pos_1th?: number | null;
  };
};

/** Renders Special − 1st difference (client spreadsheet column). */
export function AnalysisDifferenceCell({ row }: Props) {
  const diff = differenceFromRow(row);
  if (diff == null) return <td>—</td>;
  return (
    <td style={{ fontWeight: 600, color: '#7c3aed' }}>
      {formatPosition(diff)}
    </td>
  );
}
