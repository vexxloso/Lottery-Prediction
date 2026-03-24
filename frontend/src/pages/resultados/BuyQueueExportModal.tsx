import { Modal, Button, Typography, Space, InputNumber } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import { FileTextOutlined, FileOutlined, FilePdfOutlined } from '@ant-design/icons';

import { openPrintTabForLater } from './buyQueueExport';

const { Text, Paragraph } = Typography;

type Props = {
  open: boolean;
  onCancel: () => void;
  lotteryTitle: string;
  disabled: boolean;
  queueTicketCounts: number[];
  onExportCsv: (selection: { queueCount: number; requestedTickets: number; selectedTickets: number }) => void;
  onExportTxt: (selection: { queueCount: number; requestedTickets: number; selectedTickets: number }) => void;
  /**
   * `printTab` is opened synchronously on click (before any await) so the browser allows it.
   * Pass it to `openModernPrintView(..., printTab)` after saving.
   */
  onExportPdf: (printTab: Window | null, selection: { queueCount: number; requestedTickets: number; selectedTickets: number }) => void | Promise<void>;
};

export function BuyQueueExportModal({
  open,
  onCancel,
  lotteryTitle,
  disabled,
  queueTicketCounts,
  onExportCsv,
  onExportTxt,
  onExportPdf,
}: Props) {
  const [requestedTickets, setRequestedTickets] = useState(1);
  const totalTickets = useMemo(
    () => queueTicketCounts.reduce((sum, n) => sum + (Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0), 0),
    [queueTicketCounts],
  );
  useEffect(() => {
    if (!open) return;
    setRequestedTickets(Math.max(1, totalTickets));
  }, [open, totalTickets]);
  const selection = useMemo(() => {
    const need = Math.max(1, Number.isFinite(requestedTickets) ? Math.floor(requestedTickets) : 1);
    let selectedTickets = 0;
    let queueCount = 0;
    for (const n of queueTicketCounts) {
      const count = Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
      queueCount += 1;
      selectedTickets += count;
      if (selectedTickets >= need) break;
    }
    return { queueCount, requestedTickets: need, selectedTickets };
  }, [requestedTickets, queueTicketCounts]);

  return (
    <Modal
      title={`Exportar cola — ${lotteryTitle}`}
      open={open}
      onCancel={onCancel}
      footer={null}
      centered
      destroyOnClose
      width={440}
    >
      <Paragraph style={{ marginBottom: 10 }}>
        Boletos a exportar/imprimir:
      </Paragraph>
      <Space size="small" style={{ marginBottom: 10 }}>
        <InputNumber
          min={1}
          max={Math.max(1, totalTickets)}
          value={requestedTickets}
          onChange={(v) => setRequestedTickets(v ?? 1)}
          addonBefore="Cantidad"
          disabled={disabled}
        />
      </Space>
      <Paragraph type="secondary" style={{ marginBottom: 16 }}>
        Se seleccionarán automáticamente <Text strong>{selection.queueCount}</Text> cola(s), con{' '}
        <Text strong>{selection.selectedTickets}</Text> boleto(s) en total.
      </Paragraph>
      <Paragraph type="secondary" style={{ marginBottom: 20 }}>
        Elige el formato. <Text strong>PDF / imprimir</Text> guarda antes los boletos de la cola en{' '}
        <Text strong>Boletos guardados</Text>, abre la vista para imprimir y puedes usar «Guardar como PDF».
      </Paragraph>
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        <Button
          block
          size="large"
          icon={<FileTextOutlined />}
          disabled={disabled}
          onClick={() => {
            onExportCsv(selection);
            // Closing the modal in the same tick can interrupt the download (focus / unmount).
            window.setTimeout(() => onCancel(), 150);
          }}
        >
          Descargar CSV
        </Button>
        <Button
          block
          size="large"
          icon={<FileOutlined />}
          disabled={disabled}
          onClick={() => {
            onExportTxt(selection);
            window.setTimeout(() => onCancel(), 150);
          }}
        >
          Descargar TXT
        </Button>
        <Button
          block
          size="large"
          type="primary"
          icon={<FilePdfOutlined />}
          disabled={disabled}
          onClick={() => {
            const printTab = openPrintTabForLater();
            void (async () => {
              try {
                await Promise.resolve(onExportPdf(printTab, selection));
              } finally {
                window.setTimeout(() => onCancel(), 150);
              }
            })();
          }}
        >
          PDF / imprimir
        </Button>
      </Space>
      <Paragraph type="secondary" style={{ marginTop: 20, marginBottom: 0, fontSize: 12 }}>
        Números con dos cifras y barras (p. ej. <code>01/15/22/48/01</code>). CSV: separador <code>;</code>. TXT: columnas
        separadas por tabulador.
      </Paragraph>
    </Modal>
  );
}
