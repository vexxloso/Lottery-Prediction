import { Modal, Button, Typography, Space } from 'antd';
import { FileTextOutlined, FileOutlined, FilePdfOutlined } from '@ant-design/icons';

import { openPrintTabForLater } from './buyQueueExport';

const { Text, Paragraph } = Typography;

type Props = {
  open: boolean;
  onCancel: () => void;
  lotteryTitle: string;
  disabled: boolean;
  onExportCsv: () => void;
  onExportTxt: () => void;
  /**
   * `printTab` is opened synchronously on click (before any await) so the browser allows it.
   * Pass it to `openModernPrintView(..., printTab)` after saving.
   */
  onExportPdf: (printTab: Window | null) => void | Promise<void>;
};

export function BuyQueueExportModal({
  open,
  onCancel,
  lotteryTitle,
  disabled,
  onExportCsv,
  onExportTxt,
  onExportPdf,
}: Props) {
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
            onExportCsv();
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
            onExportTxt();
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
                await Promise.resolve(onExportPdf(printTab));
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
