/** Flatten buy-queue API items into table rows for CSV / print (one row per ticket). */

export type EuromillonesQueueItem = {
  tickets?: { mains?: number[]; stars?: number[]; position?: number }[];
};

export type ElGordoQueueItem = {
  tickets?: { mains?: number[]; clave?: number; position?: number }[];
};

export type LaPrimitivaQueueItem = {
  tickets?: { mains?: number[]; reintegro?: number; position?: number }[];
};

export type ExportTable = {
  headers: string[];
  rows: string[][];
};

/** Lottery-style: 1 → 01 (two digits). */
export function pad2(n: number): string {
  if (!Number.isFinite(n)) return '00';
  return String(Math.max(0, Math.floor(n))).padStart(2, '0');
}

/** Sorted numbers as 01,15,22,48,01 */
export function formatNumsSlash(nums: number[]): string {
  const sorted = [...nums].map(Number).filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
  return sorted.map((x) => pad2(x)).join(',');
}

function posCell(t: { position?: number }): string {
  if (typeof t.position === 'number' && Number.isFinite(t.position) && t.position >= 1) {
    return String(Math.floor(t.position));
  }
  return '';
}

export function flattenEuromillonesQueue(queue: EuromillonesQueueItem[]): ExportTable {
  const rows: string[][] = [];
  let idx = 0;
  for (const q of queue) {
    const ts = q.tickets;
    if (!Array.isArray(ts)) continue;
    for (const t of ts) {
      idx += 1;
      const mains = [...(t.mains ?? [])].map(Number);
      const stars = [...(t.stars ?? [])].map(Number);
      const m = formatNumsSlash(mains);
      const s = formatNumsSlash(stars);
      rows.push([String(idx), posCell(t), m, s]);
    }
  }
  return {
    headers: ['#', 'Posición', 'Principales', 'Estrellas'],
    rows,
  };
}

export function flattenElGordoQueue(queue: ElGordoQueueItem[]): ExportTable {
  const rows: string[][] = [];
  let idx = 0;
  for (const q of queue) {
    const ts = q.tickets;
    if (!Array.isArray(ts)) continue;
    for (const t of ts) {
      idx += 1;
      const mains = [...(t.mains ?? [])].map(Number);
      const m = formatNumsSlash(mains);
      const c = typeof t.clave === 'number' ? t.clave : Number(t.clave) || 0;
      rows.push([String(idx), posCell(t), m, pad2(Number(c))]);
    }
  }
  return {
    headers: ['#', 'Posición', 'Números', 'Clave'],
    rows,
  };
}

export function flattenLaPrimitivaQueue(queue: LaPrimitivaQueueItem[]): ExportTable {
  const rows: string[][] = [];
  let idx = 0;
  for (const q of queue) {
    const ts = q.tickets;
    if (!Array.isArray(ts)) continue;
    for (const t of ts) {
      idx += 1;
      const mains = [...(t.mains ?? [])].map(Number);
      const m = formatNumsSlash(mains);
      const r = typeof t.reintegro === 'number' ? t.reintegro : Number(t.reintegro) || 0;
      rows.push([String(idx), posCell(t), m, pad2(Number(r))]);
    }
  }
  return {
    headers: ['#', 'Posición', 'Números', 'Reintegro'],
    rows,
  };
}

/** Build .txt body: title lines + tab-separated table (UTF-8 BOM). */
export function buildExportTxtLines(title: string, headers: string[], rows: string[][]): string[] {
  const gen = `Generado: ${new Date().toLocaleString('es-ES')}`;
  const headRow = headers.join('\t');
  const dataRows = rows.map((r) => r.join('\t'));
  return [title, gen, '', headRow, ...dataRows];
}

/** Windows / browser-safe download name (invalid chars → _). */
function sanitizeDownloadFilename(name: string, ext: 'csv' | 'txt'): string {
  const trimmed = name.trim().replace(/[<>:"/\\|?*\u0000-\u001f]/g, '_').replace(/\.+$/, '');
  const e = ext.toLowerCase();
  const lower = trimmed.toLowerCase();
  const withExt = lower.endsWith(`.${e}`) ? trimmed : `${trimmed || 'cola'}.${e}`;
  return withExt || `cola-compra.${e}`;
}

/** European-style CSV (;) with BOM for Excel. */
export function downloadCsv(filename: string, headers: string[], rows: string[][]): void {
  const sep = ';';
  const esc = (c: string) => {
    const needs = c.includes(sep) || c.includes('"') || c.includes('\n') || c.includes('\r');
    const e = c.replace(/"/g, '""');
    return needs ? `"${e}"` : e;
  };
  const lines = [headers.map(esc).join(sep), ...rows.map((r) => r.map(esc).join(sep))];
  const blob = new Blob(['\ufeff' + lines.join('\r\n')], { type: 'text/csv;charset=utf-8' });
  const downloadName = sanitizeDownloadFilename(
    filename.toLowerCase().endsWith('.csv') ? filename : `${filename}.csv`,
    'csv',
  );
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = downloadName;
  // display:none often prevents the download from starting (Chrome, Edge). Keep link in layout, off-screen.
  Object.assign(a.style, { position: 'fixed', left: '-9999px', top: '0' });
  document.body.appendChild(a);
  // Must stay synchronous with the user click; deferring (rAF) can lose the gesture on strict browsers.
  a.click();
  window.setTimeout(() => {
    a.remove();
    URL.revokeObjectURL(url);
  }, 4000);
}

/** Plain UTF-8 text with BOM (Excel / Notepad friendly). */
export function downloadTxt(filename: string, lines: string[]): void {
  const blob = new Blob(['\ufeff' + lines.join('\r\n')], { type: 'text/plain;charset=utf-8' });
  const downloadName = sanitizeDownloadFilename(
    filename.toLowerCase().endsWith('.txt') ? filename : `${filename}.txt`,
    'txt',
  );
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = downloadName;
  Object.assign(a.style, { position: 'fixed', left: '-9999px', top: '0' });
  document.body.appendChild(a);
  a.click();
  window.setTimeout(() => {
    a.remove();
    URL.revokeObjectURL(url);
  }, 4000);
}

function escHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/**
 * Open a blank tab synchronously from a click handler (before any `await`).
 * After async work (e.g. save to server), pass this window to `openModernPrintView` so the
 * print view still opens — `window.open` after `await` is usually blocked.
 */
export function openPrintTabForLater(): Window | null {
  return window.open('about:blank', '_blank');
}

/**
 * Opens a modern printable HTML page (use browser «Guardar como PDF» in the print dialog).
 * If `targetWin` was returned by `openPrintTabForLater()` from the same click, navigation
 * happens there. Returns false if no window could be used.
 */
export function openModernPrintView(
  opts: {
    title: string;
    subtitle?: string;
    columns: string[];
    rows: string[][];
  },
  targetWin?: Window | null,
): boolean {
  const thead = `<tr>${opts.columns.map((c) => `<th>${escHtml(c)}</th>`).join('')}</tr>`;
  const tbody = opts.rows
    .map((r) => `<tr>${r.map((c) => `<td>${escHtml(c)}</td>`).join('')}</tr>`)
    .join('');
  const sub = opts.subtitle ? `<p class="sub">${escHtml(opts.subtitle)}</p>` : '';
  const html = `<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>${escHtml(opts.title)}</title>
<style>
  *{box-sizing:border-box}
  body{margin:0;padding:0;background:#e8ecf1;color:#0f172a;font-family:ui-sans-serif,system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;-webkit-print-color-adjust:exact;print-color-adjust:exact}
  .wrap{min-height:100vh;padding:32px 20px}
  .card{max-width:920px;margin:0 auto;background:#fff;border-radius:20px;box-shadow:0 25px 50px -12px rgba(15,23,42,.18);overflow:hidden;border:1px solid rgba(15,23,42,.06)}
  .hero{background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 45%,#1d4ed8 100%);color:#fff;padding:32px 36px 28px}
  .hero h1{margin:0;font-size:1.5rem;font-weight:700;letter-spacing:-.03em;line-height:1.2}
  .hero .sub{margin:10px 0 0;font-size:.875rem;opacity:.88;font-weight:400}
  .badge{display:inline-block;margin-top:14px;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,.15);font-size:.7rem;font-weight:600;letter-spacing:.04em;text-transform:uppercase}
  .table-wrap{padding:8px 0 28px}
  table{width:100%;border-collapse:collapse;font-size:.8125rem}
  thead th{text-align:left;padding:14px 20px;background:#f1f5f9;color:#334155;font-weight:600;border-bottom:2px solid #e2e8f0;white-space:nowrap}
  tbody td{padding:13px 20px;border-bottom:1px solid #f1f5f9;vertical-align:middle}
  tbody tr:nth-child(even) td{background:#fafbfc}
  tbody tr:hover td{background:#f0f9ff}
  .foot{padding:0 36px 28px;font-size:.7rem;color:#64748b;line-height:1.5}
  @media print{
    body{background:#fff}
    .wrap{padding:0}
    .card{box-shadow:none;border-radius:0;border:none}
    tbody tr:hover td{background:transparent}
    .no-print{display:none!important}
  }
</style></head><body>
<div class="wrap">
  <div class="card">
    <div class="hero">
      <h1>${escHtml(opts.title)}</h1>
      ${sub}
      <span class="badge">Cola de compra</span>
    </div>
    <div class="table-wrap">
      <table><thead>${thead}</thead><tbody>${tbody}</tbody></table>
    </div>
    <p class="foot no-print">Vista lista para imprimir. En el diálogo de impresión elija su impresora o <strong>Guardar como PDF</strong> para obtener un archivo PDF.</p>
  </div>
</div>
<script>document.addEventListener('DOMContentLoaded',function(){setTimeout(function(){window.print()},200)});</script>
</body></html>`;

  const blob = new Blob([html], { type: 'text/html;charset=utf-8' });
  const blobUrl = URL.createObjectURL(blob);
  let w: Window | null = null;
  if (targetWin != null && !targetWin.closed) {
    try {
      targetWin.location.replace(blobUrl);
      w = targetWin;
    } catch {
      w = window.open(blobUrl, '_blank');
    }
  } else {
    w = window.open(blobUrl, '_blank');
  }
  if (!w) {
    URL.revokeObjectURL(blobUrl);
    return false;
  }
  w.focus();
  window.setTimeout(() => URL.revokeObjectURL(blobUrl), 120_000);
  return true;
}

/** Human-readable lottery segment for filenames (no spaces). */
const LOTTERY_EXPORT_LABEL: Record<string, string> = {
  euromillones: 'Euromillones',
  'el-gordo': 'ElGordo',
  'la-primitiva': 'LaPrimitiva',
};

/**
 * Base filename without extension, e.g. Cola-Euromillones-2026-03-21-15-30-45
 * (date + time so exports in the same minute still differ).
 */
export function exportFilenameBase(lotterySlug: string): string {
  const label = LOTTERY_EXPORT_LABEL[lotterySlug] ?? lotterySlug.replace(/[^a-zA-Z0-9-]+/g, '-');
  const d = new Date();
  const ymd = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  const hms = `${String(d.getHours()).padStart(2, '0')}-${String(d.getMinutes()).padStart(2, '0')}-${String(d.getSeconds()).padStart(2, '0')}`;
  return `Cola-${label}-${ymd}-${hms}`;
}

/**
 * Filename format requested by users:
 *   Cola-<Type>-<last_draw_date>(<printed_timestamp>)
 *
 * Example:
 *   Cola-ElGordo-2026-03-08(2026-03-25-04-32-12)
 */
export function exportFilenameBaseWithDrawDate(
  lotterySlug: string,
  lastDrawDate: string | null | undefined,
): string {
  const label = LOTTERY_EXPORT_LABEL[lotterySlug] ?? lotterySlug.replace(/[^a-zA-Z0-9-]+/g, '-');
  const d = new Date();
  const printedYMD = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  const printedHMS = `${String(d.getHours()).padStart(2, '0')}-${String(d.getMinutes()).padStart(2, '0')}-${String(d.getSeconds()).padStart(2, '0')}`;
  const printed = `${printedYMD}-${printedHMS}`;

  const dd = (lastDrawDate ?? '').toString().trim();
  const drawDateOk = /^\d{4}-\d{2}-\d{2}$/.test(dd);
  const drawDate = drawDateOk ? dd : printedYMD; // fallback to printed date (always valid)

  return `Cola-${label}-${drawDate}(${printed})`;
}
