/**
 * Official lottery play/buy URLs (Spain — Loterías y Apuestas del Estado).
 * Used to open the real platform when the user wants to buy their bucket tickets.
 * We cannot pre-fill or auto-submit: the official site does not support that.
 */

export const OFFICIAL_PLAY_URLS: Record<string, string> = {
  'euromillones': 'https://www.loteriasyapuestas.es/euromillones',
  'la-primitiva': 'https://www.loteriasyapuestas.es/la-primitiva',
  'el-gordo': 'https://www.loteriasyapuestas.es/el-gordo-de-la-primitiva',
};

export function getOfficialPlayUrl(lotterySlug: string): string {
  return OFFICIAL_PLAY_URLS[lotterySlug] ?? 'https://www.loteriasyapuestas.es';
}
