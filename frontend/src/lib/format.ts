// Small formatters reused across views. Mirror the helpers in the
// legacy vanilla JS (charts.js) so the visual output stays consistent.

export const fmtMoney = (n: number | null | undefined): string =>
  n == null
    ? '—'
    : '$' + Math.round(Number(n)).toLocaleString();

export const fmtNumber = (n: number | null | undefined): string =>
  n == null ? '—' : Number(n).toLocaleString();

export const fmtScore = (n: number | null | undefined): string =>
  n == null ? '—' : Number(n).toFixed(1);
