// The "time window" filter (12 months / 3 years / 5 years / all)
// lives in the URL query string so links stay shareable. Components
// read it via useWindow() and write it via setWindow().
import { useSearchParams } from 'react-router-dom';
import type { Window } from '../types';

export const WINDOWS: { key: Window; label: string }[] = [
  { key: '12m', label: 'Last 12 months' },
  { key: '3y', label: 'Last 3 years' },
  { key: '5y', label: 'Last 5 years' },
  { key: 'all', label: 'All time (2007–present)' },
];

const VALID = new Set(WINDOWS.map((w) => w.key));
export const DEFAULT_WINDOW: Window = '5y';

export function useWindow(): [Window, (next: Window) => void] {
  const [params, setParams] = useSearchParams();
  const raw = params.get('window');
  const current: Window = raw && VALID.has(raw as Window) ? (raw as Window) : DEFAULT_WINDOW;

  const set = (next: Window) => {
    const updated = new URLSearchParams(params);
    if (next === DEFAULT_WINDOW) updated.delete('window');
    else updated.set('window', next);
    setParams(updated, { replace: true });
  };
  return [current, set];
}
