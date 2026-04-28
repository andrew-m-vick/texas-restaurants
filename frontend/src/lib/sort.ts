// Tiny helper for table sorting. Keeps every page's "click header to
// sort" logic to a couple of useState hooks plus a memoized derivation.
import { useMemo, useState } from 'react';

export type SortDir = 'asc' | 'desc';

export interface SortState<K extends string> {
  col: K;
  dir: SortDir;
}

export function useSortable<T, K extends string>(
  rows: T[],
  initial: SortState<K>,
  accessor: (row: T, col: K) => string | number | null | undefined
): {
  sorted: T[];
  sort: SortState<K>;
  toggle: (col: K) => void;
} {
  const [sort, setSort] = useState<SortState<K>>(initial);

  const sorted = useMemo(() => {
    const { col, dir } = sort;
    const asc = dir === 'asc';
    return [...rows].sort((a, b) => {
      const av = accessor(a, col);
      const bv = accessor(b, col);
      const aNull = av == null || av === '';
      const bNull = bv == null || bv === '';
      if (aNull && bNull) return 0;
      if (aNull) return 1; // nulls always last
      if (bNull) return -1;
      const an = typeof av === 'number' ? av : Number(av);
      const bn = typeof bv === 'number' ? bv : Number(bv);
      const numeric = !Number.isNaN(an) && !Number.isNaN(bn);
      if (numeric) return asc ? an - bn : bn - an;
      const c = String(av).localeCompare(String(bv));
      return asc ? c : -c;
    });
  }, [rows, sort, accessor]);

  const toggle = (col: K) =>
    setSort((cur) => ({
      col,
      dir: cur.col === col && cur.dir === 'asc' ? 'desc' : 'asc',
    }));

  return { sorted, sort, toggle };
}
