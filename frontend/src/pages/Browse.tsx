// Browse — filter + sort + paginate the precomputed per-window establishment
// bundle entirely client-side. Filter inputs are mirrored into the URL
// query string so links stay shareable.
import { useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import type { EstablishmentRow, EstablishmentsData } from '../types';
import { useData, dataUrl } from '../lib/data';
import { useWindow } from '../lib/window';
import { Empty, ErrorMsg, Loading } from '../components/Loading';
import { fmtMoney, fmtNumber, fmtScore } from '../lib/format';

const PER_PAGE = 50;

type SortCol =
  | 'name'
  | 'city'
  | 'zip'
  | 'score'
  | 'inspections'
  | 'revenue'
  | 'match';

const SORT_ACCESSOR: Record<SortCol, (r: EstablishmentRow) => string | number | null> = {
  name: (r) => r.canonical_name,
  city: (r) => r.city,
  zip: (r) => r.zip,
  score: (r) => (r.avg_score == null ? null : Number(r.avg_score)),
  inspections: (r) => r.inspection_count,
  revenue: (r) => r.avg_monthly_receipts,
  match: (r) => r.match_score,
};

const matchLabel = (m: string) =>
  ({
    fuzzy_zip_block: 'Fuzzy',
    mb_only: 'MB only',
    inspection_only: 'Inspection only',
  } as Record<string, string>)[m] || m;

export default function Browse() {
  const [window] = useWindow();
  const state = useData<EstablishmentsData>(dataUrl('establishments', window));
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();

  const q = params.get('q') || '';
  const zip = params.get('zip') || '';
  const match = params.get('match') || '';
  const minScore = params.get('min_score') || '';
  const maxScore = params.get('max_score') || '';
  const sort = (params.get('sort') as SortCol | null) || 'name';
  const dir = (params.get('dir') as 'asc' | 'desc' | null) || 'asc';
  const page = Math.max(1, parseInt(params.get('page') || '1', 10));

  const update = (patch: Record<string, string | null>, resetPage = false) => {
    const next = new URLSearchParams(params);
    for (const [k, v] of Object.entries(patch)) {
      if (v == null || v === '') next.delete(k);
      else next.set(k, v);
    }
    if (resetPage) next.delete('page');
    setParams(next, { replace: true });
  };

  const allRows: EstablishmentRow[] = state.status === 'ready' ? state.data.rows : [];

  const filtered = useMemo(() => {
    const needle = q.toUpperCase();
    const minS = minScore ? Number(minScore) : null;
    const maxS = maxScore ? Number(maxScore) : null;
    return allRows.filter((r) => {
      if (needle && !(r.canonical_name || '').toUpperCase().includes(needle))
        return false;
      if (zip && r.zip !== zip) return false;
      if (match && r.match_method !== match) return false;
      const sc = r.avg_score == null ? null : Number(r.avg_score);
      if (minS != null && (sc == null || sc < minS)) return false;
      if (maxS != null && (sc == null || sc > maxS)) return false;
      return true;
    });
  }, [allRows, q, zip, match, minScore, maxScore]);

  const sorted = useMemo(() => {
    const acc = SORT_ACCESSOR[sort];
    const asc = dir !== 'desc';
    return [...filtered].sort((a, b) => {
      const av = acc(a);
      const bv = acc(b);
      const aNull = av == null || av === '';
      const bNull = bv == null || bv === '';
      if (aNull && bNull) return a.id - b.id;
      if (aNull) return 1;
      if (bNull) return -1;
      const an = typeof av === 'number' ? av : Number(av);
      const bn = typeof bv === 'number' ? bv : Number(bv);
      const numeric = !Number.isNaN(an) && !Number.isNaN(bn);
      if (numeric) return asc ? an - bn : bn - an;
      const c = String(av).localeCompare(String(bv));
      return asc ? c : -c;
    });
  }, [filtered, sort, dir]);

  const total = sorted.length;
  const start = (page - 1) * PER_PAGE;
  const pageRows = sorted.slice(start, start + PER_PAGE);

  const sortClass = (col: SortCol) =>
    `sortable${col === sort ? ` ${dir}` : ''}`;
  const setSort = (col: SortCol) => {
    const nextDir = sort === col && dir === 'asc' ? 'desc' : 'asc';
    update({ sort: col, dir: nextDir });
  };

  if (state.status === 'loading') return <Loading />;
  if (state.status === 'error') return <ErrorMsg error={state.error} />;

  return (
    <>
      <h1>Browse establishments</h1>
      <div className="controls-row" style={{ flexWrap: 'wrap', gap: '0.5rem' }}>
        <input
          id="fName"
          placeholder="Name contains…"
          value={q}
          onChange={(e) => update({ q: e.target.value.trim() }, true)}
        />
        <input
          id="fZip"
          placeholder="ZIP"
          value={zip}
          onChange={(e) => update({ zip: e.target.value.trim() }, true)}
          style={{ width: 100 }}
        />
        <select
          id="fMatch"
          value={match}
          onChange={(e) => update({ match: e.target.value }, true)}
        >
          <option value="">Any match method</option>
          <option value="fuzzy_zip_block">Fuzzy</option>
          <option value="mb_only">MB only</option>
          <option value="inspection_only">Inspection only</option>
        </select>
        <input
          id="fMinScore"
          placeholder="Min score"
          value={minScore}
          onChange={(e) => update({ min_score: e.target.value.trim() }, true)}
          style={{ width: 100 }}
        />
        <input
          id="fMaxScore"
          placeholder="Max score"
          value={maxScore}
          onChange={(e) => update({ max_score: e.target.value.trim() }, true)}
          style={{ width: 100 }}
        />
        <button
          id="fReset"
          onClick={() => setParams(new URLSearchParams(), { replace: true })}
        >
          Reset
        </button>
        <span id="resultCount" className="muted">
          {total.toLocaleString()} match{total === 1 ? '' : 'es'}
        </span>
      </div>

      {!total ? (
        <Empty label="No establishments match those filters." />
      ) : (
        <table id="estTable" className="hover-table">
          <thead>
            <tr>
              <th className={sortClass('name')} onClick={() => setSort('name')}>Name</th>
              <th className={sortClass('city')} onClick={() => setSort('city')}>City</th>
              <th className={sortClass('zip')} onClick={() => setSort('zip')}>ZIP</th>
              <th>Address</th>
              <th className={`num ${sortClass('score')}`} onClick={() => setSort('score')}>Avg score</th>
              <th className={`num ${sortClass('inspections')}`} onClick={() => setSort('inspections')}>Inspections</th>
              <th className={`num ${sortClass('revenue')}`} onClick={() => setSort('revenue')}>Avg $/mo</th>
              <th className={sortClass('match')} onClick={() => setSort('match')}>Match</th>
            </tr>
          </thead>
          <tbody>
            {pageRows.map((r) => (
              <tr key={r.id} onClick={() => navigate(`/establishment/${r.id}`)}>
                <td>{r.canonical_name}</td>
                <td>{r.city}</td>
                <td>{r.zip || ''}</td>
                <td className="muted">{r.canonical_address || ''}</td>
                <td className="num">{fmtScore(r.avg_score)}</td>
                <td className="num">{fmtNumber(r.inspection_count)}</td>
                <td className="num">{fmtMoney(r.avg_monthly_receipts)}</td>
                <td className={`match-${r.match_method}`}>
                  {matchLabel(r.match_method)} {r.match_score ? `· ${r.match_score}` : ''}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="controls-row" style={{ marginTop: '0.75rem' }}>
        <button
          id="pgPrev"
          disabled={page <= 1}
          onClick={() => update({ page: String(page - 1) })}
        >
          ← Prev
        </button>
        <span id="pgInfo" className="muted">
          {total
            ? `${(start + 1).toLocaleString()}–${Math.min(
                start + PER_PAGE,
                total
              ).toLocaleString()} of ${total.toLocaleString()}`
            : ''}
        </span>
        <button
          id="pgNext"
          disabled={start + PER_PAGE >= total}
          onClick={() => update({ page: String(page + 1) })}
        >
          Next →
        </button>
      </div>
    </>
  );
}
