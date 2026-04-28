// Header search with debounced typeahead. Uses the same client-side
// filter pattern as the vanilla version but lives in component state.
import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { SearchData, SearchResult } from '../types';
import { fetchData } from '../lib/data';

export default function SearchBox() {
  const [q, setQ] = useState('');
  const [open, setOpen] = useState(false);
  const [results, setResults] = useState<SearchResult[]>([]);
  const indexRef = useRef<SearchResult[] | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const navigate = useNavigate();

  // Lazy-load the search index on first focus.
  const ensureIndex = async () => {
    if (indexRef.current) return indexRef.current;
    const data = await fetchData<SearchData>('/static/data/search.json');
    indexRef.current = data.results;
    return indexRef.current;
  };

  useEffect(() => {
    if (q.length < 2) {
      setOpen(false);
      setResults([]);
      return;
    }
    const id = setTimeout(async () => {
      const index = await ensureIndex();
      const needle = q.toUpperCase();
      const found: SearchResult[] = [];
      for (const r of index) {
        if (
          (r.canonical_name || '').toUpperCase().includes(needle) ||
          (r.canonical_address || '').toUpperCase().includes(needle)
        ) {
          found.push(r);
          if (found.length >= 15) break;
        }
      }
      setResults(found);
      setOpen(true);
    }, 200);
    return () => clearTimeout(id);
  }, [q]);

  // Close on outside click.
  useEffect(() => {
    const onDocClick = (e: MouseEvent) => {
      if (!containerRef.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('click', onDocClick);
    return () => document.removeEventListener('click', onDocClick);
  }, []);

  return (
    <div className="search-box" ref={containerRef}>
      <input
        type="text"
        placeholder="Search restaurants..."
        autoComplete="off"
        value={q}
        onChange={(e) => setQ(e.target.value)}
        onFocus={() => q.length >= 2 && setOpen(true)}
      />
      <div className={`search-results${open ? ' open' : ''}`}>
        {results.length === 0 && q.length >= 2 ? (
          <div className="empty">No matches.</div>
        ) : (
          results.map((r) => (
            <a
              key={r.id}
              href={`/app/establishment/${r.id}`}
              onClick={(e) => {
                e.preventDefault();
                navigate(`/establishment/${r.id}`);
                setOpen(false);
                setQ('');
              }}
            >
              <div className="name">{r.canonical_name}</div>
              <div className="meta">
                {r.city} · {r.zip || '—'} · {r.canonical_address || ''} · {r.match_method}
              </div>
            </a>
          ))
        )}
      </div>
    </div>
  );
}
