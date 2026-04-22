// Establishments browse tab: filter + sort + paginate the precomputed
// per-window dataset client-side. State lives in the URL query string
// so pages stay shareable.

const ESTState = {
  read() {
    const p = new URLSearchParams(window.location.search);
    return {
      q: p.get('q') || '',
      zip: p.get('zip') || '',
      match: p.get('match') || '',
      min_score: p.get('min_score') || '',
      max_score: p.get('max_score') || '',
      sort: p.get('sort') || 'name',
      dir: p.get('dir') || 'asc',
      page: parseInt(p.get('page') || '1', 10),
    };
  },
  write(patch) {
    const url = new URL(window.location.href);
    for (const [k, v] of Object.entries(patch)) {
      if (v === '' || v == null) url.searchParams.delete(k);
      else url.searchParams.set(k, v);
    }
    if (!('page' in patch) && !('sort' in patch) && !('dir' in patch)) {
      url.searchParams.delete('page');
    }
    window.history.replaceState({}, '', url);
  },
};

const PER_PAGE = 50;
const SORT_COLUMNS = {
  name: 'canonical_name',
  city: 'city',
  zip: 'zip',
  score: 'avg_score',
  inspections: 'inspection_count',
  revenue: 'avg_monthly_receipts',
  match: 'match_score',
};

let ALL_ROWS = [];

async function renderEstablishments() {
  const s = ESTState.read();
  document.getElementById('fName').value = s.q;
  document.getElementById('fZip').value = s.zip;
  document.getElementById('fMatch').value = s.match;
  document.getElementById('fMinScore').value = s.min_score;
  document.getElementById('fMaxScore').value = s.max_score;

  let timer = null;
  const apply = (patch) => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      ESTState.write(patch);
      render();
    }, 150);
  };
  document.getElementById('fName').addEventListener('input', e => apply({ q: e.target.value.trim() }));
  document.getElementById('fZip').addEventListener('input', e => apply({ zip: e.target.value.trim() }));
  document.getElementById('fMatch').addEventListener('change', e => apply({ match: e.target.value }));
  document.getElementById('fMinScore').addEventListener('input', e => apply({ min_score: e.target.value }));
  document.getElementById('fMaxScore').addEventListener('input', e => apply({ max_score: e.target.value }));
  document.getElementById('fReset').addEventListener('click', () => {
    window.location.href = window.location.pathname;
  });

  document.querySelectorAll('#estTable thead th[data-sort]').forEach(th => {
    th.classList.add('sortable');
    th.addEventListener('click', () => {
      const cur = ESTState.read();
      const col = th.dataset.sort;
      const dir = (cur.sort === col && cur.dir === 'asc') ? 'desc' : 'asc';
      ESTState.write({ sort: col, dir });
      render();
    });
  });

  document.getElementById('pgPrev').addEventListener('click', () => {
    const cur = ESTState.read();
    if (cur.page > 1) { ESTState.write({ page: cur.page - 1 }); render(); }
  });
  document.getElementById('pgNext').addEventListener('click', () => {
    const cur = ESTState.read();
    ESTState.write({ page: cur.page + 1 });
    render();
  });

  const w = window.SELECTED_WINDOW || '5y';
  const tbody = document.querySelector('#estTable tbody');
  tbody.innerHTML = '<tr><td colspan="8" class="empty-msg"><span class="spinner"></span> Loading…</td></tr>';
  const bundle = await (await fetch(`/static/data/establishments-${w}.json`)).json();
  ALL_ROWS = bundle.rows || [];
  render();
}

function filterRows(rows, s) {
  const q = s.q ? s.q.toUpperCase() : '';
  const minS = s.min_score ? Number(s.min_score) : null;
  const maxS = s.max_score ? Number(s.max_score) : null;
  return rows.filter(r => {
    if (q && !((r.canonical_name || '').toUpperCase().includes(q))) return false;
    if (s.zip && r.zip !== s.zip) return false;
    if (s.match && r.match_method !== s.match) return false;
    const sc = r.avg_score == null ? null : Number(r.avg_score);
    if (minS != null && (sc == null || sc < minS)) return false;
    if (maxS != null && (sc == null || sc > maxS)) return false;
    return true;
  });
}

function sortRows(rows, s) {
  const col = SORT_COLUMNS[s.sort] || 'canonical_name';
  const asc = s.dir !== 'desc';
  const numeric = ['avg_score', 'inspection_count', 'avg_monthly_receipts', 'match_score'].includes(col);
  const sorted = rows.slice().sort((a, b) => {
    const av = a[col], bv = b[col];
    // Nulls always last
    const aNull = av == null || av === '';
    const bNull = bv == null || bv === '';
    if (aNull && bNull) return a.id - b.id;
    if (aNull) return 1;
    if (bNull) return -1;
    if (numeric) {
      const n = Number(av) - Number(bv);
      return asc ? n : -n;
    }
    const c = String(av).localeCompare(String(bv));
    return asc ? c : -c;
  });
  return sorted;
}

function render() {
  const s = ESTState.read();
  const filtered = filterRows(ALL_ROWS, s);
  const sorted = sortRows(filtered, s);
  const total = sorted.length;
  const page = Math.max(1, s.page);
  const start = (page - 1) * PER_PAGE;
  const pageRows = sorted.slice(start, start + PER_PAGE);

  const fmtN = n => n == null ? '—' : Number(n).toLocaleString();
  const fmt$ = n => n == null ? '—' : '$' + Math.round(Number(n)).toLocaleString();
  const fmtS = n => n == null ? '—' : Number(n).toFixed(1);

  const tbody = document.querySelector('#estTable tbody');
  if (!pageRows.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty-msg">No establishments match those filters.</td></tr>';
  } else {
    tbody.innerHTML = pageRows.map(r => `
      <tr onclick="window.location='/establishment/${r.id}'">
        <td>${r.canonical_name}</td>
        <td>${r.city}</td>
        <td>${r.zip || ''}</td>
        <td class="muted">${r.canonical_address || ''}</td>
        <td class="num">${fmtS(r.avg_score)}</td>
        <td class="num">${fmtN(r.inspection_count)}</td>
        <td class="num">${fmt$(r.avg_monthly_receipts)}</td>
        <td class="match-${r.match_method}">${matchLabel(r.match_method)} ${r.match_score ? `· ${r.match_score}` : ''}</td>
      </tr>
    `).join('');
  }

  document.querySelectorAll('#estTable thead th[data-sort]').forEach(th => {
    th.classList.remove('asc', 'desc');
    if (th.dataset.sort === s.sort) th.classList.add(s.dir);
  });

  document.getElementById('resultCount').textContent =
    `${total.toLocaleString()} match${total === 1 ? '' : 'es'}`;
  const first = total ? start + 1 : 0;
  const last = Math.min(start + PER_PAGE, total);
  document.getElementById('pgInfo').textContent = total
    ? `${first.toLocaleString()}–${last.toLocaleString()} of ${total.toLocaleString()}`
    : '';
  document.getElementById('pgPrev').disabled = page <= 1;
  document.getElementById('pgNext').disabled = last >= total;
}

function matchLabel(m) {
  return {
    fuzzy_zip_block: 'Fuzzy',
    mb_only: 'MB only',
    inspection_only: 'Inspection only',
  }[m] || m;
}
