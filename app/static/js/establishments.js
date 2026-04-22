// Establishments browse tab: filter + sort + paginate 20k+ rows.
// State lives in the URL query string so pages stay shareable.

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
    // Reset page on filter change (not sort/page changes)
    if (!('page' in patch) && !('sort' in patch) && !('dir' in patch)) {
      url.searchParams.delete('page');
    }
    window.history.replaceState({}, '', url);
  },
};

function renderEstablishments() {
  const s = ESTState.read();
  // Prefill filter inputs from URL
  document.getElementById('fName').value = s.q;
  document.getElementById('fZip').value = s.zip;
  document.getElementById('fMatch').value = s.match;
  document.getElementById('fMinScore').value = s.min_score;
  document.getElementById('fMaxScore').value = s.max_score;

  // Debounced filter application
  let timer = null;
  const apply = (patch) => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      ESTState.write(patch);
      load();
    }, 200);
  };
  document.getElementById('fName').addEventListener('input', e => apply({ q: e.target.value.trim() }));
  document.getElementById('fZip').addEventListener('input', e => apply({ zip: e.target.value.trim() }));
  document.getElementById('fMatch').addEventListener('change', e => apply({ match: e.target.value }));
  document.getElementById('fMinScore').addEventListener('input', e => apply({ min_score: e.target.value }));
  document.getElementById('fMaxScore').addEventListener('input', e => apply({ max_score: e.target.value }));
  document.getElementById('fReset').addEventListener('click', () => {
    window.location.href = window.location.pathname;
  });

  // Header sorting
  document.querySelectorAll('#estTable thead th[data-sort]').forEach(th => {
    th.classList.add('sortable');
    th.addEventListener('click', () => {
      const cur = ESTState.read();
      const col = th.dataset.sort;
      const dir = (cur.sort === col && cur.dir === 'asc') ? 'desc' : 'asc';
      ESTState.write({ sort: col, dir });
      load();
    });
  });

  document.getElementById('pgPrev').addEventListener('click', () => {
    const cur = ESTState.read();
    if (cur.page > 1) { ESTState.write({ page: cur.page - 1 }); load(); }
  });
  document.getElementById('pgNext').addEventListener('click', () => {
    const cur = ESTState.read();
    ESTState.write({ page: cur.page + 1 });
    load();
  });

  load();
}

async function load() {
  const s = ESTState.read();
  const qs = new URLSearchParams();
  if (window.SELECTED_CITY && window.SELECTED_CITY !== 'ALL') qs.set('city', window.SELECTED_CITY);
  for (const k of ['q','zip','match','min_score','max_score','sort','dir','page']) {
    if (s[k] !== '' && s[k] != null && s[k] !== 1) qs.set(k, s[k]);
  }
  if (s.page > 1) qs.set('page', s.page);

  const tbody = document.querySelector('#estTable tbody');
  tbody.innerHTML = '<tr><td colspan="8" class="empty-msg"><span class="spinner"></span> Loading…</td></tr>';

  const d = await (await fetch('/api/establishments?' + qs.toString())).json();
  const fmtN = n => n == null ? '—' : Number(n).toLocaleString();
  const fmt$ = n => n == null ? '—' : '$' + Math.round(Number(n)).toLocaleString();
  const fmtS = n => n == null ? '—' : Number(n).toFixed(1);

  if (!d.rows.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty-msg">No establishments match those filters.</td></tr>';
  } else {
    tbody.innerHTML = d.rows.map(r => `
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

  // Header sort indicators
  document.querySelectorAll('#estTable thead th[data-sort]').forEach(th => {
    th.classList.remove('asc', 'desc');
    if (th.dataset.sort === s.sort) th.classList.add(s.dir);
  });

  // Pagination info
  document.getElementById('resultCount').textContent =
    `${d.total.toLocaleString()} match${d.total === 1 ? '' : 'es'}`;
  const start = (s.page - 1) * d.per_page + 1;
  const end = Math.min(s.page * d.per_page, d.total);
  document.getElementById('pgInfo').textContent = d.total
    ? `${start.toLocaleString()}–${end.toLocaleString()} of ${d.total.toLocaleString()}`
    : '';
  document.getElementById('pgPrev').disabled = s.page <= 1;
  document.getElementById('pgNext').disabled = end >= d.total;
}

function matchLabel(m) {
  return {
    fuzzy_zip_block: 'Fuzzy',
    mb_only: 'MB only',
    inspection_only: 'Inspection only',
  }[m] || m;
}
