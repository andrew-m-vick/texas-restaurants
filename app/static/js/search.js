// Header search with typeahead. The establishment index is precomputed
// into /static/data/search.json and filtered client-side.
(function () {
  const input = document.getElementById('searchInput');
  const panel = document.getElementById('searchResults');
  if (!input || !panel) return;

  let indexP = null;
  function loadIndex() {
    if (!indexP) {
      indexP = fetch('/static/data/search.json')
        .then(r => r.json())
        .then(d => d.results || []);
    }
    return indexP;
  }

  let timer = null;
  let lastQ = '';

  async function run(q) {
    if (q === lastQ) return;
    lastQ = q;
    if (q.length < 2) { panel.classList.remove('open'); return; }
    const rows = await loadIndex();
    const needle = q.toUpperCase();
    const results = [];
    for (const r of rows) {
      const name = (r.canonical_name || '').toUpperCase();
      const addr = (r.canonical_address || '').toUpperCase();
      if (name.includes(needle) || addr.includes(needle)) {
        results.push(r);
        if (results.length >= 15) break;
      }
    }
    if (!results.length) {
      panel.innerHTML = '<div class="empty">No matches.</div>';
    } else {
      panel.innerHTML = results.map(r => `
        <a href="/establishment/${r.id}">
          <div class="name">${r.canonical_name}</div>
          <div class="meta">${r.city} · ${r.zip || '—'} · ${r.canonical_address || ''} · ${r.match_method}</div>
        </a>
      `).join('');
    }
    panel.classList.add('open');
  }

  input.addEventListener('input', (e) => {
    clearTimeout(timer);
    const q = e.target.value.trim();
    timer = setTimeout(() => run(q), 200);
  });
  input.addEventListener('focus', (e) => {
    if (e.target.value.trim().length >= 2) panel.classList.add('open');
  });
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-box')) panel.classList.remove('open');
  });
})();
