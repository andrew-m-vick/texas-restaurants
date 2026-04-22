// Header search with typeahead. Fires on 250ms debounce.
(function () {
  const input = document.getElementById('searchInput');
  const panel = document.getElementById('searchResults');
  if (!input || !panel) return;

  let timer = null;
  let lastQ = '';

  async function run(q) {
    if (q === lastQ) return;
    lastQ = q;
    if (q.length < 2) { panel.classList.remove('open'); return; }
    const d = await (await fetch('/api/search?q=' + encodeURIComponent(q))).json();
    if (!d.results.length) {
      panel.innerHTML = '<div class="empty">No matches.</div>';
    } else {
      panel.innerHTML = d.results.map(r => `
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
    timer = setTimeout(() => run(q), 250);
  });
  input.addEventListener('focus', (e) => {
    if (e.target.value.trim().length >= 2) panel.classList.add('open');
  });
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-box')) panel.classList.remove('open');
  });
})();
