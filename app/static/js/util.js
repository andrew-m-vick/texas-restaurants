// Shared UI utilities: loading states, empty placeholders, sortable tables.

function showLoading(...ids) {
  for (const id of ids) {
    const el = document.getElementById(id);
    if (!el) continue;
    const card = el.closest('.card');
    if (card) card.classList.add('loading');
  }
}
function clearLoading(...ids) {
  for (const id of ids) {
    const el = document.getElementById(id);
    const card = el && el.closest('.card');
    if (card) card.classList.remove('loading');
  }
}
function emptyMsg(container, text) {
  const el = (typeof container === 'string') ? document.getElementById(container) : container;
  if (!el) return;
  el.outerHTML = `<div class="empty-msg">${text}</div>`;
}
function replaceWithEmpty(canvasId, text) {
  const c = document.getElementById(canvasId);
  if (!c) return;
  const card = c.closest('.card');
  if (card) card.classList.remove('loading');
  c.outerHTML = `<div class="empty-msg">${text}</div>`;
}

// Generic sortable-table wiring. Call once per table.
function makeSortable(tableId) {
  const table = document.getElementById(tableId);
  if (!table) return;
  const ths = table.querySelectorAll('thead th');
  ths.forEach((th, colIdx) => {
    th.classList.add('sortable');
    th.addEventListener('click', () => {
      const tbody = table.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));
      const asc = !th.classList.contains('asc');
      ths.forEach(h => h.classList.remove('asc', 'desc'));
      th.classList.add(asc ? 'asc' : 'desc');
      rows.sort((a, b) => {
        const av = a.children[colIdx].textContent.trim();
        const bv = b.children[colIdx].textContent.trim();
        const an = parseFloat(av), bn = parseFloat(bv);
        const both = !isNaN(an) && !isNaN(bn);
        if (both) return asc ? an - bn : bn - an;
        return asc ? av.localeCompare(bv) : bv.localeCompare(av);
      });
      rows.forEach(r => tbody.appendChild(r));
    });
  });
}
