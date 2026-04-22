async function renderMap() {
  const cityQs = (window.SELECTED_CITY && window.SELECTED_CITY !== 'ALL')
    ? `?city=${window.SELECTED_CITY}` : '';
  const d = await (await fetch('/api/map' + cityQs)).json();
  const centers = { AUSTIN: [30.27, -97.74], DALLAS: [32.78, -96.80] };
  const center = centers[window.SELECTED_CITY] || [31.5, -97.3];
  const zoom = (window.SELECTED_CITY && window.SELECTED_CITY !== 'ALL') ? 11 : 7;
  const map = L.map('map').setView(center, zoom);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap', maxZoom: 19
  }).addTo(map);

  let layer = L.layerGroup().addTo(map);
  const panel = document.getElementById('zipPanel');

  async function showZip(zip, city) {
    panel.innerHTML = '<div class="empty-msg"><span class="spinner"></span> Loading…</div>';
    const qs = new URLSearchParams();
    if (city) qs.set('city', city);
    const resp = await (await fetch(`/api/zip/${zip}?${qs}`)).json();
    if (!resp.establishments.length) {
      panel.innerHTML = `<h3>ZIP ${zip}</h3><div class="empty-msg">No establishments.</div>`;
      return;
    }
    panel.innerHTML = `
      <h3>ZIP ${zip} <span class="muted" style="font-size:0.85rem;font-weight:normal">· ${resp.establishments.length} establishments</span></h3>
      <ul class="zip-list">
        ${resp.establishments.map(e => `
          <li>
            <a href="/establishment/${e.id}">
              <div class="name">${e.canonical_name}</div>
              <div class="meta">
                ${e.city} · ${e.match_method}
                ${e.avg_score != null ? `· score ${Number(e.avg_score).toFixed(1)}` : ''}
                ${e.avg_monthly_receipts != null
                  ? `· $${Math.round(e.avg_monthly_receipts).toLocaleString()}/mo`
                  : ''}
              </div>
            </a>
          </li>`).join('')}
      </ul>`;
  }

  function draw(metric) {
    layer.clearLayers();
    const vals = d.zips.map(z => metric === 'revenue' ? Number(z.total_receipts) : Number(z.avg_score));
    const max = Math.max(...vals, 1);
    d.zips.forEach(z => {
      const val = metric === 'revenue' ? Number(z.total_receipts) : Number(z.avg_score);
      const radius = 6 + 30 * (val / max);
      const color = cityColorFor(z.city, metric);
      const marker = L.circleMarker([z.latitude, z.longitude], {
        radius, color, fillColor: color, fillOpacity: 0.5, weight: 1
      }).bindTooltip(
        `<b>ZIP ${z.zip}</b> (${z.city})<br>` +
        `Establishments: ${z.establishments}<br>` +
        `Avg score: ${Number(z.avg_score).toFixed(1)}<br>` +
        `Revenue: $${Number(z.total_receipts).toLocaleString()}`
      );
      marker.on('click', () => showZip(z.zip, z.city));
      marker.addTo(layer);
    });
  }

  function cityColorFor(city, metric) {
    if (metric === 'revenue') return city === 'DALLAS' ? '#f72585' : '#4cc9f0';
    return city === 'DALLAS' ? '#f9844a' : '#43aa8b';
  }

  draw('revenue');
  document.querySelectorAll('input[name=metric]').forEach(el =>
    el.addEventListener('change', e => draw(e.target.value))
  );
}
