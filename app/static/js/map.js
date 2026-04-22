async function renderMap() {
  const w = window.SELECTED_WINDOW || '5y';
  const [d, estBundle] = await Promise.all([
    (await fetch(`/static/data/map-${w}.json`)).json(),
    (await fetch(`/static/data/establishments-${w}.json`)).json(),
  ]);
  const allEstablishments = estBundle.rows || [];

  const centers = { AUSTIN: [30.27, -97.74], DALLAS: [32.78, -96.80] };
  const center = centers[window.SELECTED_CITY] || [31.5, -97.3];
  const zoom = (window.SELECTED_CITY && window.SELECTED_CITY !== 'ALL') ? 11 : 7;
  const map = L.map('map').setView(center, zoom);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap', maxZoom: 19
  }).addTo(map);

  let layer = L.layerGroup().addTo(map);
  const panel = document.getElementById('zipPanel');

  function showZip(zip, city) {
    const matches = allEstablishments
      .filter(e => e.zip === zip && (!city || e.city === city))
      .sort((a, b) => (b.avg_monthly_receipts || 0) - (a.avg_monthly_receipts || 0))
      .slice(0, 50);

    if (!matches.length) {
      panel.innerHTML = `<h3>ZIP ${zip}</h3><div class="empty-msg">No establishments.</div>`;
      return;
    }
    panel.innerHTML = `
      <h3>ZIP ${zip} <span class="muted" style="font-size:0.85rem;font-weight:normal">· ${matches.length} establishments</span></h3>
      <ul class="zip-list">
        ${matches.map(e => `
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
      const color = '#7209b7';
      const marker = L.circleMarker([z.latitude, z.longitude], {
        radius, color: '#0b0f1a', fillColor: color, fillOpacity: 0.75, weight: 1.5
      }).bindTooltip(
        `<b>ZIP ${z.zip}</b><br>` +
        `Establishments: ${z.establishments}<br>` +
        `Avg score: ${Number(z.avg_score).toFixed(1)}<br>` +
        `Revenue: $${Number(z.total_receipts).toLocaleString()}`
      );
      marker.on('click', () => showZip(z.zip, z.city));
      marker.addTo(layer);
    });
  }

  draw('revenue');
  document.querySelectorAll('input[name=metric]').forEach(el =>
    el.addEventListener('change', e => draw(e.target.value))
  );
}
