async function renderMap() {
  const d = await (await fetch('/api/map')).json();
  const map = L.map('map').setView([30.27, -97.74], 11);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap', maxZoom: 19
  }).addTo(map);

  let layer = L.layerGroup().addTo(map);

  function draw(metric) {
    layer.clearLayers();
    const vals = d.zips.map(z => metric === 'revenue' ? Number(z.total_receipts) : Number(z.avg_score));
    const max = Math.max(...vals, 1);
    d.zips.forEach(z => {
      const val = metric === 'revenue' ? Number(z.total_receipts) : Number(z.avg_score);
      const radius = 6 + 30 * (val / max);
      const color = metric === 'revenue' ? '#4cc9f0' : '#f72585';
      L.circleMarker([z.latitude, z.longitude], {
        radius, color, fillColor: color, fillOpacity: 0.5, weight: 1
      }).bindPopup(
        `<b>ZIP ${z.zip}</b><br>Establishments: ${z.establishments}<br>` +
        `Avg score: ${Number(z.avg_score).toFixed(1)}<br>` +
        `Revenue: $${Number(z.total_receipts).toLocaleString()}`
      ).addTo(layer);
    });
  }

  draw('revenue');
  document.querySelectorAll('input[name=metric]').forEach(el =>
    el.addEventListener('change', e => draw(e.target.value))
  );
}
