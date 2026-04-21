const fmt$ = n => '$' + Number(n || 0).toLocaleString(undefined, {maximumFractionDigits: 0});
const palette = ['#4cc9f0','#f72585','#7209b7','#3a0ca3','#4361ee','#4895ef','#f9844a','#43aa8b','#f9c74f','#90be6d'];
const cityColor = { AUSTIN: '#4cc9f0', DALLAS: '#f72585' };

Chart.defaults.color = '#8b93a4';
Chart.defaults.borderColor = '#262b36';

function cityParam() {
  const c = window.SELECTED_CITY;
  return (c && c !== 'ALL') ? `?city=${c}` : '';
}
async function fetchJSON(url) { return (await fetch(url + cityParam())).json(); }

// ---------- OVERVIEW ----------
async function renderOverview() {
  const d = await fetchJSON('/api/overview');
  const k = d.kpis || {};
  document.getElementById('kpis').innerHTML = `
    <div class="kpi"><div class="label">Establishments</div><div class="value">${Number(k.establishments||0).toLocaleString()}</div></div>
    <div class="kpi"><div class="label">Avg Inspection Score</div><div class="value">${k.avg_score ?? '—'}</div></div>
    <div class="kpi"><div class="label">Total Reported Revenue</div><div class="value">${fmt$(k.total_receipts)}</div></div>
    <div class="kpi"><div class="label">Inspections Recorded</div><div class="value">${Number(k.inspections||0).toLocaleString()}</div></div>
  `;
  new Chart(document.getElementById('topZips'), {
    type: 'bar',
    data: {
      labels: d.top_zips.map(r => `${r.zip} (${r.city[0]})`),
      datasets: [{ label: 'Revenue', data: d.top_zips.map(r => r.receipts),
        backgroundColor: d.top_zips.map(r => cityColor[r.city] || palette[0]) }]
    },
    options: { maintainAspectRatio: false, plugins: { legend: { display: false } } }
  });
  new Chart(document.getElementById('bottomZips'), {
    type: 'bar',
    data: {
      labels: d.bottom_zips.map(r => `${r.zip} (${r.city[0]})`),
      datasets: [{ label: 'Avg Score', data: d.bottom_zips.map(r => r.avg_score),
        backgroundColor: d.bottom_zips.map(r => cityColor[r.city] || palette[1]) }]
    },
    options: { maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } } }
  });
}

// ---------- REVENUE ----------
async function renderRevenue() {
  const d = await fetchJSON('/api/revenue');
  // Group monthly by city
  const byCity = {};
  for (const r of d.monthly) {
    (byCity[r.city] = byCity[r.city] || []).push({ x: r.month, y: Number(r.total) });
  }
  const datasets = Object.entries(byCity).map(([city, pts]) => ({
    label: city, data: pts,
    borderColor: cityColor[city] || palette[0],
    backgroundColor: (cityColor[city] || palette[0]) + '33',
    fill: false, tension: 0.2,
  }));
  new Chart(document.getElementById('monthly'), {
    type: 'line',
    data: { datasets },
    options: { maintainAspectRatio: false,
      scales: { x: { type: 'category', labels: [...new Set(d.monthly.map(r=>r.month))].sort() } } }
  });
  new Chart(document.getElementById('byZip'), {
    type: 'bar',
    data: {
      labels: d.by_zip.map(r => `${r.zip} (${r.city[0]})`),
      datasets: [{ label: 'Revenue', data: d.by_zip.map(r => r.total),
        backgroundColor: d.by_zip.map(r => cityColor[r.city] || palette[2]) }]
    },
    options: { maintainAspectRatio: false, plugins: { legend: { display: false } } }
  });
}

// ---------- INSPECTIONS ----------
async function renderInspections() {
  const d = await fetchJSON('/api/inspections');
  new Chart(document.getElementById('scoreDist'), {
    type: 'doughnut',
    data: {
      labels: d.distribution.map(r => r.score_bucket),
      datasets: [{ data: d.distribution.map(r => r.inspections), backgroundColor: palette }]
    },
    options: { maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
  });
  new Chart(document.getElementById('topViolations'), {
    type: 'bar',
    data: {
      labels: d.top_violations.map(r => (r.description || '').slice(0, 40)),
      datasets: [{ label: 'Occurrences', data: d.top_violations.map(r => r.occurrences), backgroundColor: palette[1] }]
    },
    options: { maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } } }
  });
  const tbody = document.querySelector('#repeatOffenders tbody');
  tbody.innerHTML = d.repeat_offenders.map(r => `
    <tr>
      <td>${r.canonical_name}</td>
      <td>${r.city}</td>
      <td>${r.zip||''}</td>
      <td>${r.inspection_count}</td>
      <td>${r.low_score_count}</td>
      <td>${r.avg_score ?? ''}</td>
      <td>${r.min_score ?? ''}</td>
    </tr>
  `).join('');
}

// ---------- CORRELATION ----------
function linreg(points) {
  const n = points.length;
  if (!n) return { m: 0, b: 0, r: 0 };
  let sx=0, sy=0, sxy=0, sxx=0, syy=0;
  for (const p of points) { sx+=p.x; sy+=p.y; sxy+=p.x*p.y; sxx+=p.x*p.x; syy+=p.y*p.y; }
  const m = (n*sxy - sx*sy) / (n*sxx - sx*sx || 1);
  const b = (sy - m*sx) / n;
  const r = (n*sxy - sx*sy) / Math.sqrt((n*sxx - sx*sx) * (n*syy - sy*sy) || 1);
  return { m, b, r };
}
async function renderCorrelation() {
  const d = await fetchJSON('/api/correlation');
  const byCity = {};
  for (const p of d.points) {
    (byCity[p.city] = byCity[p.city] || []).push({
      x: Number(p.avg_score), y: Number(p.avg_monthly_receipts), name: p.canonical_name
    });
  }
  const datasets = Object.entries(byCity).map(([city, pts]) => ({
    label: `${city} (n=${pts.length})`, data: pts,
    backgroundColor: (cityColor[city] || palette[0]) + 'aa',
  }));
  // Combined regression line across all points
  const all = d.points.map(p => ({ x: Number(p.avg_score), y: Number(p.avg_monthly_receipts) }));
  const { m, b, r } = linreg(all);
  if (all.length) {
    const xs = all.map(p => p.x);
    const minX = Math.min(...xs), maxX = Math.max(...xs);
    datasets.push({
      label: 'Regression', type: 'line',
      data: [{x: minX, y: m*minX+b},{x: maxX, y: m*maxX+b}],
      borderColor: '#f9844a', backgroundColor: 'transparent', pointRadius: 0,
    });
  }
  new Chart(document.getElementById('scatter'), {
    type: 'scatter',
    data: { datasets },
    options: {
      maintainAspectRatio: false,
      scales: {
        x: { title: { display: true, text: 'Avg inspection score' } },
        y: { title: { display: true, text: 'Avg monthly receipts ($)' } }
      }
    }
  });
  document.getElementById('corrStat').textContent =
    `n=${all.length} · Pearson r = ${r.toFixed(3)} · slope = ${m.toFixed(0)} $/point`;
}

// ---------- OPS ----------
async function renderOps() {
  const d = await (await fetch('/api/ops')).json();
  document.querySelector('#countsTable tbody').innerHTML = d.counts.map(r =>
    `<tr><td>${r.tbl}</td><td>${Number(r.n).toLocaleString()}</td></tr>`
  ).join('');
  document.querySelector('#runsTable tbody').innerHTML = d.runs.map(r => `
    <tr>
      <td>${r.dag_id}</td><td>${r.layer}</td>
      <td>${r.started_at ? new Date(r.started_at).toLocaleString() : ''}</td>
      <td class="status-${r.status}">${r.status}</td>
      <td>${r.rows_written ?? ''}</td>
    </tr>`).join('');
}
