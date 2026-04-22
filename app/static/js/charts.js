const fmt$ = n => '$' + Number(n || 0).toLocaleString(undefined, {maximumFractionDigits: 0});
const fmtN = n => Number(n || 0).toLocaleString();
const palette = ['#4cc9f0','#f72585','#7209b7','#3a0ca3','#4361ee','#4895ef','#f9844a','#43aa8b','#f9c74f','#90be6d'];
const cityColor = { AUSTIN: '#4cc9f0', DALLAS: '#f72585' };

Chart.defaults.color = '#8b93a4';
Chart.defaults.borderColor = '#262b36';

function cityParam(extra = '') {
  const qs = new URLSearchParams();
  if (window.SELECTED_CITY && window.SELECTED_CITY !== 'ALL') qs.set('city', window.SELECTED_CITY);
  if (window.SELECTED_WINDOW && window.SELECTED_WINDOW !== '5y') qs.set('window', window.SELECTED_WINDOW);
  if (extra) extra.split('&').filter(Boolean).forEach(pair => {
    const [k, v] = pair.split('='); qs.set(k, v);
  });
  const s = qs.toString();
  return s ? `?${s}` : '';
}
async function fetchJSON(url, extra = '') {
  return (await fetch(url + cityParam(extra))).json();
}

// ---------- OVERVIEW ----------
async function renderOverview() {
  showLoading('kpis', 'topZips', 'bottomZips');
  const windowLabels = {'12m':'(last 12 mo)','3y':'(last 3 yr)','5y':'(last 5 yr)','all':'(2007–present)'};
  const wl = document.getElementById('windowLabel');
  if (wl) wl.textContent = windowLabels[window.SELECTED_WINDOW] || '';
  const d = await fetchJSON('/api/overview');
  clearLoading('kpis', 'topZips', 'bottomZips');
  const k = d.kpis || {};
  const selected = window.SELECTED_CITY;

  // Side-by-side KPIs when city=All, single when a city is selected.
  const kpiHost = document.getElementById('kpis');
  if (selected === 'ALL' && d.by_city && d.by_city.length > 1) {
    kpiHost.classList.add('kpis-grid-multi');
    kpiHost.innerHTML = d.by_city.map(c => `
      <div class="kpi-group" style="border-top: 3px solid ${cityColor[c.city] || '#888'}">
        <div class="kpi-group-city">${c.city}</div>
        <div class="kpi-mini">
          <div><span class="label">Establishments</span><span class="value">${fmtN(c.establishments)}</span></div>
          <div><span class="label">Avg Score</span><span class="value">${c.avg_score ?? '—'}</span></div>
          <div><span class="label">Total Revenue</span><span class="value">${fmt$(c.total_receipts)}</span></div>
          <div><span class="label">Inspections</span><span class="value">${fmtN(c.inspections)}</span></div>
        </div>
      </div>
    `).join('');
  } else {
    kpiHost.classList.remove('kpis-grid-multi');
    kpiHost.innerHTML = `
      <div class="kpi"><div class="label">Establishments</div><div class="value">${fmtN(k.establishments)}</div></div>
      <div class="kpi"><div class="label">Avg Inspection Score</div><div class="value">${k.avg_score ?? '—'}</div></div>
      <div class="kpi"><div class="label">Total Reported Revenue</div><div class="value">${fmt$(k.total_receipts)}</div></div>
      <div class="kpi"><div class="label">Inspections Recorded</div><div class="value">${fmtN(k.inspections)}</div></div>
    `;
  }

  const zipClickHandler = (rows) => (evt, active) => {
    if (!active.length) return;
    const r = rows[active[0].index];
    window.location = `/establishments?zip=${r.zip}&city=${r.city}`;
  };
  const zipHoverHandler = (canvas) => (evt, active) => {
    canvas.style.cursor = active.length ? 'pointer' : 'default';
  };

  if (!d.top_zips.length) replaceWithEmpty('topZips', 'No revenue data for this selection.');
  else {
    const el = document.getElementById('topZips');
    new Chart(el, {
      type: 'bar',
      data: {
        labels: d.top_zips.map(r => `${r.zip} (${r.city[0]})`),
        datasets: [{ label: 'Revenue', data: d.top_zips.map(r => r.receipts),
          backgroundColor: d.top_zips.map(r => cityColor[r.city] || palette[0]) }]
      },
      options: {
        maintainAspectRatio: false, plugins: { legend: { display: false } },
        onClick: zipClickHandler(d.top_zips),
        onHover: zipHoverHandler(el),
      }
    });
  }
  if (!d.bottom_zips.length) replaceWithEmpty('bottomZips', 'No inspection data for this selection.');
  else {
    const el = document.getElementById('bottomZips');
    new Chart(el, {
      type: 'bar',
      data: {
        labels: d.bottom_zips.map(r => `${r.zip} (${r.city[0]})`),
        datasets: [{ label: 'Avg Score', data: d.bottom_zips.map(r => r.avg_score),
          backgroundColor: d.bottom_zips.map(r => cityColor[r.city] || palette[1]) }]
      },
      options: {
        maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } },
        onClick: zipClickHandler(d.bottom_zips),
        onHover: zipHoverHandler(el),
      }
    });
  }
}

// ---------- REVENUE ----------
async function renderRevenue() {
  showLoading('monthly', 'byZip');
  const d = await fetchJSON('/api/revenue');
  clearLoading('monthly', 'byZip');

  if (!d.monthly.length) replaceWithEmpty('monthly', 'No monthly revenue data.');
  else {
    const byCity = {};
    for (const r of d.monthly) (byCity[r.city] = byCity[r.city] || []).push({ x: r.month, y: Number(r.total) });
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
  }

  if (!d.by_zip.length) replaceWithEmpty('byZip', 'No ZIP revenue data.');
  else new Chart(document.getElementById('byZip'), {
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
  showLoading('scoreDist', 'topViolations');
  const d = await fetchJSON('/api/inspections');
  clearLoading('scoreDist', 'topViolations');

  if (!d.distribution.length) replaceWithEmpty('scoreDist', 'No score data.');
  else new Chart(document.getElementById('scoreDist'), {
    type: 'doughnut',
    data: {
      labels: d.distribution.map(r => r.score_bucket),
      datasets: [{ data: d.distribution.map(r => r.inspections), backgroundColor: palette }]
    },
    options: { maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
  });

  if (!d.top_violations.length) {
    replaceWithEmpty('topViolations',
      window.SELECTED_CITY === 'AUSTIN'
        ? 'Austin publishes only overall scores, not violation detail.'
        : 'No violation data for this selection.');
  } else new Chart(document.getElementById('topViolations'), {
    type: 'bar',
    data: {
      labels: d.top_violations.map(r => (r.description || '').slice(0, 50)),
      datasets: [{ label: 'Occurrences', data: d.top_violations.map(r => r.occurrences), backgroundColor: palette[1] }]
    },
    options: { maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } } }
  });

  const tbody = document.querySelector('#repeatOffenders tbody');
  if (!d.repeat_offenders.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty-msg">No repeat low-score establishments.</td></tr>';
  } else {
    tbody.innerHTML = d.repeat_offenders.map(r => `
      <tr onclick="window.location='/establishment/${r.establishment_id}'">
        <td>${r.canonical_name}</td>
        <td>${r.city}</td>
        <td>${r.zip||''}</td>
        <td>${r.inspection_count}</td>
        <td>${r.low_score_count}</td>
        <td>${r.avg_score ?? ''}</td>
        <td>${r.min_score ?? ''}</td>
      </tr>
    `).join('');
    document.getElementById('repeatOffenders').classList.add('hover-table');
    makeSortable('repeatOffenders');
  }
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

let corrChart = null;
let corrAllPoints = [];

async function renderCorrelation() {
  showLoading('scatter');
  const d = await fetchJSON('/api/correlation');
  clearLoading('scatter');
  corrAllPoints = d.points;
  const slider = document.getElementById('confSlider');
  const val = document.getElementById('confValue');
  if (slider) {
    slider.addEventListener('input', () => {
      val.textContent = slider.value;
      drawCorrelation(Number(slider.value));
    });
  }
  drawCorrelation(slider ? Number(slider.value) : 0);
}

function drawCorrelation(minConf) {
  const filtered = corrAllPoints.filter(p => Number(p.match_score || 0) >= minConf);
  const byCity = {};
  for (const p of filtered) {
    const y = Number(p.avg_monthly_receipts);
    if (y <= 0) continue;
    (byCity[p.city] = byCity[p.city] || []).push({
      x: Number(p.avg_score), y,
      name: p.canonical_name, zip: p.zip, id: p.establishment_id,
    });
  }
  const datasets = Object.entries(byCity).map(([city, pts]) => ({
    label: `${city} (n=${pts.length})`, data: pts,
    backgroundColor: (cityColor[city] || palette[0]) + 'bb',
    pointRadius: 3,
  }));
  const all = Object.values(byCity).flat();
  const { m: slope, r: rawR } = linreg(all);
  const logPts = all.map(p => ({ x: p.x, y: Math.log10(p.y) }));
  const lg = linreg(logPts);

  if (all.length) {
    const xs = all.map(p => p.x);
    const minX = Math.min(...xs), maxX = Math.max(...xs);
    datasets.push({
      label: 'Regression', type: 'line',
      data: [
        { x: minX, y: Math.pow(10, lg.m * minX + lg.b) },
        { x: maxX, y: Math.pow(10, lg.m * maxX + lg.b) },
      ],
      borderColor: '#f9844a', backgroundColor: 'transparent',
      pointRadius: 0, borderWidth: 2,
    });
  }

  if (corrChart) corrChart.destroy();
  const scatterEl = document.getElementById('scatter');
  corrChart = new Chart(scatterEl, {
    type: 'scatter',
    data: { datasets },
    options: {
      maintainAspectRatio: false,
      onHover: (evt, active) => {
        scatterEl.style.cursor = active.length && active[0].element.$context.raw.id ? 'pointer' : 'default';
      },
      onClick: (evt, active) => {
        if (!active.length) return;
        const p = active[0].element.$context.raw;
        if (p.id) window.location = '/establishment/' + p.id;
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const p = ctx.raw;
              if (!p.name) return '';
              return `${p.name} (${p.zip || '—'}): ${p.x} pts, $${Math.round(p.y).toLocaleString()}/mo · click to drill in`;
            }
          }
        }
      },
      scales: {
        x: { min: 70, max: 100, title: { display: true, text: 'Avg inspection score' } },
        y: {
          type: 'logarithmic',
          title: { display: true, text: 'Avg monthly receipts ($, log scale)' },
          ticks: {
            callback: (v) => {
              const log = Math.log10(v);
              if (log !== Math.floor(log)) return '';
              return '$' + v.toLocaleString();
            }
          }
        }
      }
    }
  });

  document.getElementById('corrStat').innerHTML =
    `n=${all.length} · raw Pearson r = <b>${rawR.toFixed(3)}</b> · ` +
    `log-space Pearson r = <b>${lg.r.toFixed(3)}</b> · slope = $${Math.round(slope).toLocaleString()}/point. ` +
    (Math.abs(lg.r) < 0.1
      ? `<span class="muted">Null result: inspection score does not meaningfully predict bar revenue.</span>`
      : '');
}

// ---------- OPS ----------
async function renderOps() {
  const d = await (await fetch('/api/ops')).json();
  document.querySelector('#countsTable tbody').innerHTML = d.counts.map(r =>
    `<tr><td>${r.tbl}</td><td>${fmtN(r.n)}</td></tr>`
  ).join('');
  document.querySelector('#runsTable tbody').innerHTML = d.runs.map(r => `
    <tr>
      <td>${r.dag_id}</td><td>${r.layer}</td>
      <td>${r.started_at ? new Date(r.started_at).toLocaleString() : ''}</td>
      <td class="status-${r.status}">${r.status}</td>
      <td>${r.rows_written ?? ''}</td>
    </tr>`).join('');
  makeSortable('countsTable');
  makeSortable('runsTable');
}
