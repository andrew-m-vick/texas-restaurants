const fmt$ = n => '$' + Number(n || 0).toLocaleString(undefined, {maximumFractionDigits: 0});
const fmtN = n => Number(n || 0).toLocaleString();
const palette = ['#4cc9f0','#f72585','#7209b7','#3a0ca3','#4361ee','#4895ef','#f9844a','#43aa8b','#f9c74f','#90be6d'];
const cityColor = { AUSTIN: '#4cc9f0', DALLAS: '#f72585' };
// Single accent used for ZIP-level bar charts (overview top/bottom,
// revenue-by-zip). Matches the fork/spoon amber in the favicon.
const ZIP_COLOR = '#f9c74f';

Chart.defaults.color = '#8b93a4';
Chart.defaults.borderColor = '#262b36';

// Static JSON lookup. Endpoints like /api/overview are now precomputed
// into /static/data/overview-<window>.json by the monthly ETL.
function dataUrl(endpoint) {
  const w = window.SELECTED_WINDOW || '5y';
  return `/static/data/${endpoint}-${w}.json`;
}
async function fetchJSON(endpoint) {
  return (await fetch(dataUrl(endpoint))).json();
}

// ---------- OVERVIEW ----------
async function renderOverview() {
  showLoading('kpis', 'topZips', 'bottomZips');
  const windowLabels = {'12m':'(last 12 mo)','3y':'(last 3 yr)','5y':'(last 5 yr)','all':'(2007–present)'};
  const wl = document.getElementById('windowLabel');
  if (wl) wl.textContent = windowLabels[window.SELECTED_WINDOW] || '';
  const d = await fetchJSON('overview');
  clearLoading('kpis', 'topZips', 'bottomZips');
  const k = d.kpis || {};
  document.getElementById('kpis').innerHTML = `
    <div class="kpi"><div class="label">Establishments</div><div class="value">${fmtN(k.establishments)}</div></div>
    <div class="kpi"><div class="label">Avg Inspection Score</div><div class="value">${k.avg_score ?? '—'}</div></div>
    <div class="kpi"><div class="label">Total Reported Revenue</div><div class="value">${fmt$(k.total_receipts)}</div></div>
    <div class="kpi"><div class="label">Inspections Recorded</div><div class="value">${fmtN(k.inspections)}</div></div>
  `;

  const zipClickHandler = (rows) => (evt, active) => {
    if (!active.length) return;
    const r = rows[active[0].index];
    window.location = `/establishments?zip=${r.zip}`;
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
        labels: d.top_zips.map(r => r.zip),
        datasets: [{ label: 'Revenue', data: d.top_zips.map(r => r.receipts),
          backgroundColor: ZIP_COLOR }]
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
        labels: d.bottom_zips.map(r => r.zip),
        datasets: [{ label: 'Avg Score', data: d.bottom_zips.map(r => r.avg_score),
          backgroundColor: ZIP_COLOR }]
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
  const d = await fetchJSON('revenue');
  clearLoading('monthly', 'byZip');

  if (!d.monthly.length) replaceWithEmpty('monthly', 'No monthly revenue data.');
  else {
    const byMonth = new Map();
    for (const r of d.monthly) byMonth.set(r.month, (byMonth.get(r.month) || 0) + Number(r.total));
    const months = [...byMonth.keys()].sort();
    new Chart(document.getElementById('monthly'), {
      type: 'line',
      data: {
        datasets: [{
          label: 'Revenue',
          data: months.map(m => ({ x: m, y: byMonth.get(m) })),
          borderColor: palette[0],
          backgroundColor: palette[0] + '33',
          fill: true, tension: 0.2,
        }],
      },
      options: {
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { x: { type: 'category', labels: months } },
      }
    });
  }

  if (!d.by_zip.length) replaceWithEmpty('byZip', 'No ZIP revenue data.');
  else new Chart(document.getElementById('byZip'), {
    type: 'bar',
    data: {
      labels: d.by_zip.map(r => r.zip),
      datasets: [{ label: 'Revenue', data: d.by_zip.map(r => r.total),
        backgroundColor: ZIP_COLOR }]
    },
    options: { maintainAspectRatio: false, plugins: { legend: { display: false } } }
  });
}

// ---------- INSPECTIONS ----------
async function renderInspections() {
  showLoading('scoreDist', 'topViolations');
  const d = await fetchJSON('inspections');
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

  const tbody = document.querySelector('#repeatOffenders tbody');
  if (!d.repeat_offenders.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-msg">No repeat low-score establishments.</td></tr>';
  } else {
    tbody.innerHTML = d.repeat_offenders.map(r => `
      <tr onclick="window.location='/establishment/${r.establishment_id}'">
        <td>${r.canonical_name}</td>
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
  const d = await fetchJSON('correlation');
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
  const all = [];
  for (const p of filtered) {
    const y = Number(p.avg_monthly_receipts);
    if (y <= 0) continue;
    all.push({
      x: Number(p.avg_score), y,
      name: p.canonical_name, zip: p.zip, id: p.establishment_id,
    });
  }
  const datasets = [{
    label: `n=${all.length}`, data: all,
    backgroundColor: palette[0] + 'bb',
    pointRadius: 3,
  }];
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

// ---------- LIFECYCLE ----------
async function renderLifecycle() {
  const d = await (await fetch('/static/data/lifecycle.json')).json();

  new Chart(document.getElementById('statusChart'), {
    type: 'doughnut',
    data: {
      labels: d.status.map(r => r.status || '—'),
      datasets: [{ data: d.status.map(r => r.n), backgroundColor: palette }],
    },
    options: { maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
  });

  const points = d.tenure_vs_score.map(r => ({
    x: Number(r.tenure_years),
    y: Number(r.avg_score),
    name: r.canonical_name,
    zip: r.zip,
    id: r.establishment_id,
    n: r.inspection_count,
  })).filter(p => p.y > 0 && p.x >= 0);

  const { m, b, r } = linreg(points);
  const scatterEl = document.getElementById('tenureScatter');
  const xs = points.map(p => p.x);
  const minX = Math.min(...xs, 0), maxX = Math.max(...xs, 1);

  new Chart(scatterEl, {
    type: 'scatter',
    data: {
      datasets: [
        {
          label: `Establishments (n=${points.length})`,
          data: points,
          backgroundColor: '#4cc9f0aa',
          pointRadius: 3,
        },
        {
          label: 'Regression',
          type: 'line',
          data: [{ x: minX, y: m * minX + b }, { x: maxX, y: m * maxX + b }],
          borderColor: '#f9844a',
          backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 2,
        },
      ],
    },
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
              return `${p.name} (${p.zip || '—'}): ${p.x} yrs, ${p.y.toFixed(1)} pts · click to drill in`;
            }
          }
        }
      },
      scales: {
        x: { title: { display: true, text: 'Years since earliest TABC permit' } },
        y: { suggestedMin: 70, suggestedMax: 100, title: { display: true, text: 'Avg inspection score' } },
      }
    }
  });

  document.getElementById('tenureStat').innerHTML =
    `Pearson r = <b>${r.toFixed(3)}</b> · slope = ${m.toFixed(3)} pts/year. ` +
    (Math.abs(r) < 0.1
      ? `<span class="muted">Weak signal: tenure barely moves inspection score.</span>`
      : (r > 0
          ? `<span class="muted">Older establishments score modestly higher.</span>`
          : `<span class="muted">Older establishments score modestly lower.</span>`));

  const rows = d.status_by_zip.map(z => ({
    ...z,
    pct_expired: z.total ? ((Number(z.expired) / Number(z.total)) * 100).toFixed(1) : '0.0',
  }));
  document.querySelector('#statusByZip tbody').innerHTML = rows.map(r => `
    <tr>
      <td>${r.zip}</td>
      <td class="num">${r.total}</td>
      <td class="num">${r.active}</td>
      <td class="num">${r.expired}</td>
      <td class="num">${r.pct_expired}%</td>
    </tr>
  `).join('');
  makeSortable('statusByZip');
}

// ---------- OPS ----------
async function renderOps() {
  const d = await (await fetch('/static/data/ops.json')).json();
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
