// Correlation & Lifecycle — score-vs-revenue scatter (with confidence
// slider), tenure-vs-score scatter, permit-status doughnut, churn-by-ZIP
// table.
import { useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Doughnut, Scatter } from 'react-chartjs-2';
import type {
  CorrelationData,
  LifecycleData,
} from '../types';
import { useData, dataUrl } from '../lib/data';
import { useWindow } from '../lib/window';
import { Empty, ErrorMsg, Loading } from '../components/Loading';
import { PALETTE } from '../lib/charts-setup';
import { useSortable } from '../lib/sort';

interface XY {
  x: number;
  y: number;
  name: string;
  zip: string | null;
  id: number;
}

function linreg(points: { x: number; y: number }[]) {
  const n = points.length;
  if (!n) return { m: 0, b: 0, r: 0 };
  let sx = 0, sy = 0, sxy = 0, sxx = 0, syy = 0;
  for (const p of points) {
    sx += p.x;
    sy += p.y;
    sxy += p.x * p.y;
    sxx += p.x * p.x;
    syy += p.y * p.y;
  }
  const m = (n * sxy - sx * sy) / (n * sxx - sx * sx || 1);
  const b = (sy - m * sx) / n;
  const r =
    (n * sxy - sx * sy) /
    Math.sqrt((n * sxx - sx * sx) * (n * syy - sy * sy) || 1);
  return { m, b, r };
}

type ChurnRow = LifecycleData['status_by_zip'][number] & { pct_expired: number };
type ChurnCol = 'zip' | 'total' | 'active' | 'expired' | 'cancelled' | 'pct_expired';

const churnAccessor = (r: ChurnRow, col: ChurnCol) => r[col];

export default function Correlation() {
  const [window] = useWindow();
  const corr = useData<CorrelationData>(dataUrl('correlation', window));
  const life = useData<LifecycleData>(dataUrl('lifecycle'));
  const navigate = useNavigate();

  const [minConf, setMinConf] = useState(0);

  // ---- score vs revenue ----
  const corrPoints: XY[] = useMemo(() => {
    if (corr.status !== 'ready') return [];
    return corr.data.points
      .filter((p) => Number(p.match_score || 0) >= minConf)
      .map((p) => ({
        x: Number(p.avg_score),
        y: Number(p.avg_monthly_receipts),
        name: p.canonical_name,
        zip: p.zip,
        id: p.establishment_id,
      }))
      .filter((p) => p.y > 0);
  }, [corr, minConf]);

  const corrStats = useMemo(() => {
    const raw = linreg(corrPoints);
    const log = linreg(corrPoints.map((p) => ({ x: p.x, y: Math.log10(p.y) })));
    return { raw, log };
  }, [corrPoints]);

  const corrRegression = useMemo(() => {
    if (!corrPoints.length) return [];
    const xs = corrPoints.map((p) => p.x);
    const minX = Math.min(...xs);
    const maxX = Math.max(...xs);
    return [
      { x: minX, y: Math.pow(10, corrStats.log.m * minX + corrStats.log.b) },
      { x: maxX, y: Math.pow(10, corrStats.log.m * maxX + corrStats.log.b) },
    ];
  }, [corrPoints, corrStats]);

  // ---- tenure vs score ----
  const tenurePoints: XY[] = useMemo(() => {
    if (life.status !== 'ready') return [];
    return life.data.tenure_vs_score
      .map((r) => ({
        x: Number(r.tenure_years),
        y: Number(r.avg_score),
        name: r.canonical_name,
        zip: r.zip,
        id: r.establishment_id,
      }))
      .filter((p) => p.y > 0 && p.x >= 0);
  }, [life]);

  const tenureStats = useMemo(() => linreg(tenurePoints), [tenurePoints]);

  const tenureRegression = useMemo(() => {
    if (!tenurePoints.length) return [];
    const xs = tenurePoints.map((p) => p.x);
    const minX = Math.min(...xs, 0);
    const maxX = Math.max(...xs, 1);
    return [
      { x: minX, y: tenureStats.m * minX + tenureStats.b },
      { x: maxX, y: tenureStats.m * maxX + tenureStats.b },
    ];
  }, [tenurePoints, tenureStats]);

  // ---- churn table ----
  const churnRows: ChurnRow[] = useMemo(() => {
    if (life.status !== 'ready') return [];
    return life.data.status_by_zip.map((r) => ({
      ...r,
      pct_expired: r.total ? (Number(r.expired) / Number(r.total)) * 100 : 0,
    }));
  }, [life]);

  const churn = useSortable<ChurnRow, ChurnCol>(
    churnRows,
    { col: 'total', dir: 'desc' },
    churnAccessor
  );

  if (corr.status === 'loading' || life.status === 'loading') return <Loading />;
  if (corr.status === 'error') return <ErrorMsg error={corr.error} />;
  if (life.status === 'error') return <ErrorMsg error={life.error} />;

  const sortClass = (col: ChurnCol) =>
    churn.sort.col === col ? `sortable ${churn.sort.dir}` : 'sortable';

  return (
    <>
      <h1>Correlation &amp; Lifecycle</h1>
      <p className="muted">
        Two lenses on the same matched establishments: revenue vs. inspection
        score, and tenure vs. inspection score.
      </p>

      <h2 style={{ marginTop: '1.5rem' }}>Inspection score vs. revenue</h2>
      <div className="controls-row">
        <label htmlFor="confSlider">Minimum match confidence:</label>
        <input
          id="confSlider"
          type="range"
          min={0}
          max={100}
          step={5}
          value={minConf}
          onChange={(e) => setMinConf(Number(e.target.value))}
          style={{ width: 220 }}
        />
        <span>{minConf}</span>
        <span className="muted">
          (higher = only include higher-quality name/address matches)
        </span>
      </div>
      <div className="card tall">
        {!corrPoints.length ? (
          <Empty label="No matched points at this confidence threshold." />
        ) : (
          <Scatter
            data={{
              datasets: [
                {
                  label: `n=${corrPoints.length}`,
                  data: corrPoints,
                  backgroundColor: PALETTE[0] + 'bb',
                  pointRadius: 3,
                },
                {
                  label: 'Regression',
                  type: 'line' as const,
                  data: corrRegression,
                  borderColor: '#f9844a',
                  backgroundColor: 'transparent',
                  pointRadius: 0,
                  borderWidth: 2,
                },
              ],
            }}
            options={{
              maintainAspectRatio: false,
              onHover: (evt, active) => {
                const target = evt.native?.target as HTMLElement | null;
                if (target) {
                  target.style.cursor =
                    active.length && (active[0].element as { $context?: { raw?: { id?: number } } })?.$context?.raw?.id
                      ? 'pointer'
                      : 'default';
                }
              },
              onClick: (_evt, active) => {
                if (!active.length) return;
                const raw = (active[0].element as unknown as { $context: { raw: XY } }).$context.raw;
                if (raw?.id) navigate(`/establishment/${raw.id}`);
              },
              plugins: {
                tooltip: {
                  callbacks: {
                    label: (ctx) => {
                      const p = ctx.raw as XY;
                      if (!p?.name) return '';
                      return `${p.name} (${p.zip || '—'}): ${p.x} pts, $${Math.round(
                        p.y
                      ).toLocaleString()}/mo · click to drill in`;
                    },
                  },
                },
              },
              scales: {
                x: {
                  min: 70,
                  max: 100,
                  title: { display: true, text: 'Avg inspection score' },
                },
                y: {
                  type: 'logarithmic',
                  title: { display: true, text: 'Avg monthly receipts ($, log scale)' },
                  ticks: {
                    callback: (v) => {
                      const log = Math.log10(Number(v));
                      if (log !== Math.floor(log)) return '';
                      return '$' + Number(v).toLocaleString();
                    },
                  },
                },
              },
            }}
          />
        )}
      </div>
      <p className="muted">
        n={corrPoints.length} · raw Pearson r ={' '}
        <b>{corrStats.raw.r.toFixed(3)}</b> · log-space Pearson r ={' '}
        <b>{corrStats.log.r.toFixed(3)}</b> · slope = $
        {Math.round(corrStats.raw.m).toLocaleString()}/point.{' '}
        {Math.abs(corrStats.log.r) < 0.1 && (
          <span className="muted">
            Null result: inspection score does not meaningfully predict bar revenue.
          </span>
        )}
      </p>

      <h2 style={{ marginTop: '2rem' }}>Tenure vs. inspection score</h2>
      <p className="muted">
        Years since earliest TABC permit (original-issue date) plotted against
        the establishment's average inspection score. Each point is one
        establishment with at least two inspections.
      </p>
      <div className="card tall">
        {!tenurePoints.length ? (
          <Empty label="No matched establishments with TABC tenure data." />
        ) : (
          <Scatter
            data={{
              datasets: [
                {
                  label: `Establishments (n=${tenurePoints.length})`,
                  data: tenurePoints,
                  backgroundColor: '#4cc9f0aa',
                  pointRadius: 3,
                },
                {
                  label: 'Regression',
                  type: 'line' as const,
                  data: tenureRegression,
                  borderColor: '#f9844a',
                  backgroundColor: 'transparent',
                  pointRadius: 0,
                  borderWidth: 2,
                },
              ],
            }}
            options={{
              maintainAspectRatio: false,
              onClick: (_evt, active) => {
                if (!active.length) return;
                const raw = (active[0].element as unknown as { $context: { raw: XY } }).$context.raw;
                if (raw?.id) navigate(`/establishment/${raw.id}`);
              },
              plugins: {
                tooltip: {
                  callbacks: {
                    label: (ctx) => {
                      const p = ctx.raw as XY;
                      if (!p?.name) return '';
                      return `${p.name} (${p.zip || '—'}): ${p.x} yrs, ${p.y.toFixed(
                        1
                      )} pts · click to drill in`;
                    },
                  },
                },
              },
              scales: {
                x: { title: { display: true, text: 'Years since earliest TABC permit' } },
                y: {
                  suggestedMin: 70,
                  suggestedMax: 100,
                  title: { display: true, text: 'Avg inspection score' },
                },
              },
            }}
          />
        )}
      </div>
      <p className="muted">
        Pearson r = <b>{tenureStats.r.toFixed(3)}</b> · slope ={' '}
        {tenureStats.m.toFixed(3)} pts/year.{' '}
        {Math.abs(tenureStats.r) < 0.1 ? (
          <span className="muted">
            Weak signal: tenure barely moves inspection score.
          </span>
        ) : tenureStats.r > 0 ? (
          <span className="muted">Older establishments score modestly higher.</span>
        ) : (
          <span className="muted">Older establishments score modestly lower.</span>
        )}
      </p>

      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2>TABC permit status</h2>
        <div style={{ maxHeight: 260 }}>
          <Doughnut
            data={{
              labels: life.data.status.map((r) => r.status || '—'),
              datasets: [{ data: life.data.status.map((r) => r.n), backgroundColor: PALETTE }],
            }}
            options={{
              maintainAspectRatio: false,
              plugins: { legend: { position: 'right' } },
            }}
          />
        </div>
      </div>

      <div className="card">
        <h2>Permit churn by ZIP</h2>
        <table id="statusByZip" className="hover-table">
          <thead>
            <tr>
              <th className={sortClass('zip')} onClick={() => churn.toggle('zip')}>ZIP</th>
              <th className={`num ${sortClass('total')}`} onClick={() => churn.toggle('total')}>Permits</th>
              <th className={`num ${sortClass('active')}`} onClick={() => churn.toggle('active')}>Active</th>
              <th className={`num ${sortClass('expired')}`} onClick={() => churn.toggle('expired')}>Expired</th>
              <th className={`num ${sortClass('pct_expired')}`} onClick={() => churn.toggle('pct_expired')}>% Expired</th>
            </tr>
          </thead>
          <tbody>
            {churn.sorted.map((r) => (
              <tr key={r.zip}>
                <td>{r.zip}</td>
                <td className="num">{r.total}</td>
                <td className="num">{r.active}</td>
                <td className="num">{r.expired}</td>
                <td className="num">{r.pct_expired.toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
