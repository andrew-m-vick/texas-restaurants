// Overview page — KPIs + top/bottom ZIPs.
//
// This is the worked example for the rest of the app. The shape is:
//   1. Read the current time-window from the URL.
//   2. useData<T>(...) fetches the matching JSON file.
//   3. Switch on state to show loading/error/ready.
//   4. Render KPIs as plain divs and the ZIP bars via the shared
//      <ZipBarChart /> component.
import type { OverviewData } from '../types';
import { useData, dataUrl } from '../lib/data';
import { useWindow } from '../lib/window';
import { fmtMoney, fmtNumber } from '../lib/format';
import { Empty, ErrorMsg, Loading } from '../components/Loading';
import ZipBarChart from '../components/ZipBarChart';

const WINDOW_LABELS: Record<string, string> = {
  '12m': '(last 12 mo)',
  '3y': '(last 3 yr)',
  '5y': '(last 5 yr)',
  all: '(2007–present)',
};

export default function Overview() {
  const [window] = useWindow();
  const state = useData<OverviewData>(dataUrl('overview', window));

  if (state.status === 'loading') return <Loading />;
  if (state.status === 'error') return <ErrorMsg error={state.error} />;

  const { kpis, top_zips, bottom_zips } = state.data;

  return (
    <>
      <h1>
        Austin overview <span className="muted">{WINDOW_LABELS[window]}</span>
      </h1>

      <div id="kpis" className="kpi-row">
        <div className="kpi">
          <div className="label">Establishments</div>
          <div className="value">{fmtNumber(kpis.establishments)}</div>
        </div>
        <div className="kpi">
          <div className="label">Avg Inspection Score</div>
          <div className="value">{kpis.avg_score ?? '—'}</div>
        </div>
        <div className="kpi">
          <div className="label">Total Reported Revenue</div>
          <div className="value">{fmtMoney(kpis.total_receipts)}</div>
        </div>
        <div className="kpi">
          <div className="label">Inspections Recorded</div>
          <div className="value">{fmtNumber(kpis.inspections)}</div>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h2>Top ZIPs by revenue</h2>
          {top_zips.length === 0 ? (
            <Empty label="No revenue data for this selection." />
          ) : (
            <div style={{ height: 280 }}>
              <ZipBarChart
                label="Revenue"
                rows={top_zips.map((z) => ({ zip: z.zip, value: Number(z.receipts) }))}
              />
            </div>
          )}
        </div>
        <div className="card">
          <h2>Bottom ZIPs by avg score</h2>
          {bottom_zips.length === 0 ? (
            <Empty label="No inspection data for this selection." />
          ) : (
            <div style={{ height: 280 }}>
              <ZipBarChart
                label="Avg Score"
                horizontal
                rows={bottom_zips.map((z) => ({ zip: z.zip, value: Number(z.avg_score) }))}
              />
            </div>
          )}
        </div>
      </div>
    </>
  );
}
