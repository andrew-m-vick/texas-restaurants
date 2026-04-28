// Revenue page — monthly time series (Line) + revenue-by-zip (Bar).
import { useMemo } from 'react';
import { Line } from 'react-chartjs-2';
import type { RevenueData } from '../types';
import { useData, dataUrl } from '../lib/data';
import { useWindow } from '../lib/window';
import { Empty, ErrorMsg, Loading } from '../components/Loading';
import ZipBarChart from '../components/ZipBarChart';
import { PALETTE } from '../lib/charts-setup';

export default function Revenue() {
  const [window] = useWindow();
  const state = useData<RevenueData>(dataUrl('revenue', window));

  // Hooks must run on every render; compute monthly totals from
  // whatever data we have (or [] while loading).
  const data = state.status === 'ready' ? state.data : null;
  const monthlySeries = useMemo(() => {
    if (!data) return { labels: [] as string[], values: [] as number[] };
    const byMonth = new Map<string, number>();
    for (const r of data.monthly) {
      byMonth.set(r.month, (byMonth.get(r.month) || 0) + Number(r.total));
    }
    const labels = [...byMonth.keys()].sort();
    return { labels, values: labels.map((m) => byMonth.get(m) || 0) };
  }, [data]);

  if (state.status === 'loading') return <Loading />;
  if (state.status === 'error') return <ErrorMsg error={state.error} />;
  const d = state.data;

  return (
    <>
      <h1>Revenue</h1>
      <div className="card">
        <h2>Monthly receipts</h2>
        {!d.monthly.length ? (
          <Empty label="No monthly revenue data." />
        ) : (
          <div style={{ height: 320 }}>
            <Line
              data={{
                labels: monthlySeries.labels,
                datasets: [
                  {
                    label: 'Revenue',
                    data: monthlySeries.values,
                    borderColor: PALETTE[0],
                    backgroundColor: PALETTE[0] + '33',
                    fill: true,
                    tension: 0.2,
                    pointRadius: 0,
                  },
                ],
              }}
              options={{
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { x: { type: 'category' } },
              }}
            />
          </div>
        )}
      </div>

      <div className="card">
        <h2>Top ZIPs by revenue</h2>
        {!d.by_zip.length ? (
          <Empty label="No ZIP revenue data." />
        ) : (
          <div style={{ height: 360 }}>
            <ZipBarChart
              label="Revenue"
              rows={d.by_zip.map((r) => ({ zip: r.zip, value: Number(r.total) }))}
            />
          </div>
        )}
      </div>
    </>
  );
}
