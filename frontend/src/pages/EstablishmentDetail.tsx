// Per-establishment detail: header + breadcrumbs, inspection-history line,
// stacked-bar revenue, TABC license table with derived tenure.
import { useMemo } from 'react';
import { useParams } from 'react-router-dom';
import { Bar, Line } from 'react-chartjs-2';
import type { EstablishmentDetail } from '../types';
import { useData } from '../lib/data';
import { Empty, Loading } from '../components/Loading';

export default function EstablishmentDetailPage() {
  const { id } = useParams<{ id: string }>();
  const state = useData<EstablishmentDetail>(
    `/static/data/establishment/${id}.json`
  );

  // Derive tenure ahead of the early returns so hooks order stays stable.
  const tenure = useMemo(() => {
    if (state.status !== 'ready' || !state.data.licenses) return null;
    const earliest = state.data.licenses
      .map((l) => l.original_issue_date)
      .filter((d): d is string => !!d)
      .sort()[0];
    if (!earliest) return null;
    const years =
      (Date.now() - new Date(earliest).getTime()) / (365.25 * 24 * 3600 * 1000);
    return { earliest, years };
  }, [state]);

  if (state.status === 'loading') return <Loading />;
  // The export step writes one file per establishment, so a fetch failure
  // here means the ID isn't in the warehouse — render "not found" instead
  // of a generic error.
  if (state.status === 'error') {
    return (
      <div className="card">
        <h1>Not found.</h1>
      </div>
    );
  }

  const data = state.data;
  const h = data.header;

  return (
    <>
      <div className="breadcrumbs">
        <a href="/app/establishments">← Browse</a>
        {h.zip ? (
          <>
            {' · '}
            <a href={`/app/establishments?zip=${h.zip}`}>{h.zip}</a>
          </>
        ) : null}
      </div>
      <h1>{h.canonical_name}</h1>
      <p className="muted">
        {h.canonical_address || ''} · {h.city} · {h.zip || '—'} · match: {h.match_method}
        {h.match_score ? ` (score ${h.match_score})` : ''}
      </p>

      <div className="grid-2">
        <div className="card">
          <h2>Inspection history</h2>
          {!data.inspections.length ? (
            <Empty label="No inspections recorded." />
          ) : (
            <div style={{ height: 280 }}>
              <Line
                data={{
                  labels: data.inspections.map((r) => r.inspection_date),
                  datasets: [
                    {
                      label: 'Score',
                      data: data.inspections.map((r) => r.score),
                      borderColor: '#4cc9f0',
                      backgroundColor: '#4cc9f033',
                      fill: true,
                      tension: 0.1,
                      spanGaps: true,
                    },
                  ],
                }}
                options={{
                  maintainAspectRatio: false,
                  scales: { y: { suggestedMin: 60, suggestedMax: 100 } },
                  plugins: { legend: { display: false } },
                }}
              />
            </div>
          )}
        </div>
        <div className="card">
          <h2>Monthly receipts</h2>
          {!data.revenue.length ? (
            <Empty label="No mixed-beverage receipts." />
          ) : (
            <div style={{ height: 280 }}>
              <Bar
                data={{
                  labels: data.revenue.map((r) => r.month),
                  datasets: [
                    {
                      label: 'Liquor',
                      data: data.revenue.map((r) => r.liquor_receipts),
                      backgroundColor: '#f72585',
                    },
                    {
                      label: 'Wine',
                      data: data.revenue.map((r) => r.wine_receipts),
                      backgroundColor: '#7209b7',
                    },
                    {
                      label: 'Beer',
                      data: data.revenue.map((r) => r.beer_receipts),
                      backgroundColor: '#4cc9f0',
                    },
                  ],
                }}
                options={{
                  maintainAspectRatio: false,
                  scales: { x: { stacked: true }, y: { stacked: true } },
                }}
              />
            </div>
          )}
        </div>
      </div>

      {data.licenses && data.licenses.length > 0 && (
        <div className="card">
          <h2>TABC licenses</h2>
          <table>
            <thead>
              <tr>
                <th>License</th>
                <th>Type</th>
                <th>Status</th>
                <th>Original issue</th>
                <th>Current issued</th>
                <th>Expiration</th>
                <th>Gun sign</th>
              </tr>
            </thead>
            <tbody>
              {data.licenses.map((l) => (
                <tr key={l.license_id}>
                  <td>{l.license_id}</td>
                  <td>{l.license_type || ''}</td>
                  <td>{l.primary_status || ''}</td>
                  <td>{l.original_issue_date || ''}</td>
                  <td>{l.current_issued_date || ''}</td>
                  <td>{l.expiration_date || ''}</td>
                  <td>{l.gun_sign || ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {tenure && (
            <p className="muted" style={{ marginTop: '0.75rem' }}>
              Earliest permit: {tenure.earliest} · tenure: {tenure.years.toFixed(1)} years
            </p>
          )}
        </div>
      )}
    </>
  );
}
