// Inspections page — score distribution (Doughnut) + sortable
// repeat-low-score-establishments table.
import { useNavigate } from 'react-router-dom';
import { Doughnut } from 'react-chartjs-2';
import type { InspectionsData, RepeatOffender } from '../types';
import { useData, dataUrl } from '../lib/data';
import { useWindow } from '../lib/window';
import { Empty, ErrorMsg, Loading } from '../components/Loading';
import { PALETTE } from '../lib/charts-setup';
import { useSortable } from '../lib/sort';
import { fmtScore, fmtNumber } from '../lib/format';

type RepeatCol =
  | 'name'
  | 'zip'
  | 'inspections'
  | 'low'
  | 'avg'
  | 'min';

const accessor = (r: RepeatOffender, col: RepeatCol) => {
  switch (col) {
    case 'name':
      return r.canonical_name;
    case 'zip':
      return r.zip ?? '';
    case 'inspections':
      return r.inspection_count;
    case 'low':
      return r.low_score_count;
    case 'avg':
      return r.avg_score;
    case 'min':
      return r.min_score;
  }
};

export default function Inspections() {
  const [window] = useWindow();
  const state = useData<InspectionsData>(dataUrl('inspections', window));
  const navigate = useNavigate();

  const rows = state.status === 'ready' ? state.data.repeat_offenders : [];
  const { sorted, sort, toggle } = useSortable<RepeatOffender, RepeatCol>(
    rows,
    { col: 'low', dir: 'desc' },
    accessor
  );

  if (state.status === 'loading') return <Loading />;
  if (state.status === 'error') return <ErrorMsg error={state.error} />;
  const d = state.data;

  const sortClass = (col: RepeatCol) =>
    sort.col === col ? `sortable ${sort.dir}` : 'sortable';

  return (
    <>
      <h1>Inspections</h1>
      <div className="card">
        <h2>Score distribution</h2>
        {!d.distribution.length ? (
          <Empty label="No score data." />
        ) : (
          <div style={{ height: 320, maxWidth: 520, margin: '0 auto' }}>
            <Doughnut
              data={{
                labels: d.distribution.map((r) => r.score_bucket),
                datasets: [
                  { data: d.distribution.map((r) => r.inspections), backgroundColor: PALETTE },
                ],
              }}
              options={{
                maintainAspectRatio: false,
                plugins: { legend: { position: 'right' } },
              }}
            />
          </div>
        )}
      </div>
      <div className="card">
        <h2>Repeat low-score establishments</h2>
          {!sorted.length ? (
            <Empty label="No repeat low-score establishments." />
          ) : (
            <table id="repeatOffenders" className="hover-table">
              <thead>
                <tr>
                  <th className={sortClass('name')} onClick={() => toggle('name')}>Name</th>
                  <th className={sortClass('zip')} onClick={() => toggle('zip')}>ZIP</th>
                  <th className={sortClass('inspections')} onClick={() => toggle('inspections')}>Inspections</th>
                  <th className={sortClass('low')} onClick={() => toggle('low')}>Low (&lt;85)</th>
                  <th className={sortClass('avg')} onClick={() => toggle('avg')}>Avg</th>
                  <th className={sortClass('min')} onClick={() => toggle('min')}>Min</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((r) => (
                  <tr
                    key={r.establishment_id}
                    onClick={() => navigate(`/establishment/${r.establishment_id}`)}
                  >
                    <td>{r.canonical_name}</td>
                    <td>{r.zip || ''}</td>
                    <td>{fmtNumber(r.inspection_count)}</td>
                    <td>{fmtNumber(r.low_score_count)}</td>
                    <td>{fmtScore(r.avg_score)}</td>
                    <td>{fmtScore(r.min_score)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
      </div>
    </>
  );
}
