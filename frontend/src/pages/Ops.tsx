// Pipeline tab — sortable runs + table-row counts.
import type { OpsCount, OpsData, PipelineRun } from '../types';
import { useData } from '../lib/data';
import { Empty, ErrorMsg, Loading } from '../components/Loading';
import { useSortable } from '../lib/sort';
import { fmtNumber } from '../lib/format';

type RunCol = 'dag_id' | 'layer' | 'started_at' | 'status' | 'rows_written';
type CountCol = 'tbl' | 'n';

const runAccessor = (r: PipelineRun, col: RunCol) => r[col];
const countAccessor = (r: OpsCount, col: CountCol) => r[col];

export default function Ops() {
  const state = useData<OpsData>('/static/data/ops.json');

  const runs: PipelineRun[] = state.status === 'ready' ? state.data.runs : [];
  const counts: OpsCount[] = state.status === 'ready' ? state.data.counts : [];

  const runSort = useSortable<PipelineRun, RunCol>(
    runs,
    { col: 'started_at', dir: 'desc' },
    runAccessor
  );
  const countSort = useSortable<OpsCount, CountCol>(
    counts,
    { col: 'tbl', dir: 'asc' },
    countAccessor
  );

  if (state.status === 'loading') return <Loading />;
  if (state.status === 'error') return <ErrorMsg error={state.error} />;

  const runClass = (col: RunCol) =>
    runSort.sort.col === col ? `sortable ${runSort.sort.dir}` : 'sortable';
  const countClass = (col: CountCol) =>
    countSort.sort.col === col ? `sortable ${countSort.sort.dir}` : 'sortable';

  return (
    <>
      <h1>Pipeline</h1>
      <div className="grid-2">
        <div className="card">
          <h2>Recent runs</h2>
          {!runSort.sorted.length ? (
            <Empty label="No pipeline runs recorded." />
          ) : (
            <table id="runsTable" className="hover-table">
              <thead>
                <tr>
                  <th className={runClass('dag_id')} onClick={() => runSort.toggle('dag_id')}>DAG</th>
                  <th className={runClass('layer')} onClick={() => runSort.toggle('layer')}>Layer</th>
                  <th className={runClass('started_at')} onClick={() => runSort.toggle('started_at')}>Started</th>
                  <th className={runClass('status')} onClick={() => runSort.toggle('status')}>Status</th>
                  <th className={`num ${runClass('rows_written')}`} onClick={() => runSort.toggle('rows_written')}>Rows</th>
                </tr>
              </thead>
              <tbody>
                {runSort.sorted.map((r, i) => (
                  <tr key={`${r.dag_id}-${r.started_at}-${i}`}>
                    <td>{r.dag_id}</td>
                    <td>{r.layer}</td>
                    <td>{r.started_at ? new Date(r.started_at).toLocaleString() : ''}</td>
                    <td className={`status-${r.status}`}>{r.status}</td>
                    <td className="num">{fmtNumber(r.rows_written)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        <div className="card">
          <h2>Row counts by table</h2>
          {!countSort.sorted.length ? (
            <Empty label="No row counts available." />
          ) : (
            <table id="countsTable" className="hover-table">
              <thead>
                <tr>
                  <th className={countClass('tbl')} onClick={() => countSort.toggle('tbl')}>Table</th>
                  <th className={`num ${countClass('n')}`} onClick={() => countSort.toggle('n')}>Rows</th>
                </tr>
              </thead>
              <tbody>
                {countSort.sorted.map((r) => (
                  <tr key={r.tbl}>
                    <td>{r.tbl}</td>
                    <td className="num">{fmtNumber(r.n)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </>
  );
}
