CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    rows_written BIGINT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_started
    ON ops.pipeline_runs (dag_id, started_at DESC);
