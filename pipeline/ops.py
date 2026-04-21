from contextlib import contextmanager
from sqlalchemy import text
from .db import engine


@contextmanager
def track_run(dag_id: str, layer: str):
    """Log start/finish of a pipeline step in ops.pipeline_runs."""
    with engine.begin() as conn:
        run_id = conn.execute(
            text(
                "INSERT INTO ops.pipeline_runs (dag_id, layer) "
                "VALUES (:d, :l) RETURNING id"
            ),
            {"d": dag_id, "l": layer},
        ).scalar_one()

    state = {"rows": 0, "notes": None}
    try:
        yield state
        status = "success"
    except Exception as e:
        status = "failed"
        state["notes"] = str(e)[:500]
        raise
    finally:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE ops.pipeline_runs "
                    "SET finished_at=now(), status=:s, rows_written=:r, notes=:n "
                    "WHERE id=:id"
                ),
                {
                    "s": status,
                    "r": state["rows"],
                    "n": state["notes"],
                    "id": run_id,
                },
            )
