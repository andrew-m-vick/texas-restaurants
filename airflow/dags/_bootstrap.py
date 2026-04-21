"""Make the repo root importable from DAG modules so `from pipeline.* import ...`
works regardless of where Airflow's DAG folder is configured."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
