"""DAG integrity test. Needs Airflow, so it is skipped in the host virtualenv.

It runs wherever Airflow is installed (e.g. CI). Locally, the same check is:
    docker compose exec airflow-scheduler airflow dags list-import-errors
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parents[1] / "dag"


def test_dag_imports_and_has_expected_tasks():
    from airflow.dag_processing.dagbag import DagBag

    dag_bag = DagBag(dag_folder=str(DAGS_DIR))

    assert dag_bag.import_errors == {}
    dag = dag_bag.get_dag("nyc_taxi_etl")
    assert dag is not None
    assert {
        "resolve_month",
        "create_schema",
        "extract",
        "clean",
        "load_staging",
        "load_dimensions",
        "load_fact",
        "run_quality_checks",
        "summarise_run",
    } == set(dag.task_ids)
