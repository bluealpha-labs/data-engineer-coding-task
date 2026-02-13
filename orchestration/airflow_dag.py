"""
Airflow DAG: ingest → validate → load_warehouse.
Uses staging files to pass data between tasks (Airflow tasks run in separate processes).
Point Airflow's DAGS_FOLDER at this repo's orchestration/ directory so this file is loaded.
"""
from pathlib import Path
import sys

# Project root (parent of orchestration/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import os
import pickle
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _staging_dir(run_id: str) -> Path:
    p = _PROJECT_ROOT / "output" / "airflow_staging" / run_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def task_ingest(**context):
    """Load Google, Facebook, CRM; write DataFrames to staging; push paths to XCom."""
    from ingestion.loaders import load_google_ads, load_facebook, load_crm

    run_id = context["run_id"]
    stage = _staging_dir(run_id) / "ingest"
    stage.mkdir(parents=True, exist_ok=True)

    df_google = load_google_ads()
    df_facebook = load_facebook()
    df_crm = load_crm()

    for name, df in [("google", df_google), ("facebook", df_facebook), ("crm", df_crm)]:
        path = stage / f"{name}.pkl"
        with open(path, "wb") as f:
            pickle.dump(df, f)

    return {"stage": str(stage), "run_id": run_id}


def task_validate(**context):
    """Read staged DataFrames, validate, write report and validated DataFrames; push paths to XCom."""
    from validation.report import ValidationReport
    from validation.validators import validate_google_ads, validate_facebook, validate_crm

    ti = context["ti"]
    ingest_out = ti.xcom_pull(task_ids="ingest")
    stage_ingest = Path(ingest_out["stage"])
    run_id = ingest_out["run_id"]

    with open(stage_ingest / "google.pkl", "rb") as f:
        df_google = pickle.load(f)
    with open(stage_ingest / "facebook.pkl", "rb") as f:
        df_facebook = pickle.load(f)
    with open(stage_ingest / "crm.pkl", "rb") as f:
        df_crm = pickle.load(f)

    report = ValidationReport()
    df_google, report = validate_google_ads(df_google, report)
    df_facebook, report = validate_facebook(df_facebook, report)
    df_crm, report = validate_crm(df_crm, report)

    report_path = _PROJECT_ROOT / "output" / "validation_report.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report.save_csv(report_path)

    stage_validate = _staging_dir(run_id) / "validate"
    stage_validate.mkdir(parents=True, exist_ok=True)
    for name, df in [("google", df_google), ("facebook", df_facebook), ("crm", df_crm)]:
        with open(stage_validate / f"{name}.pkl", "wb") as f:
            pickle.dump(df, f)

    return {"stage": str(stage_validate), "run_id": run_id}


def task_load_warehouse(**context):
    """Read validated DataFrames from staging and load into warehouse."""
    from transformation.schema import get_engine
    from transformation.load import load_from_validated

    ti = context["ti"]
    run_id = context["run_id"]
    validate_out = ti.xcom_pull(task_ids="validate")
    # Use path from XCom, or reconstruct from run_id (in case XCom serialization differs)
    if validate_out and isinstance(validate_out, dict) and validate_out.get("stage"):
        stage_validate = Path(validate_out["stage"])
    else:
        stage_validate = _PROJECT_ROOT / "output" / "airflow_staging" / run_id / "validate"
    if not stage_validate.exists():
        raise FileNotFoundError(f"Staging dir not found: {stage_validate} (run_id={run_id}). Check that validate task wrote pickles there.")

    with open(stage_validate / "google.pkl", "rb") as f:
        df_google = pickle.load(f)
    with open(stage_validate / "facebook.pkl", "rb") as f:
        df_facebook = pickle.load(f)
    with open(stage_validate / "crm.pkl", "rb") as f:
        df_crm = pickle.load(f)

    # Force warehouse path to project output/ (Airflow task may run with different cwd)
    db_path = _PROJECT_ROOT / "output" / "warehouse.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["WAREHOUSE_URL"] = f"sqlite:///{db_path}"
    engine = get_engine()
    load_from_validated(engine, df_google, df_facebook, df_crm)


with DAG(
    dag_id="bluealpha_pipeline",
    description="Ingest → validate → load_warehouse (same as run_pipeline.py)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bluealpha", "pipeline"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
        retries=int(os.environ.get("PIPELINE_MAX_RETRIES", "3")),
        retry_delay=timedelta(seconds=2),
    )
    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
        retries=int(os.environ.get("PIPELINE_MAX_RETRIES", "3")),
        retry_delay=timedelta(seconds=2),
    )
    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=task_load_warehouse,
        retries=int(os.environ.get("PIPELINE_MAX_RETRIES", "3")),
        retry_delay=timedelta(seconds=2),
    )

    ingest >> validate >> load_warehouse
