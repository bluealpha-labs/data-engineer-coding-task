"""
Orchestration DAG: ingest → validate → load_warehouse.
- Dependencies: validate after ingest, load_warehouse after validate.
- Retry: each task retried on failure (configurable attempts + exponential backoff).
- Idempotent: safe to re-run (full refresh of report and warehouse; no incremental state).
"""
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional, Tuple

# Project root
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

# Config (override via env if needed)
MAX_RETRIES = int(os.environ.get("PIPELINE_MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE = float(os.environ.get("PIPELINE_RETRY_BACKOFF", "2"))


def _retry(fn: Callable[[], Any], task_name: str) -> Any:
    """Run fn(); on failure, retry with exponential backoff. Raise last exception after all retries."""
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"  [{task_name}] attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. Retrying in {wait:.1f}s...")
                time.sleep(wait)
    raise last_exc


def _task_ingest() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Task: load all three sources. No dependencies."""
    from ingestion.loaders import load_google_ads, load_facebook, load_crm
    df_google = load_google_ads()
    df_facebook = load_facebook()
    df_crm = load_crm()
    return df_google, df_facebook, df_crm


def _task_validate(
    df_google: pd.DataFrame,
    df_facebook: pd.DataFrame,
    df_crm: pd.DataFrame,
    report_path: Path,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, int]:
    """Task: validate (no drop), write report. Depends on ingest outputs. Returns (dfs, report_len)."""
    from validation.report import ValidationReport
    from validation.validators import validate_google_ads, validate_facebook, validate_crm
    report = ValidationReport()
    df_google, report = validate_google_ads(df_google, report)
    df_facebook, report = validate_facebook(df_facebook, report)
    df_crm, report = validate_crm(df_crm, report)
    report.save_csv(report_path)
    return df_google, df_facebook, df_crm, len(report)


def _task_load_warehouse(
    df_google: pd.DataFrame,
    df_facebook: pd.DataFrame,
    df_crm: pd.DataFrame,
) -> None:
    """Task: load to warehouse (full refresh). Depends on validate outputs."""
    from transformation.schema import get_engine
    from transformation.load import load_from_validated
    engine = get_engine()
    load_from_validated(engine, df_google, df_facebook, df_crm)


def run_dag(
    report_path: Optional[Path] = None,
    warehouse_url: Optional[str] = None,
) -> None:
    """
    Run the pipeline DAG: ingest → validate → load_warehouse.
    Each step is retried on failure. Re-running is idempotent (overwrites report and warehouse).
    """
    report_path = report_path or _ROOT / "output" / "validation_report.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if warehouse_url:
        os.environ["WAREHOUSE_URL"] = warehouse_url

    print("DAG: ingest → validate → load_warehouse (idempotent, retries on failure)")
    print("-" * 56)

    # --- Ingest ---
    print("Task: ingest")
    df_google, df_facebook, df_crm = _retry(_task_ingest, "ingest")
    print(f"  Google: {len(df_google)}, Facebook: {len(df_facebook)}, CRM: {len(df_crm)} rows.")

    # --- Validate ---
    print("Task: validate")
    def _validate():
        return _task_validate(df_google, df_facebook, df_crm, report_path)
    df_google, df_facebook, df_crm, report_len = _retry(_validate, "validate")
    print(f"  Issues recorded: {report_len}. Report: {report_path}")

    # --- Load warehouse ---
    print("Task: load_warehouse")
    def _load():
        _task_load_warehouse(df_google, df_facebook, df_crm)
    _retry(_load, "load_warehouse")
    from transformation.schema import get_engine
    engine = get_engine()
    print(f"  Loaded: dim_campaign, fact_ad_performance, fact_orders -> {engine.url}")

    print("-" * 56)
    print("DAG run finished (idempotent: safe to re-run).")