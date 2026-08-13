"""github_archive_daily — Daily GitHub Archive ingestion DAG.

The DAG checks whether the GitHub Archive file is available, downloads it,
validates it, loads it into DuckDB, and reports the result.

Specification: ../SPEC.md → "DAG".
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from gh_sensor import GHArchiveSensor
from include.gh_etl import download, load_to_duckdb, summarize, validate


# Paths inside the Airflow containers.
DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"


def download_archive(ds: str, **context) -> None:
    """Download the archive and store its path in XCom."""

    path = download(ds, LANDING_DIR)

    # Push the downloaded file path to XCom for downstream tasks.
    context["ti"].xcom_push(
        key="file_path",
        value=path,
    )


def validate_file(**context) -> None:
    """Validate the downloaded archive."""

    # Pull the file path from the download_archive task.
    path = context["ti"].xcom_pull(
        task_ids="download_archive",
        key="file_path",
    )

    validate(path)


def load_data(**context) -> None:
    """Load the validated archive into DuckDB."""

    # Get the logical date from Airflow.
    ds = context["ds"]

    # Pull the file path from the download_archive task.
    path = context["ti"].xcom_pull(
        task_ids="download_archive",
        key="file_path",
    )

    rows = load_to_duckdb(
        path,
        ds,
        DB_PATH,
    )

    # Store the number of loaded rows in XCom.
    context["ti"].xcom_push(
        key="rows",
        value=rows,
    )


def notify_completion(**context) -> None:
    """Print a summary after successful loading."""

    # Use Airflow's logical date, not the current system date.
    ds = context["ds"]

    summary = summarize(
        ds,
        DB_PATH,
    )

    print(
        f"GitHub Archive ingestion completed for {ds}: "
        f"{summary['rows']} rows, "
        f"{summary['event_types']} event types."
    )


with DAG(
    dag_id="github_archive_daily",
    description="Daily GitHub Archive ingestion into DuckDB",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["github", "archive", "duckdb"],
) as dag:

    # Check that the GitHub Archive file is available.
    check_availability = GHArchiveSensor(
        task_id="check_availability",
        hour=14,
        timeout=600,
        poke_interval=60,
        mode="reschedule",
    )

    # Download the archive for the logical date.
    download_task = PythonOperator(
        task_id="download_archive",
        python_callable=download_archive,
    )

    # Validate the downloaded file.
    validate_task = PythonOperator(
        task_id="validate_file",
        python_callable=validate_file,
    )

    # Load the validated data into DuckDB.
    load_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_data,
    )

    # Print the final summary.
    notify_task = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
    )

    # Define the required DAG dependency graph.
    (
        check_availability
        >> download_task
        >> validate_task
        >> load_task
        >> notify_task
    )