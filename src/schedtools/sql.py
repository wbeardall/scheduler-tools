import os
import sqlite3
import warnings
from enum import Enum
from typing import List, Literal, Optional, Union

from schedtools.consts import job_id_key
from schedtools.schemas import JobSpec, JobState

default_db_path = os.path.join(".tracking", "jobs.db")

job_tracking_db_key = "JOB_TRACKING_DB"


def set_job_tracking_db_path(path: str):
    os.environ[job_tracking_db_key] = path


def get_job_tracking_db_path(path: Optional[str] = None) -> str:
    if path is None:
        path = os.environ.get(
            job_tracking_db_key, os.path.join(os.path.expanduser("~"), default_db_path)
        )
    return path


class JobTrackingConnection:
    conn: Union[sqlite3.Connection, None] = None

    def get(self) -> sqlite3.Connection:
        if self.conn is None:
            path = get_job_tracking_db_path()
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            self.conn = sqlite3.connect(path)
            ensure_table(self.conn)
            self.conn.row_factory = sqlite3.Row
        return self.conn


default_tracking_connection = JobTrackingConnection()


def update_job_state(
    *,
    state: Union[JobState, str],
    comment: Optional[str] = None,
    job_id: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
    on_fail: Literal["raise", "warn", "ignore"] = "raise",
):
    variables = [state]
    to_set = [
        "state = ?",
    ]
    if comment is not None:
        variables.append(comment)
        to_set.append("comment = ?")

    try:
        if conn is None:
            conn = default_tracking_connection.get()
        if job_id is None:
            job_id = os.environ.get(job_id_key)
        if job_id is None:
            raise RuntimeError(
                f"job_id not provided, and the {job_id_key} environment variable is not set."
            )
        if isinstance(state, Enum):
            state = state.value
        conn.execute(
            f"UPDATE jobs SET {', '.join(to_set)}, modified_time = CURRENT_TIMESTAMP WHERE id = ?",
            (*[el.value if isinstance(el, Enum) else el for el in variables], job_id),
        )
        conn.commit()
    except Exception as e:
        if on_fail == "raise":
            raise RuntimeError("Failed to update job state") from e
        elif on_fail == "warn":
            warnings.warn(f"Failed to update job state: {e}")


OnConflict = Literal["update", "skip", "throw"]


def validate_on_conflict(on_conflict: OnConflict) -> None:
    if on_conflict not in ["update", "skip", "throw"]:
        raise ValueError(f"Invalid on_conflict: {on_conflict}")


def ensure_table(conn: sqlite3.Connection):
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                cluster TEXT NOT NULL,
                queue TEXT,
                project TEXT,
                jobscript_path TEXT NOT NULL,
                experiment_path TEXT NOT NULL,
                comment TEXT,
                modified_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
    )
    conn.commit()


def upsert_jobs(
    conn: sqlite3.Connection, jobs: List[JobSpec], on_conflict: OnConflict = "update"
):
    """
    Upsert jobs into the database.

    Args:
        conn: The database connection.
        jobs: The jobs to upsert.
        on_conflict: The conflict resolution strategy.
    """
    if not isinstance(jobs, list):
        jobs = [jobs]
    validate_on_conflict(on_conflict)
    if on_conflict == "update":
        conflict_clause = "ON CONFLICT (id) DO UPDATE SET " + ", ".join(
            [
                f"{key} = EXCLUDED.{key}"
                for key in [
                    "state",
                    "cluster",
                    "queue",
                    "project",
                    "jobscript_path",
                    "experiment_path",
                    "modified_time",
                ]
            ]
        )
    elif on_conflict == "skip":
        conflict_clause = "ON CONFLICT (id) DO NOTHING"
    else:
        # SQLite will throw
        conflict_clause = ""

    job_dicts = [job.to_sqlite() for job in jobs]
    conn.executemany(
        """
        INSERT INTO jobs (id, state, cluster, queue, project, jobscript_path, experiment_path, comment, modified_time)
        VALUES (:id, :state, :cluster, :queue, :project, :jobscript_path, :experiment_path, :comment, :modified_time)
        """
        + conflict_clause,
        job_dicts,
    )
    conn.commit()
