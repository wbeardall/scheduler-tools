import os
import sqlite3
import warnings
from enum import Enum
from typing import List, Literal, Optional, Union

from schedtools.consts import job_id_key
from schedtools.schemas import JobSpec, JobState, Queue

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
    job_id: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
    on_fail: Literal["raise", "warn", "ignore"] = "raise",
):
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
            "UPDATE jobs SET state = ?, modified_time = CURRENT_TIMESTAMP WHERE id = ?",
            (state, job_id),
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
                queue TEXT,
                project TEXT,
                jobscript_path TEXT NOT NULL,
                experiment_path TEXT NOT NULL,
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
        INSERT INTO jobs (id, state, queue, project, jobscript_path, experiment_path, modified_time)
        VALUES (:id, :state, :queue, :project, :jobscript_path, :experiment_path, CURRENT_TIMESTAMP)
        """
        + conflict_clause,
        job_dicts,
    )
    conn.commit()


class JobTrackingQueue(Queue):
    on_conflict: OnConflict
    conn: sqlite3.Connection

    def __init__(
        self,
        *,
        db: Optional[Union[str, sqlite3.Connection]] = None,
        jobs: Union[List[JobSpec], None] = None,
        on_conflict: OnConflict = "update",
    ):
        validate_on_conflict(on_conflict)
        if isinstance(db, sqlite3.Connection):
            self.conn = db
        else:
            db_path = get_job_tracking_db_path(db)
            if not os.path.exists(os.path.dirname(db_path)):
                os.makedirs(os.path.dirname(db_path))
            self.conn = sqlite3.connect(db_path)

        self.conn.row_factory = sqlite3.Row
        self.ensure_table()
        self.jobs = []
        self.pull()
        self.on_conflict = on_conflict
        if jobs is not None and len(jobs) > 0:
            for job in jobs:
                self.register(job)

    def ensure_table(self):
        ensure_table(self.conn)

    def get_on_conflict(self, on_conflict: Optional[OnConflict] = None) -> OnConflict:
        if on_conflict is None:
            return self.on_conflict
        validate_on_conflict(on_conflict)
        return on_conflict

    def register(
        self,
        jobs: Union[JobSpec, List[JobSpec]],
        *,
        on_conflict: Optional[OnConflict] = None,
    ):
        on_conflict = self.get_on_conflict(on_conflict)
        if isinstance(jobs, JobSpec):
            jobs = [jobs]
        elif not isinstance(jobs, list):
            raise TypeError(f"Invalid type for jobs: {type(jobs)}")

        for job in jobs:
            if job.id in self.jobs:
                if on_conflict == "update":
                    warnings.warn(
                        f"Job {job.id} already registered. It will be overwritten."
                    )
                elif on_conflict == "throw":
                    raise KeyError(f"Job {job.id} already registered.")
                else:
                    continue
            self.jobs.append(job)

        upsert_jobs(self.conn, jobs, on_conflict)

    def pull(self):
        cursor = self.conn.execute("SELECT * FROM jobs")
        for row in cursor:
            self.jobs.append(JobSpec.from_sqlite(row))

    def pop(self, job_id: str) -> JobSpec:
        job = super().pop(job_id)
        self.conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        self.conn.commit()
        return job
