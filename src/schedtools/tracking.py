import os
import sqlite3
import warnings
from typing import List, Literal, Optional, Union

from schedtools.schemas import JobSpec, Queue

default_db_path = os.path.join(".schedtools", "jobs.db")

schedtools_db_key = "SCHEDTOOLS_TRACKING_DB"


def set_job_tracking_db_path(path: str):
    os.environ[schedtools_db_key] = path


def get_job_tracking_db_path(path: Optional[str] = None) -> str:
    if path is None:
        path = os.environ.get(
            schedtools_db_key, os.path.join(os.path.expanduser("~"), default_db_path)
        )
    return path


OnConflict = Literal["update", "skip", "throw"]


class JobTrackingQueue(Queue):
    on_conflict: OnConflict

    def __init__(
        self,
        *,
        db_path: Optional[str] = None,
        jobs: Union[List[JobSpec], None] = None,
        on_conflict: OnConflict = "update",
    ):
        self.db_path = get_job_tracking_db_path(db_path)
        if not os.path.exists(os.path.dirname(self.db_path)):
            os.makedirs(os.path.dirname(self.db_path))
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.ensure_table()
        self.jobs = {}
        self.pull()
        self.on_conflict = on_conflict
        if jobs is not None and len(jobs) > 0:
            for job in jobs:
                self.register(job)

    def ensure_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                queue TEXT,
                project TEXT,
                jobscript_path TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def get_on_conflict(self, on_conflict: Optional[OnConflict] = None) -> OnConflict:
        if on_conflict is None:
            return self.on_conflict
        if on_conflict not in ["update", "skip", "throw"]:
            raise ValueError(f"Invalid on_conflict: {on_conflict}")
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
        job_dicts = []
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
            self.jobs[job.id] = job
            job_dicts.append(job.to_sqlite())

        if on_conflict == "update":
            conflict_clause = "ON CONFLICT (id) DO UPDATE SET " + ", ".join(
                [
                    f"{key} = EXCLUDED.{key}"
                    for key in ["state", "queue", "project", "jobscript_path"]
                ]
            )
        elif on_conflict == "skip":
            conflict_clause = "ON CONFLICT (id) DO NOTHING"
        else:
            # SQLite will throw
            conflict_clause = ""

        self.conn.executemany(
            """
            INSERT INTO jobs (id, state, queue, project, jobscript_path)
            VALUES (:id, :state, :queue, :project, :jobscript_path)
            """
            + conflict_clause,
            job_dicts,
        )
        self.conn.commit()

    def pull(self):
        cursor = self.conn.execute("SELECT * FROM jobs")
        for row in cursor:
            self.jobs[row["id"]] = JobSpec.from_sqlite(row)

    def pop(self, job_id: str) -> JobSpec:
        job = super().pop(job_id)
        self.conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        self.conn.commit()
        return job
