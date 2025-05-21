import os
import sqlite3
import tempfile
import warnings
from contextlib import contextmanager
from typing import List, Optional, Union

from paramiko import SSHClient

from schedtools.schemas import JobSpec, Queue
from schedtools.shell_handler import CommandHandler, LocalHandler, ShellHandler
from schedtools.sql import (
    OnConflict,
    default_db_path,
    ensure_table,
    get_job_tracking_db_path,
    upsert_jobs,
    validate_on_conflict,
)


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

    @classmethod
    def from_handler(cls, handler: CommandHandler) -> "JobTrackingQueue":
        with job_tracking_queue(handler, write_back=False) as queue:
            return queue


@contextmanager
def job_tracking_queue(
    handler: Union[CommandHandler, str, SSHClient], *, write_back: bool = False
):
    if not isinstance(handler, CommandHandler):
        handler = ShellHandler(handler)

    if isinstance(handler, LocalHandler):
        yield JobTrackingQueue(db=default_db_path)

    else:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Pull the tracked database from the cluster
            temp_path = os.path.join(temp_dir, "db")
            with handler.open_file(default_db_path, "rb") as f:
                with open(temp_path, "wb") as wf:
                    wf.write(f.read())

            yield JobTrackingQueue(db=temp_path)

            if write_back:
                with handler.open_file(default_db_path, "wb") as f:
                    f.write(open(temp_path, "rb").read())
