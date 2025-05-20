import os
import sqlite3
import tempfile

import pytest

from schedtools.schemas import JobSpec, JobState
from schedtools.tracking import (
    JobTrackingQueue,
    ensure_table,
    update_job_state,
    upsert_jobs,
)


@pytest.fixture
def db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield os.path.join(temp_dir, "test.db")


@pytest.fixture
def db_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_table(conn)
    yield conn
    conn.close()


@pytest.mark.parametrize("on_conflict", ["throw", "skip", "update"])
def test_job_tracking_insert(db_conn, on_conflict):
    jobs = [
        JobSpec.from_unsubmitted(
            jobscript_path=f"test-{i}", experiment_path=f"/tmp/test-output-{i}"
        )
        for i in range(3)
    ]

    upsert_jobs(db_conn, jobs, on_conflict=on_conflict)


@pytest.mark.parametrize("on_conflict", ["throw", "skip", "update"])
def test_job_tracking_upsert(db_conn, on_conflict):
    jobs = [
        JobSpec.from_unsubmitted(
            jobscript_path=f"test-{i}", experiment_path=f"/tmp/test-output-{i}"
        )
        for i in range(2)
    ]

    upsert_jobs(db_conn, jobs, on_conflict=on_conflict)

    # If skip on conflict, we expect the original paths
    if on_conflict == "skip":
        expected_paths = [job.experiment_path for job in jobs]

    for job in jobs:
        job.experiment_path = "new-path"

    # If update on conflict, we expect the new paths
    if on_conflict == "update":
        expected_paths = [job.experiment_path for job in jobs]

    if on_conflict == "throw":
        with pytest.raises(sqlite3.IntegrityError):
            upsert_jobs(db_conn, jobs, on_conflict=on_conflict)
    else:
        upsert_jobs(db_conn, jobs, on_conflict=on_conflict)

        paths = [
            row["experiment_path"]
            for row in db_conn.execute("SELECT experiment_path FROM jobs").fetchall()
        ]
        assert paths == expected_paths


def test_job_tracking_update(db_conn):
    job = JobSpec.from_unsubmitted(
        jobscript_path="test", experiment_path="/tmp/test-output"
    )
    upsert_jobs(db_conn, [job])
    update_job_state(conn=db_conn, job_id=job.id, state=JobState.COMPLETED)

    rows = db_conn.execute("SELECT * FROM jobs").fetchall()
    assert len(rows) == 1
    assert rows[0]["state"] == JobState.COMPLETED.value


def test_job_tracking_queue(db_path):
    queue = JobTrackingQueue(db=db_path)
    job = JobSpec.from_unsubmitted(
        jobscript_path="test", experiment_path="/tmp/test-output"
    )
    queue.register(job)
    assert job.id in queue

    new_queue = JobTrackingQueue(db=db_path)
    assert job.id in new_queue
    assert new_queue.get(job.id) == job

    queue.pop(job.id)
    assert job.id not in queue

    new_queue = JobTrackingQueue(db=db_path)
    assert job.id not in new_queue
