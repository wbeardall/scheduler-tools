import os
import tempfile

import pytest

from schedtools.schemas import JobSpec
from schedtools.tracking import JobTrackingQueue


@pytest.fixture
def db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield os.path.join(temp_dir, "test.db")


def test_job_tracking_queue(db_path):
    queue = JobTrackingQueue(db_path=db_path)
    job = JobSpec.from_unsubmitted(jobscript_path="test")
    queue.register(job)
    assert job.id in queue

    new_queue = JobTrackingQueue(db_path=db_path)
    assert job.id in new_queue
    assert new_queue.get(job.id) == job

    queue.pop(job.id)
    assert job.id not in queue

    new_queue = JobTrackingQueue(db_path=db_path)
    assert job.id not in new_queue
