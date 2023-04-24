import uuid
import random

from schedtools.core import PBSJob, Queue, DEFAULT_PRIORITY, UNSUBMITTED_PRIORITY

def test_unsubmitted():
    job = PBSJob.unsubmitted("a_dummy_location.pbs")
    assert job.status == "unsubmitted"
    assert job.priority == UNSUBMITTED_PRIORITY

def test_queue_iteration():
    max_priority=3
    jobs = []
    for _ in range(10):
        id = str(uuid.uuid1())
        jobs.append(PBSJob(id=id, Job_Name=id,priority=random.randint(0,max_priority)))
    queue = Queue(jobs)
    last_priority = max_priority
    for job in queue:
        assert job.priority <= last_priority
        last_priority = job.priority

def test_queue_priority_inference():
    jobs = []
    for state in [None, "Q"]:
        id = str(uuid.uuid1())
        job = dict(id=id, Job_Name=id)
        if state is not None:
            job["job_state"] = state
        job = PBSJob(**job)
        if state is not None:
            assert job.priority == DEFAULT_PRIORITY
        else:
            assert job.priority == UNSUBMITTED_PRIORITY
        jobs.append(job)

    queue = Queue(jobs)
    last_priority = DEFAULT_PRIORITY
    for job in queue:
        assert job.priority <= last_priority
        last_priority = job.priority