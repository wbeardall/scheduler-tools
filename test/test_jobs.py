import os
import tempfile

import pytest

from schedtools.managers import PBS
from schedtools.schemas import Job
from schedtools.sql import set_job_tracking_db_path

if __package__ is None or __package__ == "":
    from dummy_handler import DummyHandler
else:
    from .dummy_handler import DummyHandler


@pytest.fixture()
def fresh_tracking_db():
    with tempfile.TemporaryDirectory() as temp_dir:
        set_job_tracking_db_path(os.path.join(temp_dir, "db"))
        yield


def test_get_jobs():
    handler = DummyHandler()
    jobs = PBS.get_jobs_from_handler(handler)
    attrs = [
        {"scheduler_id": "7013474", "name": "job-01.pbs", "percent_completion": 0},
        {"scheduler_id": "7013475", "name": "job-02.pbs", "percent_completion": 12.5},
    ]
    for i, job in enumerate(jobs):
        assert isinstance(job, Job)
        for k, v in attrs[i].items():
            assert getattr(job, k) == v
        # Test attribute-style field access
        assert job.project == "_pbs_project_default"
        assert job.percent_completion == 0


# @pytest.mark.skip
# @pytest.mark.parametrize(
#     "valid",
#     [
#         # pytest.param(False,marks=pytest.mark.xfail(reason="Unrecognised batch system")),
#         True
#     ],
# )
# @pytest.mark.parametrize("jobs", [False, True])
# @pytest.mark.parametrize("tracked", [False, True])
# @pytest.mark.parametrize("rerun", [False, True])
# @pytest.mark.parametrize("memkill", [False, True])
# @pytest.mark.parametrize("wallkill", [False, True])
# @pytest.mark.parametrize("qsub", [False, True])
# def test_rerun(to_destroy, valid, jobs, tracked, rerun, memkill, wallkill, qsub):
#     os.environ["SCHEDTOOLS_PROG"] = "rerun"
#     to_destroy.append(os.path.dirname(RERUN_TRACKED_CACHE))
#     handler = DummyHandler(
#         valid=valid,
#         jobs=jobs,
#         tracked=tracked,
#         rerun=rerun,
#         memkill=memkill,
#         wallkill=wallkill,
#         qsub=qsub,
#     )
#     rerun_jobs(
#         handler=handler,
#         logger=logging.getLogger(__name__).addHandler(logging.NullHandler()),
#     )
#     cached = []  # get_tracked_cache()
#     if tracked:
#         if memkill and (not qsub) and (not rerun):
#             # if qsub, id should not be in cached
#             assert "70134" in cached
#         if wallkill and (not qsub) and (not rerun):
#             # if qsub, id should not be in cached
#             assert "70135" in cached
#     if jobs:
#         assert "7013474" in cached
#         assert "7013475" in cached
