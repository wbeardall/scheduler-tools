import logging
import os
import shutil

import pytest

from schedtools.core import PBSJob
from schedtools.jobs import (
    RERUN_TRACKED_CACHE,
    RERUN_TRACKED_FILE,
    get_tracked_cache,
    get_tracked_from_cluster,
    rerun_jobs,
    track_new_jobs,
)
from schedtools.managers import PBS
from schedtools.shell_handler import LocalHandler

if __package__ is None or __package__ == "":
    from dummy_handler import DummyHandler
else:
    from .dummy_handler import DummyHandler


def test_get_jobs():
    handler = DummyHandler()
    jobs = PBS.get_jobs_from_handler(handler)
    attrs = [
        {"id": "7013474", "Job_Name": "job-01.pbs"},
        {"id": "7013475", "Job_Name": "job-02.pbs"},
    ]
    for i, job in enumerate(jobs):
        assert isinstance(job, PBSJob)
        for k, v in attrs[i].items():
            assert job[k] == v
        # Test attribute-style field access
        assert job.project == "_pbs_project_default"
        assert job.percent_completion == 0


@pytest.mark.skipif(
    condition=os.environ.get("LOGNAME", "runner") == "runner",
    reason="Executing tests on GHA, and localhost SSH does not behave well in this environment.",
)
@pytest.mark.parametrize("tracked", [True, False])
def test_get_tracked(to_destroy, tracked):
    if tracked:
        tracked_path = os.path.join(
            os.path.expanduser("~"), os.path.split(RERUN_TRACKED_FILE)[-1]
        )
        to_destroy.append(tracked_path)
        shutil.copyfile(
            os.path.join(os.path.dirname(__file__), "dummy_tracked.json"), tracked_path
        )
    queue = get_tracked_from_cluster(
        {"hostname": "localhost", "user": os.environ["LOGNAME"]}
    )
    if tracked:
        assert len(queue) == 2
    else:
        assert len(queue) == 0


@pytest.mark.parametrize("tracked", [True, False])
def test_track_new_jobs(to_destroy, tracked):
    n_new = 3
    if tracked:
        tracked_path = os.path.join(
            os.path.expanduser("~"), os.path.split(RERUN_TRACKED_FILE)[-1]
        )
        to_destroy.append(tracked_path)
        shutil.copyfile(
            os.path.join(os.path.dirname(__file__), "dummy_tracked.json"), tracked_path
        )
    new_jobs = [PBSJob.unsubmitted(os.devnull) for _ in range(n_new)]

    handler = LocalHandler()

    track_new_jobs(handler, new_jobs)

    queue = get_tracked_from_cluster(handler)

    if tracked:
        assert len(queue) == 2 + n_new
    else:
        assert len(queue) == 0 + n_new


@pytest.mark.parametrize(
    "valid",
    [
        # pytest.param(False,marks=pytest.mark.xfail(reason="Unrecognised batch system")),
        True
    ],
)
@pytest.mark.parametrize("jobs", [False, True])
@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("rerun", [False, True])
@pytest.mark.parametrize("memkill", [False, True])
@pytest.mark.parametrize("wallkill", [False, True])
@pytest.mark.parametrize("qsub", [False, True])
def test_rerun(to_destroy, valid, jobs, tracked, rerun, memkill, wallkill, qsub):
    os.environ["SCHEDTOOLS_PROG"] = "rerun"
    to_destroy.append(os.path.dirname(RERUN_TRACKED_CACHE))
    handler = DummyHandler(
        valid=valid,
        jobs=jobs,
        tracked=tracked,
        rerun=rerun,
        memkill=memkill,
        wallkill=wallkill,
        qsub=qsub,
    )
    rerun_jobs(
        handler=handler,
        logger=logging.getLogger(__name__).addHandler(logging.NullHandler()),
    )
    cached = get_tracked_cache()
    if tracked:
        if memkill and (not qsub) and (not rerun):
            # if qsub, id should not be in cached
            assert "70134" in cached
        if wallkill and (not qsub) and (not rerun):
            # if qsub, id should not be in cached
            assert "70135" in cached
    if jobs:
        assert "7013474" in cached
        assert "7013475" in cached
