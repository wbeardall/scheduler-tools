import json
import logging
import os

import pytest

from schedtools.core import PBSJob
from schedtools.jobs import RERUN_TRACKED_FILE, RERUN_TRACKED_CACHE, rerun_jobs, get_tracked_cache
from schedtools.managers import PBS
from schedtools.shell_handler import ShellHandler, SSHResult

dummy_queue = """a bunch of junk data at the top
of the file
intended to simulate cluster information echoed at login

Job Id: 7013474.pbs
    Job_Name = job-01.pbs
    Job_Owner = user@cx3-2-29.cx3.hpc.ic.ac.uk
    job_state = Q
    queue = v1_gpu72
    server = pbs1.rcs.ic.ac.uk
    Checkpoint = u
    ctime = Mon Feb 13 19:00:07 2023
    Error_Path = cx3-2-29.cx3.hpc.ic.ac.uk:/rds/general/user/user/home/project-
	directory/scripts/job-01.pbs.e7013474
    group_list = hpc-gstan
    Hold_Types = n
    Join_Path = n
    Keep_Files = oed
    Mail_Points = n
    mtime = Mon Feb 13 20:11:15 2023
    Output_Path = cx3-2-29.cx3.hpc.ic.ac.uk:/rds/general/user/user/home/project
	-directory/scripts/job-01.pbs.o7013474
    Priority = 0
    qtime = Mon Feb 13 19:00:07 2023
    Rerunable = True
    Resource_List.mem = 8gb
    Resource_List.mpiprocs = 4
    Resource_List.ncpus = 4
    Resource_List.ngpus = 1
    Resource_List.nodect = 1
    Resource_List.place = free
    Resource_List.select = 1:ncpus=4:mem=8gb:ngpus=1:gpu_type=RTX6000:mpiprocs=
	4:ompthreads=1
    Resource_List.walltime = 72:00:00
    substate = 10
    Variable_List = PBS_O_SYSTEM=Linux,PBS_O_SHELL=/bin/bash,
	PBS_O_HOME=/rds/general/user/user/home,PBS_O_LOGNAME=user,
	PBS_O_WORKDIR=/rds/general/user/user/home/project-directory/scrip
	ts,PBS_O_LANG=en_GB.UTF-8,
	PBS_O_PATH=/rds/general/user/user/home/anaconda3/envs/tsk/bin:/rds/ge
	neral/user/user/home/anaconda3/condabin:/rds/general/user/user/home/a
	naconda3/bin:/usr/lib/qt-3.3/bin:/apps/modules/4.7.1/bin:/usr/local/bin
	:/usr/bin:/usr/local/sbin:/usr/sbin:/rds/general/user/user/home/.dotne
	t/tools:/opt/ibutils/bin:/opt/pbs/bin:/apps/anaconda3/4.9.2/install,
	PBS_O_MAIL=/var/spool/mail/user,PBS_O_QUEUE=v1_gpu72,
	PBS_O_HOST=cx3-2-29.cx3.hpc.ic.ac.uk
    comment = Not Running: Insufficient amount of resource: ncpus 
    etime = Mon Feb 13 19:00:07 2023
    eligible_time = 01:10:51
    Submit_arguments = -W group_list=hpc-gstan /rds/general/user/user/home/pro
	ject-directory/pbs_scripts/job-01.pbs
    estimated.exec_vnode = (cx3-10-8.cx3.hpc.ic.ac.uk:ncpus=4:mem=8388608kb:ngp
	us=1)
    estimated.start_time = Tue Feb 14 02:10:01 2023
    project = _pbs_project_default

Job Id: 7013475.pbs
    Job_Name = job-02.pbs
    Job_Owner = user@cx3-2-29.cx3.hpc.ic.ac.uk
    job_state = Q
    queue = v1_gpu72
    server = pbs1.rcs.ic.ac.uk
    Checkpoint = u
    ctime = Mon Feb 13 19:00:08 2023
    Error_Path = cx3-2-29.cx3.hpc.ic.ac.uk:/rds/general/user/user/home/project-
	directory/scripts/job-02.pbs.e7013475
    group_list = hpc-gstan
    Hold_Types = n
    Join_Path = n
    Keep_Files = oed
    Mail_Points = n
    mtime = Mon Feb 13 20:11:15 2023
    Output_Path = cx3-2-29.cx3.hpc.ic.ac.uk:/rds/general/user/user/home/project
	-directory/scripts/job-02.pbs.o7013475
    Priority = 0
    qtime = Mon Feb 13 19:00:08 2023
    Rerunable = True
    Resource_List.mem = 8gb
    Resource_List.mpiprocs = 4
    Resource_List.ncpus = 4
    Resource_List.ngpus = 1
    Resource_List.nodect = 1
    Resource_List.place = free
    Resource_List.select = 1:ncpus=4:mem=8gb:ngpus=1:gpu_type=RTX6000:mpiprocs=
	4:ompthreads=1
    Resource_List.walltime = 72:00:00
    substate = 10
    Variable_List = PBS_O_SYSTEM=Linux,PBS_O_SHELL=/bin/bash,
	PBS_O_HOME=/rds/general/user/user/home,PBS_O_LOGNAME=user,
	PBS_O_WORKDIR=/rds/general/user/user/home/project-directory/scrip
	ts,PBS_O_LANG=en_GB.UTF-8,
	PBS_O_PATH=/rds/general/user/user/home/anaconda3/envs/tsk/bin:/rds/ge
	neral/user/user/home/anaconda3/condabin:/rds/general/user/user/home/a
	naconda3/bin:/usr/lib/qt-3.3/bin:/apps/modules/4.7.1/bin:/usr/local/bin
	:/usr/bin:/usr/local/sbin:/usr/sbin:/rds/general/user/user/home/.dotne
	t/tools:/opt/ibutils/bin:/opt/pbs/bin:/apps/anaconda3/4.9.2/install,
	PBS_O_MAIL=/var/spool/mail/user,PBS_O_QUEUE=v1_gpu72,
	PBS_O_HOST=cx3-2-29.cx3.hpc.ic.ac.uk
    comment = Not Running: Insufficient amount of resource: ncpus 
    etime = Mon Feb 13 19:00:08 2023
    eligible_time = 01:10:51
    Submit_arguments = -W group_list=hpc-gstan /rds/general/user/user/home/pro
	ject-directory/pbs_scripts/job-02.pbs
    estimated.exec_vnode = (cx3-10-8.cx3.hpc.ic.ac.uk:ncpus=4:mem=8388608kb:ngp
	us=1)
    estimated.start_time = Tue Feb 14 02:10:01 2023
    project = _pbs_project_default
"""

with open(os.path.join(os.path.dirname(__file__),"dummy_tracked.json"),"r") as f:
    dummy_tracked = f.readlines()

class DummyHandler(ShellHandler):
    def __init__(self,valid=True, jobs=True, tracked=True, rerun=True, memkill=True, wallkill=True,qsub=True,
                 qdel=True):
        self.responses = {
            "qstat": SSHResult([], [],[],0) if valid else SSHResult([], [],[],1),
            "qstat -f": SSHResult([], dummy_queue.split("\n"),[],0) if jobs else SSHResult([], [],[],0),
            f"cat {RERUN_TRACKED_FILE}": SSHResult([], dummy_tracked,[],0) if tracked else SSHResult([], [],[],1),
            "cat /rds/general/user/user/home/project-directory/scripts/job-03.pbs.o70134": (
                SSHResult([],["PBS: job killed: mem"],[],159) if memkill else SSHResult([], [],[],0)
            ),
            "cat /rds/general/user/user/home/project-directory/scripts/job-03.pbs.o70135": (
                SSHResult([],["PBS: job killed: walltime"],[],159) if wallkill else SSHResult([], [],[],0)
            )
        }
        self.responses_in = {
            "qrerun": SSHResult([],[],[],0) if rerun else SSHResult([],[],[],159),
            "qsub": SSHResult([],[],[],0) if qsub else SSHResult([],[],[],38),
            "qdel": SSHResult([],[],[],0) if qdel else SSHResult([],[],[],1),
        }

    def execute(self,command):
        if command in self.responses:
            return self.responses[command]
        for k,v in self.responses_in.items():
            if k in command:
                return v
        return SSHResult([], [],["command not found."],1)

def test_get_jobs():
    handler = DummyHandler()
    jobs = PBS.get_jobs_from_handler(handler)
    attrs = [
        {"id":"7013474",
        "Job_Name": "job-01.pbs"},
        {"id":"7013475",
        "Job_Name": "job-02.pbs"},
    ]
    for i, job in enumerate(jobs):
        assert isinstance(job, PBSJob)
        for k,v in attrs[i].items():
            assert job[k] == v
        # Test attribute-style field access
        assert job.project == "_pbs_project_default"
        assert job.percent_completion == 0

@pytest.mark.parametrize("valid",[
    #pytest.param(False,marks=pytest.mark.xfail(reason="Unrecognised batch system")),
    True])
@pytest.mark.parametrize("jobs",[False,True])
@pytest.mark.parametrize("tracked",[False,True])
@pytest.mark.parametrize("rerun",[False,True])
@pytest.mark.parametrize("memkill",[False,True])
@pytest.mark.parametrize("wallkill",[False,True])
@pytest.mark.parametrize("qsub",[False,True])
def test_rerun(to_destroy, valid, jobs, tracked, rerun, memkill, wallkill,qsub):
    os.environ["SCHEDTOOLS_PROG"] = "rerun"
    to_destroy.append(os.path.dirname(RERUN_TRACKED_CACHE))
    handler = DummyHandler(valid=valid,jobs=jobs,tracked=tracked,rerun=rerun,memkill=memkill,wallkill=wallkill,qsub=qsub)
    rerun_jobs(handler=handler,logger=logging.getLogger(__name__).addHandler(logging.NullHandler()))
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