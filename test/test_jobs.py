from schedtools.pbs_dataclasses import PBSJob
from schedtools.jobs import PRIORITY_RERUN_FILE, get_jobs, get_rerun_from_file
from schedtools.shell_handler import SSHResult

qstat_raw_example = """a bunch of junk data at the top
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

class DummyHandler(object):
    def __init__(self,*args, **kwargs):
        pass
    def execute(self,command):
        if command == "qstat -f":
            return SSHResult([], qstat_raw_example.split("\n"),[],0)
        elif command == f"cat {PRIORITY_RERUN_FILE}":
            return SSHResult([], ['{"1234":"/path/to/file1", "1235":"/path/to/file2"}\n'],[],0)
        elif command.startswith("qrerun"):
            return  SSHResult([],[],[],159)


def test_get_jobs():
    handler = DummyHandler()
    jobs = get_jobs(handler)
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

def test_rerun_from_file():
    handler = DummyHandler()
    jobs = get_rerun_from_file(handler)
    expected = {
        "1234": "/path/to/file1",
        "1235": "/path/to/file2"
    }
    for i, (k,v) in enumerate(expected.items()):
        assert jobs[i]["id"] == k
        assert jobs[i].jobscript_path == v