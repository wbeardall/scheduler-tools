from typing import List, Union

from schedtools.utils import walltime_to

class PBSJob(dict):
    """Simple dict-like interface for storing PBS job information.
    
    Follows field name conventions of `qstat -f` for simplicity, even though
    these are non-Pythonic.
    """
    status_dict = dict(
        E="exiting",
		H="held",
		Q="queued",
		R="running",
		T="moving",
		W="waiting",
		S="suspended"
    )
    # TODO: Make into a proper dataclass if needed.
    def __getattr__(self, key, *args, **kwargs):
        try:
            return super().__getattr__(key, *args, **kwargs)
        except AttributeError:
            return self.__getitem__(key, *args, **kwargs)

    @property
    def id(self):
        return self["id"]
    
    @property
    def name(self):
        return self["Job_Name"]
        
    @property
    def jobscript_path(self):
        if "jobscript_path" in self:
            return self["jobscript_path"]
        return self["Submit_arguments"].replace("\n","").split()[-1]
    
    @property
    def error_path(self):
        return self["Error_Path"].split(":")[-1]

    @property
    def percent_completion(self):
        if "resources_used.walltime" in self:
            return 100 * walltime_to(self["resources_used.walltime"]) / walltime_to(self["Resource_List.walltime"]) 
        return 0

    @property
    def status(self):
        return self.status_dict[self["job_state"]]

    @property
    def is_running(self):
        return self.status=="running"

    @property
    def is_queued(self):
        return self.status=="queued"
    
class Queue:
    def __init__(self,jobs: List[PBSJob] = []):
        self.jobs = {j.id:j for j in jobs if len(j)}

    def pop(self,job: Union[str, PBSJob]):
        if isinstance(job, PBSJob):
            job = job.id
        self.jobs.pop(job)

    def append(self,job: PBSJob):
        self.jobs[job.id] = job

    def extend(self, jobs: List[PBSJob]):
        self.update({j.id:j for j in jobs})

    def update(self, other: Union[dict, "Queue"]):
        if isinstance(other, Queue):
            other = other.jobs
        self.jobs.update(other)

    def __iter__(self):
        return iter(self.jobs.values())
    
    def __contains__(self, job):
        if isinstance(job, PBSJob):
            job = job.id
        return job in self.jobs.keys()
    
    def __len__(self):
        return len(self.jobs)