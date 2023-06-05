import uuid
from datetime import datetime, timedelta
from typing import List, Union

from schedtools.utils import walltime_to

DEFAULT_PRIORITY = 0
UNSUBMITTED_PRIORITY = -1


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
        S="suspended",
        U="unsubmitted",  # This is an additional flag for jobs which are tracked but have not been submitted
    )
    # TODO: Make into a proper dataclass if needed.
    def __getattr__(self, key, *args, **kwargs):
        try:
            return super().__getattr__(key, *args, **kwargs)
        except AttributeError:
            return self.__getitem__(key, *args, **kwargs)

    @classmethod
    def unsubmitted(cls, jobscript_path):
        """Create unsubmitted job for tracking with low priority.

        Args:
            jobscript_path: path to jobscript.

        Returns:
            PBSJob
        """
        return cls(
            id=str(uuid.uuid1()),
            jobscript_path=jobscript_path,
            job_state="U",
            priority=UNSUBMITTED_PRIORITY,
        )

    @property
    def id(self):
        return self["id"]

    @property
    def name(self):
        return self["Job_Name"]

    @property
    def priority(self):
        return int(
            self.get(
                "priority",
                UNSUBMITTED_PRIORITY
                if self.status == "unsubmitted"
                else DEFAULT_PRIORITY,
            )
        )

    @property
    def jobscript_path(self):
        if "jobscript_path" in self:
            return self["jobscript_path"]
        return self["Submit_arguments"].replace("\n", "").split()[-1]

    @property
    def error_path(self):
        return self["Error_Path"].split(":")[-1]

    @property
    def percent_completion(self):
        if "resources_used.walltime" in self:
            return (
                100
                * walltime_to(self["resources_used.walltime"])
                / walltime_to(self["Resource_List.walltime"])
            )
        return 0

    @property
    def status(self):
        return self.status_dict[self.get("job_state", "U")]

    @property
    def is_running(self):
        return self.status == "running"

    @property
    def is_queued(self):
        return self.status == "queued"

    @property
    def walltime(self):
        hours, minutes, seconds = map(int, self["Resource_List.walltime"].split(":"))
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)

    @property
    def start_time(self):
        if "stime" in self:
            return datetime.strptime(self["stime"], "%a %b %d %H:%M:%S %Y")
        return None

    @property
    def end_time(self):
        if self.start_time is None:
            return None
        return self.start_time + self.walltime

    @property
    def has_elapsed(self):
        if self.end_time is None:
            return False
        return self.end_time < datetime.now()


class Queue:
    def __init__(self, jobs: List[PBSJob] = []):
        self.jobs = {j.id: j for j in jobs if len(j)}

    def pop(self, job: Union[str, PBSJob]):
        if isinstance(job, PBSJob):
            job = job.id
        self.jobs.pop(job)

    def append(self, job: PBSJob):
        self.jobs[job.id] = job

    def extend(self, jobs: List[PBSJob]):
        self.update({j.id: j for j in jobs})

    def update(self, other: Union[dict, "Queue"]):
        if isinstance(other, Queue):
            other = other.jobs
        self.jobs.update(other)

    def __iter__(self):
        # Sort by priority when iterating
        return iter(sorted(self.jobs.values(), key=lambda x: x.priority, reverse=True))

    def __contains__(self, job):
        if isinstance(job, PBSJob):
            job = job.id
        return job in self.jobs.keys()

    def __len__(self):
        return len(self.jobs)

    def count(self, status):
        assert status in PBSJob.status_dict.values()
        return len([job for job in self if job.status == status])
