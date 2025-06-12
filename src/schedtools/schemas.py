import copy
import os
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Iterator, List, Mapping, Union

from schedtools.clusters import Cluster
from schedtools.consts import experiment_path_key, job_id_key
from schedtools.utils import (
    parse_datetime,
    parse_memory,
    parse_timeperiod,
)


class JobState(Enum):
    EXITING = "exiting"
    HELD = "held"
    QUEUED = "queued"
    RUNNING = "running"
    MOVING = "moving"
    WAITING = "waiting"
    SUSPENDED = "suspended"
    UNKNOWN = "unknown"
    UNSUBMITTED = "unsubmitted"
    COMPLETED = "completed"
    FAILED = "failed"
    ALERT = "alert"

    @classmethod
    def parse(cls, state: str) -> "JobState":
        match state:
            case "E":
                return cls.EXITING
            case "H":
                return cls.HELD
            case "Q":
                return cls.QUEUED
            case "R":
                return cls.RUNNING
            case "T":
                return cls.MOVING
            case "W":
                return cls.WAITING
            case "S":
                return cls.SUSPENDED
            case "U":
                return cls.UNSUBMITTED
            case _:
                return cls.UNKNOWN


def _match_str_job(a: str, b: Union["JobSpec", "Job"]) -> bool:
    any_match = []
    if hasattr(b, "id"):
        any_match.append(a == b.id)
    if hasattr(b, "scheduler_id"):
        any_match.append(a == b.scheduler_id)
    return any(any_match)


def match_jobs(
    a: Union["JobSpec", "Job", str], b: Union["JobSpec", "Job", str]
) -> bool:
    if isinstance(a, str) and isinstance(b, str):
        return a == b
    elif isinstance(a, str):
        return _match_str_job(a, b)
    elif isinstance(b, str):
        return _match_str_job(b, a)
    any_match = []
    if hasattr(a, "id") and hasattr(b, "id"):
        any_match.append(a.id == b.id)
    if hasattr(a, "scheduler_id") and hasattr(b, "scheduler_id"):
        any_match.append(a.scheduler_id == b.scheduler_id)
    if any(any_match):
        return True
    return False


@dataclass
class ResourceRequest:
    # Memory requested
    mem_bytes: int

    # Number of CPUs requested
    ncpus: int

    # Number of GPUs requested
    ngpus: int

    # Number of nodes requested
    node_count: int

    # Place requested
    place: str

    # Priority requested
    priority: Union[int, None]

    # Select statement from job script
    select_statement: str

    # Walltime requested
    walltime: timedelta

    @classmethod
    def parse(cls, resource_list: dict) -> "ResourceRequest":
        return cls(
            mem_bytes=parse_memory(resource_list["mem"]),
            ncpus=resource_list["ncpus"],
            ngpus=resource_list.get("ngpus", 0),
            node_count=resource_list["nodect"],
            place=resource_list["place"],
            # Older PBS versions don't have priority_job
            priority=resource_list.get("priority_job", None),
            select_statement=resource_list["select"],
            walltime=parse_timeperiod(resource_list["walltime"]),
        )

    def to_sqlite(self) -> dict:
        return {
            "mem_bytes": self.mem_bytes,
            "ncpus": self.ncpus,
            "ngpus": self.ngpus,
            "node_count": self.node_count,
            "place": self.place,
            "priority": self.priority,
            "select_statement": self.select_statement,
            "walltime": self.walltime.total_seconds(),
        }

    # NOTE: Unused, as we are not serializing the resource requests into SQLite currently.
    # @classmethod
    # def from_sqlite(cls, data: dict) -> "ResourceRequest":
    #     return cls(
    #         mem_bytes=data["mem_bytes"],
    #         ncpus=data["ncpus"],
    #         ngpus=data["ngpus"],
    #         node_count=data["node_count"],
    #         place=data["place"],
    #         priority=data["priority"],
    #         select_statement=data["select_statement"],
    #         walltime=timedelta(seconds=data["walltime"]),
    #     )


@dataclass
class ResourceUsage:
    cpu_percent: int
    cpu_time: timedelta
    mem_bytes: int
    vmem_bytes: int
    ncpus: int
    ngpus: int
    walltime: timedelta

    @classmethod
    def parse(cls, resource_usage: dict) -> "ResourceUsage":
        return cls(
            cpu_percent=resource_usage.get("cpupercent", 0),
            cpu_time=parse_timeperiod(resource_usage.get("cput", "00:00:00")),
            mem_bytes=parse_memory(resource_usage.get("mem", "0b")),
            vmem_bytes=parse_memory(resource_usage.get("vmem", "0b")),
            ncpus=resource_usage.get("ncpus", 0),
            ngpus=resource_usage.get("ngpus", 0),
            walltime=parse_timeperiod(resource_usage.get("walltime", "00:00:00")),
        )


@dataclass
class JobSpec:
    id: str
    name: str
    experiment_path: str
    cluster: Cluster
    state: JobState
    # Time when the job was last modified
    modified_time: datetime
    comment: Union[str, None]
    queue: Union[str, None]
    project: Union[str, None]
    jobscript_path: Union[str, None]

    @property
    def is_running(self) -> bool:
        return self.state == JobState.RUNNING

    @property
    def is_queued(self) -> bool:
        return self.state == JobState.QUEUED

    @property
    def percent_completion(self) -> int:
        if self.state == JobState.COMPLETED:
            return 100
        return 0

    @classmethod
    def from_sqlite(cls, data: Mapping[str, Any]) -> "JobSpec":
        if not isinstance(data, dict):
            data = dict(data)
        return cls(
            id=data["id"],
            name=os.path.basename(data["experiment_path"]),
            experiment_path=data["experiment_path"],
            cluster=Cluster(data.get("cluster", "unknown")),
            state=JobState(data["state"]),
            queue=data["queue"],
            project=data["project"],
            jobscript_path=data["jobscript_path"],
            modified_time=parse_datetime(data["modified_time"]),
            comment=data.get("comment", None),
        )

    def __eq__(self, other: Any) -> bool:
        return match_jobs(self, other)

    @classmethod
    def from_unsubmitted(
        cls,
        *,
        jobscript_path: str,
        experiment_path: str,
        queue: Union[str, None] = None,
        project: Union[str, None] = None,
        cluster: Union[Cluster, str, None] = None,
    ) -> "JobSpec":
        if cluster is None:
            cluster = Cluster.from_local()
        elif isinstance(cluster, str):
            cluster = Cluster(cluster)
        return cls(
            id=str(uuid.uuid4()),
            experiment_path=experiment_path,
            name=os.path.basename(experiment_path),
            state=JobState.UNSUBMITTED,
            cluster=cluster,
            jobscript_path=jobscript_path,
            queue=queue,
            project=project,
            modified_time=datetime.now(),
            comment=None,
        )

    def to_sqlite(self) -> dict:
        return {
            "id": self.id,
            "experiment_path": self.experiment_path,
            "state": self.state.value,
            "queue": self.queue,
            "project": self.project,
            "jobscript_path": self.jobscript_path,
            "cluster": self.cluster.value,
            "modified_time": self.modified_time.isoformat(),
            "comment": self.comment,
        }


@dataclass
class Job(JobSpec):
    """A job from a scheduler."""

    # Scheduler-defined job ID
    scheduler_id: str

    # Job owner
    owner: str

    # Summary of requested resources
    resource_request: ResourceRequest

    # Summary of resource usage
    resource_usage: Union[ResourceUsage, None]

    # Server name
    server: str

    # Time when the job started execution.  Changes when job is restarted.
    start_time: Union[datetime, None]

    # Time when the job was created
    creation_time: datetime

    # Time when job became eligible to run.
    queue_time: datetime

    # Checkpoint
    checkpoint: str

    # Arguments used to submit the job
    submit_arguments: Union[List[str], None]

    # Path to the error file
    error_path: str

    # Path to the output file
    output_path: str

    # Priority of the job
    priority: int

    # Number of times the job has been restarted
    run_count: int

    # Unmodified job details
    job_details: dict

    @property
    def end_time(self) -> Union[datetime, None]:
        if self.start_time is None:
            return None
        return self.start_time + self.resource_request.walltime

    @property
    def owner_name(self) -> str:
        """Get the owner name from the owner string, which is in the format
        `user@host`."""
        return self.owner.split("@")[0]

    @property
    def percent_completion(self):
        if self.state == JobState.COMPLETED:
            return 100
        if self.state == JobState.FAILED:
            return 0
        if self.resource_usage is not None and self.resource_usage.walltime:
            return (
                100
                * self.resource_usage.walltime.total_seconds()
                / self.resource_request.walltime.total_seconds()
            )
        return 0

    @property
    def walltime(self) -> timedelta:
        return self.resource_request.walltime

    @classmethod
    def parse(cls, scheduler_id: str, job: dict) -> "Job":
        if "resources_used" in job:
            resource_usage = ResourceUsage.parse(job["resources_used"])
        else:
            resource_usage = None
        if "stime" in job:
            start_time = parse_datetime(job["stime"])
        else:
            start_time = None
        if "Submit_arguments" in job:
            submit_arguments = job["Submit_arguments"].replace("\n", "").split()
            jobscript_path = submit_arguments[-1]
        else:
            submit_arguments = None
            jobscript_path = None
        # Attempt to get the job ID from the job's environment variables
        variables: dict = job.get("Variable_List", {})
        id = variables.get(job_id_key, None)
        experiment_path = variables.get(experiment_path_key, None)
        cluster = Cluster.from_server(job["server"])

        return cls(
            id=id,
            experiment_path=experiment_path,
            cluster=cluster,
            scheduler_id=scheduler_id,
            name=job["Job_Name"],
            owner=job["Job_Owner"],
            state=JobState.parse(job.get("job_state", "unknown")),
            resource_usage=resource_usage,
            resource_request=ResourceRequest.parse(job["Resource_List"]),
            queue=job["queue"],
            server=job["server"],
            project=job["project"],
            start_time=start_time,
            jobscript_path=jobscript_path,
            creation_time=parse_datetime(job["ctime"]),
            queue_time=parse_datetime(job["qtime"]),
            checkpoint=job["Checkpoint"],
            submit_arguments=submit_arguments,
            # Error_Path is of form `<hostname>:<error_path>`
            error_path=job["Error_Path"].split(":")[-1],
            # Output_Path is of form `<hostname>:<output_path>`
            output_path=job["Output_Path"].split(":")[-1],
            modified_time=parse_datetime(job["mtime"]),
            priority=job.get("Priority", 0),
            comment=job.get("comment", None),
            run_count=job.get("run_count", 0),
            job_details=job,
        )


class Queue:
    jobs: List[Job]

    def __init__(self, jobs: List[Job]):
        self.jobs = jobs

    @classmethod
    def parse(cls, query_response: dict) -> "Queue":
        return cls([Job.parse(id, job) for id, job in query_response.items()])

    def __iter__(self) -> Iterator[Job]:
        return iter(self.jobs)

    def __getitem__(self, id: str) -> Job:
        for job in self.jobs:
            if match_jobs(job, id):
                return job
        raise KeyError(f"Job with ID '{id}' not found in queue")

    def add(self, job: Job) -> None:
        for i, j in enumerate(self.jobs):
            if match_jobs(j, job):
                self.jobs[i] = job
                break
        else:
            self.jobs.append(job)

    def merge(self, other: "Queue") -> "Queue":
        new = copy.deepcopy(self)
        for job in other.jobs:
            new.add(job)
        return new

    def __contains__(self, job: Union[str, Job]) -> bool:
        for j in self.jobs:
            if match_jobs(j, job):
                return True
        return False

    def __len__(self) -> int:
        return len(self.jobs)

    def get(self, id: str) -> Job:
        return self.__getitem__(id)

    def pop(self, job: str) -> Job:
        for i, j in enumerate(self.jobs):
            if match_jobs(j, job):
                return self.jobs.pop(i)
        raise KeyError(f"Job with ID '{job}' not found in queue")

    def count(self, status: JobState) -> int:
        return sum(1 for job in self if job.state == status)

    def filter_owner(self, owner: str) -> "Queue":
        if "@" in owner:
            return Queue([j for j in self if j.owner == owner])
        else:
            return Queue([j for j in self if j.owner_name == owner])

    def filter_state(self, states: Union[JobState, List[JobState]]) -> "Queue":
        if not isinstance(states, Iterable):
            states = [states]
        states = [el if isinstance(el, JobState) else JobState(el) for el in states]
        return Queue([j for j in self if j.state in states])

    def filter_id(self, ids: Union[str, List[str]]) -> "Queue":
        if isinstance(ids, str):
            ids = [ids]
        return Queue([j for j in self if j.id in ids])

    def filter_name(self, pattern: str) -> "Queue":
        return Queue([j for j in self if re.search(pattern, j.name)])

    def filter_cluster(self, cluster: Union[Cluster, str]) -> "Queue":
        if isinstance(cluster, Cluster):
            cluster = cluster.value
        return Queue([j for j in self if j.cluster.value == cluster])
