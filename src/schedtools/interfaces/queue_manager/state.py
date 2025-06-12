import os
from dataclasses import dataclass
from functools import cached_property, lru_cache
from typing import Any, Dict, Union
from urllib.parse import urlparse

import paramiko

from schedtools.clusters import Cluster
from schedtools.managers import WorkloadManager, get_workload_manager
from schedtools.schemas import Job, JobSpec, JobState, Queue
from schedtools.shell_handler import ShellHandler
from schedtools.utils import connect_from_attrs, connect_to_host


@dataclass
class Host:
    name: str
    user: str
    requires_password: bool
    in_config: bool
    hostname: Union[str, None] = None
    port: Union[int, None] = None

    @classmethod
    def from_config(cls, host_alias: str, ssh: paramiko.SSHConfig):
        host_config = ssh.lookup(host_alias)
        prefer_password = (
            host_config.get("preferredauthentications", None) == "password"
        )
        no_key = len(host_config.get("identityfile", [])) == 0
        requires_password = prefer_password or no_key
        return cls(
            name=host_alias,
            requires_password=requires_password,
            user=host_config["user"],
            in_config=True,
        )

    @classmethod
    def from_url(cls, url: str) -> "Host":
        parsed = urlparse(url)
        if parsed.scheme != "ssh":
            raise ValueError(f"Invalid URL: {url}")
        return cls(
            name=parsed.hostname,
            user=parsed.username,
            hostname=parsed.hostname,
            port=parsed.port,
            requires_password=True,
            in_config=False,
        )


class KVStore(dict):
    def push(self, key: str, value: Any):
        self[key] = value

    def get(self, key: str) -> Any:
        return self[key]


@dataclass
class JobLog:
    filename: Union[str, None] = None
    log: Union[str, None] = None
    notify: Union[str, None] = None
    close: bool = False


@dataclass
class JobFilter:
    state: Union[JobState, None] = None
    name: Union[str, None] = None

    @property
    def is_empty(self) -> bool:
        return self.state is None and self.name is None

    def __str__(self) -> str:
        if self.is_empty:
            return "All jobs"
        clauses = []
        if self.state is not None:
            clauses.append(f"state = {self.state.value}")
        if self.name is not None:
            clauses.append(f"name matches '{self.name}'")
        return ", ".join(clauses)


class ManagerState:
    ssh: paramiko.SSHConfig
    selected_hostname: str | None = None
    ssh_client: paramiko.SSHClient | None = None
    kv_store: KVStore
    added_hosts: Dict[str, Host]
    queues: Dict[str, Queue]
    cluster_map: Dict[str, Cluster]
    filter: JobFilter

    def __init__(self):
        self.ssh = paramiko.SSHConfig()
        self.ssh.parse(
            open(os.environ.get("SSH_CONFIG", os.path.expanduser("~/.ssh/config")))
        )
        self.kv_store = KVStore()
        self.added_hosts = {}
        self.queues = {}
        self.cluster_map = {}
        self.filter = JobFilter()

    @cached_property
    def _config_hosts(self) -> Dict[str, Host]:
        # Filter out wildcards
        hostnames = sorted([el for el in self.ssh.get_hostnames() if el != "*"])

        return {host: Host.from_config(host, self.ssh) for host in hostnames}

    @property
    def hosts(self) -> Dict[str, Host]:
        return {**self.added_hosts, **self._config_hosts}

    def connect(
        self, pw_key: Union[str, None] = None, timeout_seconds: Union[int, None] = None
    ) -> paramiko.SSHClient:
        if self.ssh_client is not None:
            return self.ssh_client
        kwargs = dict(timeout=timeout_seconds)
        if pw_key is not None:
            kwargs["password"] = self.kv_store.get(pw_key)
            kwargs["allow_agent"] = False
            kwargs["look_for_keys"] = False
        host = self.selected_host
        if host.in_config:
            self.ssh_client = connect_to_host(host.name, allow_prompt=False, **kwargs)
        else:
            self.ssh_client = connect_from_attrs(
                hostname=host.hostname, user=host.user, port=host.port, **kwargs
            )
        return self.ssh_client

    def register_host_from_url(self, url: str) -> str:
        host = Host.from_url(url)
        self.added_hosts[host.name] = host
        return host.name

    def set_selected_host(self, hostname: str) -> None:
        if hostname == self.selected_hostname:
            return
        if hostname not in self.hosts:
            raise ValueError(f"Host '{hostname}' not found in registered hosts.")
        self.selected_hostname = hostname
        # Reset the SSH client to force a reconnect
        self.ssh_client = None

    @property
    def shell_handler(self) -> ShellHandler:
        if self.ssh_client is None:
            raise ValueError("No SSH client connected")
        return self._get_shell_handler(self.ssh_client)

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_shell_handler(client: paramiko.SSHClient) -> ShellHandler:
        return ShellHandler(client)

    @property
    def workload_manager(self) -> WorkloadManager:
        return self._get_workload_manager(self.shell_handler)

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_workload_manager(shell_handler: ShellHandler) -> WorkloadManager:
        return get_workload_manager(shell_handler)

    @property
    def job_data(self) -> Queue:
        if self.selected_hostname not in self.queues:
            # Always force a refresh of the job data on remote before pulling
            self.shell_handler.set_missing_alerts()
            cached_queue = self.workload_manager.get_cluster_jobs_from_db()
            queue = self.workload_manager.get_jobs()

            if self.selected_host.user is not None:
                queue = queue.filter_owner(self.selected_host.user)

            queue = cached_queue.merge(queue)
            self.queues[self.selected_hostname] = queue
        return self.apply_filter(self.queues[self.selected_hostname])

    def apply_filter(self, queue: Queue) -> Queue:
        if self.filter is None:
            return queue
        if self.filter.state is not None:
            queue = queue.filter_state(self.filter.state)
        if self.filter.name is not None:
            queue = queue.filter_name(self.filter.name)
        return queue

    def set_filter(self, filter: JobFilter) -> None:
        self.filter = filter

    def resubmit_filtered_jobs(self) -> None:
        if self.filter is None or self.filter.is_empty:
            raise ValueError("No filter set.")
        jobs = self.job_data
        for job in jobs:
            self.workload_manager.resubmit_job(job)

    def delete_filtered_jobs(self, expected_count: int) -> None:
        if self.filter is None or self.filter.is_empty:
            raise ValueError("No filter set.")
        jobs = self.job_data
        if len(jobs) != expected_count:
            raise ValueError(
                f"Expected {expected_count} jobs, got {len(jobs)}. Exiting for safety."
            )
        self.shell_handler.delete_jobs([job.id for job in jobs])

    @property
    def selected_host(self) -> Host:
        if self.selected_hostname is None:
            raise ValueError("No host selected")
        return self.hosts[self.selected_hostname]

    def evict_queue(self, hostname: str) -> None:
        self.queues.pop(hostname, None)

    def evict_current_queue(self) -> None:
        self.evict_queue(self.selected_hostname)

    def get_job(self, job_id: str) -> Job:
        return self.job_data.get(job_id)

    def resubmit_alerted_jobs(self) -> None:
        alerted = self.job_data.filter_state(JobState.ALERT)
        for job in alerted:
            self.workload_manager.resubmit_job(job)

    def count_by_state(self, state: JobState) -> int:
        return len(self.job_data.filter_state(state))

    @lru_cache(maxsize=None)
    def get_job_log(self, job_id: str) -> JobLog:
        job = self.get_job(job_id)
        experiment_logfile = os.path.join(
            job.experiment_path,
            "logfile.log",
        )
        try:
            with self.shell_handler.open_file(experiment_logfile) as f:
                return JobLog(filename=experiment_logfile, log=f.read().decode("utf-8"))
        except IOError:
            output_path = getattr(job, "output_path", None)
            if output_path is None:
                return JobLog(
                    filename=None,
                    log=None,
                    notify=f"Could not identify any log paths for job {job.name}",
                    close=True,
                )
            try:
                with self.shell_handler.open_file(output_path) as f:
                    return JobLog(
                        filename=output_path,
                        log=f.read().decode("utf-8"),
                        notify="Could not find experiment log, falling back to job log file",
                    )
            except IOError:
                return JobLog(
                    log=None,
                    notify=f"No logs found for job {job.name}",
                    close=True,
                )


def can_elevate_job(job: Job) -> bool:
    return job.state == JobState.QUEUED


def can_resubmit_job(job: Job) -> bool:
    return job.state in [JobState.FAILED, JobState.ALERT]


def get_live_icon(job: Union[Job, JobSpec]) -> str:
    if job.state == JobState.COMPLETED:
        return "✅"
    if job.state == JobState.FAILED:
        return "❌"
    if job.state == JobState.ALERT:
        return "⚠️"
    if isinstance(job, Job):
        return "🟢"
    return "🔴"
