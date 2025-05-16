from dataclasses import dataclass
from functools import cached_property, lru_cache
import os
from typing import List, Union

import paramiko

from schedtools.managers import WorkloadManager, get_workload_manager
from schedtools.shell_handler import ShellHandler
from schedtools.core import PBSJob, Queue
from schedtools.utils import connect_to_host


@dataclass
class Host:
    name: str
    user: str
    requires_password: bool
    password: str | None = None

    @classmethod
    def from_config(cls, host_alias: str, ssh: paramiko.SSHConfig):
        host_config = ssh.lookup(host_alias)
        prefer_password = host_config.get('preferredauthentications', None) == 'password'
        no_key = len(host_config.get('identityfile', [])) == 0
        requires_password = prefer_password or no_key
        return cls(
            name=host_alias,
            requires_password=requires_password,
            user=host_config['user'],
        )

@dataclass
class Job:
    id: str
    name: str
    owner: str
    status: str
    walltime: str
    start_time: str
    end_time: str
    percent_completion: float
    @classmethod
    def from_pbs_job(cls, pbs_job: PBSJob) -> 'Job':
        return cls(
            id=pbs_job.id,
            name=pbs_job.name,
            owner=pbs_job.owner,
            status=pbs_job.status,
            walltime=pbs_job.walltime,
            start_time=pbs_job.start_time,
            end_time=pbs_job.end_time,
            percent_completion=pbs_job.percent_completion,
        )

class ManagerState:
    ssh: paramiko.SSHConfig
    selected_host: Host | None = None
    ssh_client: paramiko.SSHClient | None = None

    def __init__(self):
        self.ssh = paramiko.SSHConfig()
        self.ssh.parse(open(os.environ.get("SSH_CONFIG", os.path.expanduser("~/.ssh/config"))))


    @cached_property
    def hosts(self) -> List[Host]:
        # Filter out wildcards
        hostnames = sorted([el for el in self.ssh.get_hostnames() if el != '*'])

        hosts = [Host.from_config(host, self.ssh) for host in hostnames]
        return hosts

    def set_selected_host(self, host: Host):
        self.selected_host = host

    def get_selected_host(self) -> Host | None:
        return self.selected_host

    def connect(self, password: str | None = None) -> paramiko.SSHClient:
        if self.ssh_client is not None:
            return self.ssh_client
        if self.selected_host is None:
            raise ValueError("No host selected")
        kwargs = {}
        if password is not None:
            kwargs["password"] = password
        self.ssh_client = connect_to_host(self.selected_host.name, allow_prompt=False, **kwargs)
        return self.ssh_client

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
        return self._get_job_data(self.workload_manager, self.selected_host.user)
    
    @staticmethod
    @lru_cache(maxsize=None)
    def _get_job_data(workload_manager: WorkloadManager, user: Union[str, None] = None) -> dict:
        jobs = workload_manager.get_jobs()
        if user is not None:
            jobs = jobs.filter_owner(user)
        return jobs
    
    def get_job(self, job_id: str) -> Job:
        return Job.from_pbs_job(self.job_data.get(job_id))
