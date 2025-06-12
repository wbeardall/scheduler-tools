import os
import platform
import re
import subprocess
import traceback
import warnings
from collections.abc import Iterable
from datetime import datetime, timedelta
from functools import wraps
from getpass import getpass
from typing import Protocol, Union

import paramiko
import paramiko.config


def connect_from_attrs(
    *, hostname: str, user: str, password: str | None, port: int | None = None, **kwargs
) -> paramiko.SSHClient:
    port = port or 22
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=hostname,
        username=user,
        port=port,
        password=password,
        **kwargs,
    )
    return ssh_client


def connect_to_host(
    host_alias, get_password=False, allow_prompt=True, **kwargs
) -> paramiko.SSHClient:
    """Connect to an SSH host using an alias defined in `~/.ssh/config`.

    Args:
        host_alias: Alias for host to connect to.
    """
    if isinstance(host_alias, str):
        ssh = paramiko.SSHConfig()
        user_config_file = os.environ.get(
            "SSH_CONFIG", os.path.expanduser("~/.ssh/config")
        )
        if os.path.exists(user_config_file):
            with open(user_config_file) as f:
                ssh.parse(f)

        host_config = ssh.lookup(host_alias)
    else:
        assert isinstance(host_alias, dict)
        host_config = host_alias

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh_client.connect(
            hostname=host_config["hostname"],
            username=host_config["user"],
            port=int(host_config.get("port", 22)),
            key_filename=host_config.get("identityfile", None),
            **kwargs,
        )
        password = None
    except (
        paramiko.ssh_exception.PasswordRequiredException,
        paramiko.ssh_exception.AuthenticationException,
    ) as e:
        if not allow_prompt:
            raise e
        password = getpass(
            prompt="Password for {}@{}: ".format(
                host_config["user"], host_config["hostname"]
            )
        )
        ssh_client.connect(
            hostname=host_config["hostname"],
            username=host_config["user"],
            port=int(host_config.get("port", 22)),
            password=password,
            # We already have the password, so don't look for keys or talk to the agent
            allow_agent=False,
            look_for_keys=False,
        )
    if get_password:
        return password
    return ssh_client


def journald_active():
    if platform.system() in ["Windows", "Darwin"]:
        return False
    return not subprocess.run(
        "systemctl is-active --quiet systemd-journald".split()
    ).returncode


def systemd_service():
    return "SYSTEMD_SERVICE" in os.environ


def config_dir():
    if systemd_service():
        base = "/etc"
    else:
        base = os.path.expanduser("~")
    return os.path.join(base, ".schedtools")


class Singleton(type):
    """Singleton metaclass.

    Only allows one instance of each class.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class RevDict(dict):
    @property
    def rev(self):
        return RevDict({v: k for k, v in self.items()})


class RetryError(Exception):
    """ """

    pass


def retry_on(exception, max_tries=5, allow=None):
    """Decorator for retrying functions under specific exception conditions.

    `allow` can be passed a callable to further constrain allowed conditions.
    Logic is as follows: when `allow` is provided, exceptions have to both match `exception` and
    `allow` to continue.

    Args:
        exception: `Exception` or tuple of exceptions to retry upon
        max_tries: Max retry attempts. Defaults to 5.
        allow: Optional callable to further specify retry conditions. Defaults to None.

    """
    if isinstance(exception, Iterable):
        if not all([issubclass(el, Exception) for el in exception]):
            raise TypeError("Iterables must only contain Exception subclasses")
        exception = tuple(exception)
    elif not issubclass(exception, Exception):
        raise TypeError("exception must be Exception or Iterable of Exceptions")

    if allow is None:

        def allow(x):
            return True

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(max_tries):
                try:
                    return func(*args, **kwargs)
                except exception as e:
                    if not allow(e):
                        raise e
                    if "DISABLE_RETRY" in os.environ:
                        raise e
                    if i == max_tries - 1:
                        raise RetryError(
                            f"Maximum retry count ({max_tries}) reached."
                        ) from e
                    warnings.warn(
                        "{}\n{} failed. Retrying... ({}/{})".format(
                            e, func, i + 1, max_tries
                        )
                    )
                    traceback.print_exc()

        return wrapper

    return decorator


class JobWithId(Protocol):
    id: str


class JobWithSchedulerId(Protocol):
    scheduler_id: str


def get_job_id(job: Union[str, JobWithId]):
    return getattr(job, "id", job)


def get_scheduler_id(job: Union[str, JobWithId]):
    return getattr(job, "scheduler_id", job)


def get_any_identifier(job: Union[str, JobWithId, JobWithSchedulerId]):
    if isinstance(job, str):
        return job
    if hasattr(job, "id"):
        return job.id
    if hasattr(job, "scheduler_id"):
        return job.scheduler_id
    raise ValueError(f"Job {job} has no id or scheduler_id")


def parse_timeperiod(walltime: str) -> int:
    hours, minutes, seconds = map(int, walltime.split(":"))
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def parse_datetime(datetime_str: str) -> datetime:
    try:
        return datetime.fromisoformat(datetime_str)
    except ValueError:
        return datetime.strptime(datetime_str, "%a %b %d %H:%M:%S %Y")


def parse_memory(memory: str) -> int:
    scale_map = {"gb": 1e9, "mb": 1e6, "kb": 1e3, "b": 1}
    pattern = r"^(\d+)([kmg]?b)?$"
    match = re.match(pattern, memory)
    if match:
        numeric_part = match.group(1)
        scale = match.group(2)
        if scale is None:
            multiplier = 1
        else:
            multiplier = scale_map[scale.lower()]
        return int(numeric_part) * multiplier
    raise ValueError(f"Unrecognized memory format: {memory}")


def escape_literal(literal: str) -> str:
    return literal.replace("'", "\\'").replace('"', '\\"')
