from collections.abc import Iterable
from functools import wraps
import os
from getpass import getpass
import re
import subprocess
import traceback
import warnings

import paramiko

def connect_to_host(host_alias, get_password=False, **kwargs):
    """Connect to an SSH host using an alias defined in `~/.ssh/config`.

    Args:
        host_alias: Alias for host to connect to.
    """
    if isinstance(host_alias,str):
        ssh = paramiko.SSHConfig()
        user_config_file = os.environ.get("SSH_CONFIG",os.path.expanduser("~/.ssh/config"))
        if os.path.exists(user_config_file):
            with open(user_config_file) as f:
                ssh.parse(f)

        host_config = ssh.lookup(host_alias)
    else:
        assert isinstance(host_alias,dict)
        host_config = host_alias
    
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh_client.connect(
            hostname=host_config["hostname"],
            username=host_config["user"],
            port=int(host_config.get("port", 22)),
            key_filename=host_config.get("identityfile", None),
            **kwargs
        )
        password = None
    except paramiko.ssh_exception.PasswordRequiredException:
        password = getpass(prompt="Password for {}@{}: ".format(host_config["user"], host_config["hostname"]))
        ssh_client.connect(
            hostname=host_config["hostname"],
            username=host_config["user"],
            port=int(host_config.get("port", 22)),
            password=password,
            # We already have the password, so don't look for keys or talk to the agent
            allow_agent=False,
            look_for_keys=False
        )
    if get_password:
        return password
    return ssh_client

def walltime_to(walltime, period="h"):
    assert period in ["s","m","h"]
    walltime = [int(el) for el in walltime.split(":")]
    assert len(walltime) == 3
    s = walltime[0]*60*60 + walltime[1]*60 + walltime[2]
    if period == "s":
        return s
    elif period == "m":
        return s/60
    else:
        return s / 3600

def memory_to(memory, scale="MB"):
    scale_map = {
        "gb":1000,
        "mb":1,
        "":1
    }
    pattern = r"(\d+)([A-Za-z]{0,2})"
    match = re.match(pattern, memory)
    if match:
        numeric_part = match.group(1)
        alpha_part = match.group(2)
        return int(numeric_part)*scale_map[alpha_part.lower()] / scale_map[scale.lower()]
    else:
        raise ValueError(f"Unrecognized memory format: {memory}")

def journald_active():
    return not subprocess.run("systemctl is-active --quiet systemd-journald".split()).returncode

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
        return RevDict({v:k for k,v in self.items()})
    
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
        allow = lambda x: True
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