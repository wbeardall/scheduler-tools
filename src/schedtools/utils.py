import datetime
import os
from getpass import getpass

import paramiko

def connect_to_host(host_alias, get_password=False, **kwargs):
    """Connect to an SSH host using an alias defined in `~/.ssh/config`.

    Args:
        host_alias: Alias for host to connect to.
    """
    ssh = paramiko.SSHConfig()
    user_config_file = os.environ.get("SSH_CONFIG",os.path.expanduser("~/.ssh/config"))
    if os.path.exists(user_config_file):
        with open(user_config_file) as f:
            ssh.parse(f)

    host_config = ssh.lookup(host_alias)
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
            password=password
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

def log_timestamp(file, msg):
    with open(file, "a") as f:
        f.write(f"{msg} ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n")
    