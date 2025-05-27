import os
import re
import subprocess
from abc import ABC, abstractmethod
from collections import namedtuple
from enum import Enum
from functools import cached_property
from typing import Optional, Union

import paramiko

from schedtools.clusters import Cluster
from schedtools.interfaces import set_missing_alerts
from schedtools.schemas import JobState
from schedtools.sql import update_job_state
from schedtools.utils import connect_to_host, escape_literal

SSHResult = namedtuple("SSHResult", ["stdin", "stdout", "stderr", "returncode"])

VENV_BIN = os.path.join(".schedtools", ".venv", "bin")
UPDATE_JOB_STATE = os.path.join(VENV_BIN, "update-job-state")
SET_MISSING_ALERTS = os.path.join(VENV_BIN, "set-missing-alerts")


class CommandHandler(ABC):
    @abstractmethod
    def execute(self, cmd: str, unformat: bool = False) -> SSHResult:
        pass


class LocalHandler(CommandHandler):
    """Thin wrapper of `subprocess.run` to allow for local use of `schedtools.managers.WorkloadManager` objects."""

    login_message = []

    def execute(self, cmd: str, unformat: bool = False):
        """

        Examples:
            >>> execute('ls')
            >>> execute('finger')
            >>> execute('cd folder_name')

        Args:
            cmd: the command to be executed on the remote computer
            unformat: remove formatting special characters from output
        """
        result = subprocess.run(cmd, capture_output=True, shell=True, text=True)
        return SSHResult(
            stdin=cmd,
            stdout=result.stdout,
            stderr=result.stderr,
            returncode=result.returncode,
        )

    def open_file(self, path: str, mode: str = "r"):
        return open(path, mode)

    def update_job_state(
        self,
        *,
        job_id: str,
        state: JobState,
        comment: Optional[str] = None,
        on_fail: str = "raise",
    ):
        update_job_state(
            state=state,
            comment=comment,
            job_id=job_id,
            on_fail=on_fail,
        )

    def set_missing_alerts(self):
        set_missing_alerts()


class ShellHandler(CommandHandler):
    def __init__(self, ssh: Union[paramiko.SSHClient, str], **kwargs):
        if not isinstance(ssh, paramiko.SSHClient):
            ssh = connect_to_host(ssh, **kwargs)
        self.ssh = ssh
        channel = self.ssh.invoke_shell()
        self.stdin = channel.makefile("wb")
        self.stdout = channel.makefile("r")
        # Execute a dummy command to clear any login-related shell junk
        self.login_message = [
            el for el in self.execute("echo").stdout.split("\n") if len(el)
        ][:-1]

    def open_file(self, path: str, mode: str = "r"):
        file = self.ssh.open_sftp().open(path, mode)
        file.prefetch()
        return file

    def __del__(self):
        try:
            self.ssh.close()
        except Exception:
            pass

    def close(self):
        self.ssh.close()

    def execute(self, cmd: str, unformat: bool = False):
        """Execute a command remotely.

        Examples:
            >>> execute('ls')
            >>> execute('finger')
            >>> execute('cd folder_name')

        Args:
            cmd: the command to be executed on the remote computer
            unformat: remove formatting special characters from output
        """
        if not len(cmd):
            raise ValueError("Cannot execute empty command.")
        cmd = cmd.strip("\n")
        self.stdin.write(cmd + "\n")
        finish = "end of stdOUT buffer. finished with exit status"
        echo_cmd = "echo {} $?".format(finish)
        self.stdin.write(echo_cmd + "\n")
        self.stdin.flush()

        shout = []
        sherr = []
        exit_status = 0
        for line in self.stdout:
            if str(line).startswith(cmd) or str(line).startswith(echo_cmd):
                # up for now filled with shell junk from stdin
                shout = []
            elif str(line).startswith(finish):
                # our finish command ends with the exit status
                exit_status = int(str(line).rsplit(maxsplit=1)[1])
                if exit_status:
                    # stderr is combined with stdout.
                    # thus, swap sherr with shout in a case of failure.
                    sherr = shout
                    shout = []
                break
            else:
                if unformat:
                    # get rid of 'coloring and formatting' special characters
                    line = re.compile(r"(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]").sub("", line)
                shout.append(line.replace("\b", "").replace("\r", ""))

        # first and last lines of shout/sherr contain a prompt
        if shout and echo_cmd in shout[-1]:
            shout.pop()
        if shout and cmd in shout[0]:
            shout.pop(0)
        if sherr and echo_cmd in sherr[-1]:
            sherr.pop()
        if sherr and cmd in sherr[0]:
            sherr.pop(0)

        return SSHResult(cmd, "\n".join(shout), "\n".join(sherr), exit_status)

    def update_job_state(
        self,
        *,
        job_id: str,
        state: Union[JobState, str],
        comment: Optional[str] = None,
        on_fail: str = "raise",
    ):
        if isinstance(state, Enum):
            state = state.value
        if comment:
            comment_arg = f"--comment {escape_literal(comment)}"
        else:
            comment_arg = ""
        return self.execute(
            f"$HOME/{UPDATE_JOB_STATE} --job-id {job_id} --state {state} {comment_arg} --on-fail {on_fail}"
        )

    def set_missing_alerts(self):
        return self.execute(f"$HOME/{SET_MISSING_ALERTS}")

    @cached_property
    def cluster(self) -> Cluster:
        return Cluster.from_handler(self)
