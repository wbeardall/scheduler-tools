import re
import subprocess
from collections import namedtuple
from typing import Union

import paramiko

from schedtools.utils import connect_to_host

SSHResult = namedtuple("SSHResult", ["stdin", "stdout", "stderr", "returncode"])


class CommandHandler:
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

    def __del__(self):
        try:
            self.ssh.close()
        except:
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
        shin = self.stdin
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
