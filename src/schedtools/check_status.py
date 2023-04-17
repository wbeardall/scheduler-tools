import argparse
import os
import shutil
import subprocess
import warnings

from schedtools.service import get_service_file
from schedtools.shell_handler import ShellHandler

def _check_status_local(services, show_logs):
    registered = [el for el in services if os.path.exists(get_service_file(el))]
    if len(registered)==0:
        print("No `schedtools` services are registered with `systemd`.")
        return
    print("The following services are registered: \n" + "\n".join(registered))
    terminal_width, _ = shutil.get_terminal_size()
    print("_"*terminal_width)
    prefix = ["sudo"] if show_logs else []
    for service in registered:
        subprocess.call(prefix + f"systemctl status {service}.service".split())
        print("_"*terminal_width)

def _check_status_remote(host, services, show_logs):
    if show_logs:
        warnings.warn("show-logs is unused on remote hosts due to the requirement for elevated privileges.")
    handler = ShellHandler(host)
    registered = [el for el in services if not handler.execute(f"test -f {get_service_file(el)}").returncode]
    if len(registered)==0:
        print(f"No `schedtools` services are registered with `systemd` on host {host}.")
        return
    print(f"The following services are registered on host {host}: \n" + "\n".join(registered))
    terminal_width, _ = shutil.get_terminal_size()
    print("_"*terminal_width)
    prefix = "sudo " if show_logs else ""
    # Disable the default systemd pager to prevent hangs
    handler.execute("export SYSTEMD_LESS=-FXR\n")
    for service in registered:
        handler.execute("echo")
        result = handler.execute(f"systemctl status {service}.service")
        for line in result.stdout:
            print(line)
        print("_"*terminal_width)

def check_status():
    """Utility for checking the status of `schedtools` programs registered as services locally or on a remote machine."""
    parser = argparse.ArgumentParser(description=check_status.__doc__)
    parser.add_argument("host",default=None,nargs="?",help="Optional host on which to check status. "
                        "Checks locally if not provided.")
    parser.add_argument("-l", "--show-logs",action="store_true",
                        help="Display `journalctl` logs for each service. Requires elevated privileges, and so is "
                        "ignored if checking status on a remote machine.")
    services = ["rerun", "storage-tracker"]
    args = parser.parse_args()
    if args.host is None:
        _check_status_local(services, args.show_logs)
    else:
        _check_status_remote(args.host,services, args.show_logs)
    

if __name__=="__main__":
    check_status()