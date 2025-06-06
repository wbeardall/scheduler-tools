import argparse
import csv
import os
import platform
import subprocess
import tempfile
import uuid

from schedtools.managers import get_workload_manager
from schedtools.shell_handler import ShellHandler


def open_with_default_prog(file_path):
    if platform.system() == "Windows":
        command = "start"
    elif platform.system() == "Darwin":  # macOS
        command = "open"
    else:  # Linux
        command = "xdg-open"

    subprocess.run([command, file_path])


def queue_status():
    """Get status of queued jobs on cluster in spreadsheet form."""
    os.environ["SCHEDTOOLS_PROG"] = "queue-status"
    parser = argparse.ArgumentParser(description=queue_status.__doc__)
    parser.add_argument("host", type=str, help="Host alias in `~/.ssh/config`.")
    parser.add_argument("--owner", type=str, help="Only show jobs owned by this user")

    args = parser.parse_args()

    handler = ShellHandler(args.host)
    manager = get_workload_manager(handler)
    queued = manager.get_jobs()
    if args.owner:
        queued = queued.filter_owner(args.owner)

    d = [dict(j) for j in queued]
    if not len(d):
        print("No jobs to show")
        return
    file_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid1())) + ".csv"
    with open(file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=d[0].keys())
        writer.writeheader()
        writer.writerows(d)
    open_with_default_prog(file_path)


if __name__ == "__main__":
    queue_status()
