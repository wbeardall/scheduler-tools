import argparse
import os

from schedtools.jobs import delete_queued_duplicates
from schedtools.log import loggers
from schedtools.shell_handler import ShellHandler
from schedtools.utils import connect_to_host

def delete_duplicate_jobs():
    """Delete duplicate jobs on a cluster."""
    parser = argparse.ArgumentParser(description=delete_duplicate_jobs.__doc__)
    parser.add_argument("host",type=str,
        help="Host alias in `~/.ssh/config`.")
    parser.add_argument("-p","--password",type=str,default=None,
        help="Password for host authentication.")
    parser.add_argument("-r","--running",action="store_true",
        help="Consider running jobs when determining duplicate jobs.")

    args = parser.parse_args()

    if args.password:
        kwargs = {"password":args.password}
    elif "CLUSTER_SSH_PASSWORD" in os.environ:
        kwargs = {"password":os.environ["CLUSTER_SSH_PASSWORD"]}
    else:
        kwargs = {}

    password = connect_to_host(args.host, True, **kwargs)
    if password is not None:
        kwargs["password"] = password

    delete_queued_duplicates(ShellHandler(args.host, **kwargs),logger=loggers.current,count_running=args.running)

if __name__=="__main__":
    delete_duplicate_jobs()