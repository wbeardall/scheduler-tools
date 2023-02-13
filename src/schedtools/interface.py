import argparse
import atexit
import os
import warnings
import time

import daemon
from apscheduler.schedulers.blocking import BlockingScheduler

from schedtools.jobs import rerun_jobs
from schedtools.log import loggers
from schedtools.service import make_service
from schedtools.utils import connect_to_host

EXPECTED_WALLTIME = 72
SAFE_BUFFER = 1.5

def rerun():
    os.environ["SCHEDTOOLS_PROG"] = "rerun"
    parser = argparse.ArgumentParser()
    parser.add_argument("host",type=str,
        help="Host alias in `~/.ssh/config`.")
    parser.add_argument("-t","--threshold",type=float,default=90,
        help="Threshold percentage [0-100] above which to rerun jobs.")
    parser.add_argument("-i","--interval",type=float,default=1.,
        help="Interval (in hours) at which to check for jobs to rerun.")
    parser.add_argument("-p","--password",type=str,default=None,
        help="Password for host authentication.")
    parser.add_argument("-s","--service",action="store_true",
        help="Register this program as a service rather than running it.")
    args = parser.parse_args()
    if (1 - (args.threshold / 100)) * EXPECTED_WALLTIME < SAFE_BUFFER * args.interval:
        threshold =  (1 - SAFE_BUFFER * args.interval / EXPECTED_WALLTIME) * 100
        warnings.warn(f"Threshold ({args.threshold:.1f}%) unsafe for {EXPECTED_WALLTIME}h "
            f"walltime and {args.interval:.1f}h refresh interval. Setting threshold to {threshold:.1f}%.")
    else:
        threshold = args.threshold

    if args.password:
        kwargs = {"password":args.password}
    else:
        kwargs = {}

    password = connect_to_host(args.host, True, **kwargs)
    if password is not None:
        kwargs["password"] = password

    if args.service:
        command = f"{__file__} {args.host} -t {threshold:.1f} -i {args.interval:.1f}"
        if kwargs.get("password"):
            command += " -p " + kwargs["password"]
        make_service("rerun", command, {
            "SSH_CONFIG":os.path.expanduser("~/.ssh/config"),
            "SCHEDTOOLS_USER":os.environ["LOGNAME"],
            })
        # Hand over to service, so we don't need to run the rest now.
        return 

    logger = loggers.current
    scheduler = BlockingScheduler()
    logger.info("Scheduler created.")
    scheduler.add_job(rerun_jobs, 'interval', hours=args.interval,
        kwargs=dict(handler=args.host,threshold=threshold,logger=logger,**kwargs))
    logger.info("Rerun task scheduled.")
    # Wrap in DaemonContext to prevent exit after logout
    with daemon.DaemonContext(files_preserve=[handler.stream for handler in logger.handlers if hasattr(handler,"stream")]):
        # Clean up upon script exit
        atexit.register(lambda: scheduler.shutdown())
        scheduler.start()

if __name__=="__main__":
    rerun()