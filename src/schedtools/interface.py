import argparse
import atexit
from functools import partial
import warnings

import daemon
from apscheduler.schedulers.background import BackgroundScheduler

from schedtools.jobs import rerun_jobs
from schedtools.utils import connect_to_host

EXPECTED_WALLTIME = 72
SAFE_BUFFER = 1.5

def rerun():
    parser = argparse.ArgumentParser()
    parser.add_argument("host",type=str,
        help="Host alias in `~/.ssh/config`.")
    parser.add_argument("-t","--threshold",type=float,default=90,
        help="Threshold percentage [0-100] above which to rerun jobs.")
    parser.add_argument("-i","--interval",type=float,default=1.,
        help="Interval (in hours) at which to check for jobs to rerun.")
    parser.add_argument("-p","--password",type=str,default=None,
        help="Password for host authentication.")
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

    scheduler = BackgroundScheduler()
    scheduler.add_job(partial(rerun_jobs,handler=args.host,threshold=threshold,**kwargs), 'interval', hours=args.interval)
    
    # Wrap in DaemonContext to prevent exit after logout
    with daemon.DaemonContext():
        scheduler.start()
        # Clean up upon script exit
        atexit.register(lambda: scheduler.shutdown())

