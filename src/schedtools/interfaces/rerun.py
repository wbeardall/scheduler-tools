import argparse
import atexit
import datetime
import os
import time
import warnings

import daemon
from apscheduler.schedulers.blocking import BlockingScheduler

from schedtools.jobs import rerun_jobs
from schedtools.log import loggers
from schedtools.service import make_service
from schedtools.utils import connect_to_host, systemd_service

EXPECTED_WALLTIME = 72
SAFE_BUFFER = 1.5


def rerun():
    """Utility for automatically rerunning jobs on clusters when they are in danger of timing out."""
    os.environ["SCHEDTOOLS_PROG"] = "rerun"
    parser = argparse.ArgumentParser(description=rerun.__doc__)
    parser.add_argument("host", type=str, help="Host alias in `~/.ssh/config`.")
    parser.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=90,
        help="Threshold percentage [0-100] above which to rerun jobs.",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=1.0,
        help="Interval (in hours) at which to check for jobs to rerun.",
    )
    parser.add_argument(
        "-p",
        "--password",
        type=str,
        default=None,
        help="Password for host authentication.",
    )
    parser.add_argument(
        "-c",
        "--continue_on_rerun",
        action="store_true",
        help="Allow running jobs to continue after requeuing. Only enable if having multiple instances "
        "of the same job isn't going to corrupt output data or duplicate results.",
    )
    parser.add_argument(
        "-s",
        "--service",
        action="store_true",
        help="Register this program as a service rather than running it.",
    )
    args = parser.parse_args()
    if (1 - (args.threshold / 100)) * EXPECTED_WALLTIME < SAFE_BUFFER * args.interval:
        threshold = (1 - SAFE_BUFFER * args.interval / EXPECTED_WALLTIME) * 100
        warnings.warn(
            f"Threshold ({args.threshold:.1f}%) unsafe for {EXPECTED_WALLTIME}h "
            f"walltime and {args.interval:.1f}h refresh interval. Setting threshold to {threshold:.1f}%."
        )
    else:
        threshold = args.threshold

    if args.password:
        kwargs = {"password": args.password}
    elif "CLUSTER_SSH_PASSWORD" in os.environ:
        kwargs = {"password": os.environ["CLUSTER_SSH_PASSWORD"]}
    else:
        kwargs = {}

    password = connect_to_host(args.host, True, **kwargs)
    if password is not None:
        kwargs["password"] = password

    if args.service:
        command = f"{__file__} {args.host} -t {threshold:.1f} -i {args.interval:.1f}"
        if args.continue_on_rerun:
            command += " -c"
        env_vars = {
            "SSH_CONFIG": os.path.expanduser("~/.ssh/config"),
            "SCHEDTOOLS_USER": os.environ["LOGNAME"],
            "SYSTEMD_SERVICE": "True",
        }
        if kwargs.get("password"):
            env_vars["CLUSTER_SSH_PASSWORD"] = kwargs["password"]
        make_service("rerun", command, env_vars)
        # Hand over to service, so we don't need to run the rest now.
        return

    logger = loggers.current
    scheduler = BlockingScheduler()

    # Only daemonize if not being run by systemd.
    if systemd_service():
        while True:
            rerun_jobs(
                handler=args.host,
                threshold=threshold,
                logger=logger,
                continue_on_rerun=args.continue_on_rerun,
                **kwargs,
            )
            time.sleep(args.interval * 3600)
    else:
        logger.info("Scheduler created.")
        scheduler.add_job(
            rerun_jobs,
            "interval",
            hours=args.interval,
            next_run_time=datetime.datetime.now(),
            kwargs=dict(
                handler=args.host,
                threshold=threshold,
                logger=logger,
                continue_on_rerun=args.continue_on_rerun,
                **kwargs,
            ),
        )
        logger.info("Rerun task scheduled.")
        # Wrap in DaemonContext to prevent exit after logout
        with daemon.DaemonContext(
            files_preserve=[
                handler.stream
                for handler in logger.handlers
                if hasattr(handler, "stream")
            ]
        ):
            # Clean up upon script exit
            atexit.register(lambda: scheduler.shutdown())
            scheduler.start()


if __name__ == "__main__":
    rerun()
