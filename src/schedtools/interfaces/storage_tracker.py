import argparse
import atexit
import datetime
import os
import time
from logging import Logger
from typing import Any, Dict, Union

import daemon
from apscheduler.schedulers.blocking import BlockingScheduler

from schedtools.log import loggers
from schedtools.managers import get_workload_manager
from schedtools.service import make_service
from schedtools.shell_handler import ShellHandler
from schedtools.smtp import load_credentials
from schedtools.utils import connect_to_host, systemd_service


def check_storage(
    handler: Union[ShellHandler, str, Dict[str, Any]],
    threshold: Union[int, float] = 85,
    logger: Union[Logger, None] = None,
    **kwargs,
):
    """Check cluster storage and alert if usage is greater than threshold (%).

    kwargs are provided to pass e.g. passwords to the created handler instance
    without needing them stored anywhere.

    Args:
        handler: Shell handler instance, or SSH host alias or host config dictionary
        threshold: Threshold above which to trigger alerts. Defaults to 85.
        logger: Logger instance. Defaults to None.
    """
    if logger is None:
        logger = loggers.current
    try:
        logger.info("Checking storage.")
        if not isinstance(handler, ShellHandler):
            handler = ShellHandler(handler, **kwargs)

        manager = get_workload_manager(handler, logger)
        stats = manager.get_storage_stats()
        for partition, v in stats.items():
            for element, data in v.items():
                if data["percent_used"] > threshold:
                    logger.error(
                        f"{element} usage in partition '{partition}' at {data['percent_used']:.1f}% capacity ({data['used']} / {data['total']})"
                    )
    except Exception as e:
        logger.exception(e)
        raise e


def storage_tracker():  # pragma: no cover
    """Utility for tracking storage usage on a cluster, and sending email notifications when the quota is almost filled."""
    os.environ["SCHEDTOOLS_PROG"] = "storage-tracker"
    parser = argparse.ArgumentParser(description=storage_tracker.__doc__)
    parser.add_argument("host", type=str, help="Host alias in `~/.ssh/config`.")
    parser.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=85,
        help="Threshold percentage [0-100] above which to send a storage notification.",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=1.0,
        help="Interval (in days) at which to check storage usage.",
    )
    parser.add_argument(
        "-p",
        "--password",
        type=str,
        default=None,
        help="Password for host authentication.",
    )
    parser.add_argument(
        "-s",
        "--service",
        action="store_true",
        help="Register this program as a service rather than running it.",
    )
    args = parser.parse_args()
    assert args.threshold < 100, "threshold must be less than 100."

    if args.password:
        kwargs = {"password": args.password}
    elif "CLUSTER_SSH_PASSWORD" in os.environ:
        kwargs = {"password": os.environ["CLUSTER_SSH_PASSWORD"]}
    else:
        kwargs = {}

    password = connect_to_host(args.host, True, **kwargs)
    if password is not None:
        kwargs["password"] = password
    # Check that SMTP credentials are valid
    _ = load_credentials()

    if args.service:
        command = (
            f"{__file__} {args.host} -t {args.threshold:.1f} -i {args.interval:.1f}"
        )
        env_vars = {
            "SSH_CONFIG": os.path.expanduser("~/.ssh/config"),
            "SCHEDTOOLS_USER": os.environ["LOGNAME"],
            "SYSTEMD_SERVICE": "True",
        }
        if kwargs.get("password"):
            env_vars["CLUSTER_SSH_PASSWORD"] = kwargs["password"]
        make_service("storage-tracker", command, env_vars)
        # Hand over to service, so we don't need to run the rest now.
        return

    logger = loggers.current
    scheduler = BlockingScheduler()

    # Only daemonize if not being run by systemd.
    if systemd_service():
        while True:
            check_storage(
                handler=args.host, threshold=args.threshold, logger=logger, **kwargs
            )
            time.sleep(args.interval * 60 * 60 * 24)
    else:
        logger.info("Scheduler created.")
        scheduler.add_job(
            check_storage,
            "interval",
            days=args.interval,
            next_run_time=datetime.datetime.now(),
            kwargs=dict(
                handler=args.host, threshold=args.threshold, logger=logger, **kwargs
            ),
        )
        logger.info("Storage tracker task scheduled.")
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
    storage_tracker()
