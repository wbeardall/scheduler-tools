import json
import os
from logging import Logger
from typing import Any, Dict, List, Union

from paramiko import SSHClient

from schedtools.exceptions import (
    JobDeletionError,
    JobSubmissionError,
    MissingJobScriptError,
    QueueFullError,
)
from schedtools.log import loggers
from schedtools.managers import WorkloadManager, get_workload_manager
from schedtools.schemas import Job, Queue
from schedtools.shell_handler import CommandHandler, ShellHandler
from schedtools.tracking import job_tracking_queue
from schedtools.utils import systemd_service

RERUN_TRACKED_FILE = "$HOME/.rerun-tracked.json"
if systemd_service():
    CACHE_DIR = "/var/tmp/rerun-service"
else:
    CACHE_DIR = os.path.join(os.path.expanduser("~"), ".rerun")
RERUN_TRACKED_CACHE = os.path.join(CACHE_DIR, "rerun-tracked-cache.json")


def track_new_jobs(
    handler: Union[CommandHandler, str, SSHClient],
    jobs: Union[Job, List[Job]],
    logger: Union[Logger, None] = None,
):
    """Add new jobs to the tracked list.

    Useful if the scheduler queue is full, but there are still more jobs to submit.
    Effectively gives the user an infinite job queue. Concurrent running job limits naturally still apply.

    Args:
        handler: `ShellHandler` instance to use to query cluster
        jobs: job or list of jobs to track.
        logger: Logger instance. Defaults to None.

    Raises:
        RuntimeError: saving tracked jobs failed.
    """
    if not isinstance(jobs, list):
        jobs = [jobs]
    if logger is None:
        logger = loggers.current

    with job_tracking_queue(handler) as tracked:
        tracked.register(jobs)


def get_tracked_cache():
    if os.path.exists(RERUN_TRACKED_CACHE):
        with open(RERUN_TRACKED_CACHE, "r") as f:
            cached = Queue([Job(job) for job in json.load(f)])
    else:
        cached = Queue()
    return cached


def delete_queued_duplicates(
    handler: Union[CommandHandler, str, Dict[str, Any]],
    manager: Union[WorkloadManager, None] = None,
    logger: Union[Logger, None] = None,
    count_running: bool = False,
):
    """Delete duplicates of queued jobs.

    This function determines job identity by the jobscript path, because job name is
    not guaranteed unique.

    Args:
        handler: Shell handler instance, or SSH host alias or host config dictionary
        manager: Workload manager instance. Defaults to None.
        logger: Logger instance. Defaults to None.
        count_running: Include running jobs when identifying duplicates. Defaults to False.
    """
    if not isinstance(handler, CommandHandler):
        handler = ShellHandler(handler)
    if manager is None:
        manager = get_workload_manager(handler, logger or loggers.current)

    jobs = manager.get_jobs()
    waiting_scripts = []
    duplicates = Queue()
    for job in jobs:
        if not count_running and not job.is_queued:
            continue
        if job.jobscript_path in waiting_scripts:
            duplicates.append(job)
        else:
            waiting_scripts.append(job.jobscript_path)
    for job in duplicates:
        try:
            manager.delete_job(job)
        except JobDeletionError:
            pass


def rerun_jobs(
    handler: Union[CommandHandler, str, Dict[str, Any]],
    threshold: Union[int, float] = 90,
    logger: Union[Logger, None] = None,
    continue_on_rerun: bool = False,
    **kwargs,
):
    """Rerun PBS jobs where elapsed time is greater than threshold (%).

    kwargs are provided to pass e.g. passwords to the created handler instance
    without needing them stored anywhere.

    Args:
        handler: Shell handler instance, or SSH host alias or host config dictionary
        threshold: Job threshold percentage above which to rerun jobs. Defaults to 90.
        logger: Logger instance. Defaults to None.
        continue_on_rerun: Whether to continue or cancel a job upon requeuing. Defaults to False.
    """
    if logger is None:
        logger = loggers.current
    try:
        logger.info("Executing rerun.")
        if not isinstance(handler, CommandHandler):
            handler = ShellHandler(handler, **kwargs)

        manager = get_workload_manager(handler, logger)
        queued = manager.get_jobs()

        with job_tracking_queue(handler) as tracked:
            tracked.register(get_tracked_cache())
        to_rerun = Queue(
            [job for job in tracked if (job not in queued) and manager.was_killed(job)]
        )
        to_rerun.extend([job for job in queued if job.percent_completion >= threshold])
        # Remove completed jobs that no longer appear in the queue AND were not killed AND
        # were running at last register AND for which the entire runtime has elapsed.
        completed = Queue(
            [
                job
                for job in tracked
                if (job not in queued)
                and job.is_running
                and job.has_elapsed
                and not manager.was_killed(job)
            ]
        )
        for job in completed:
            logger.info(
                f"Job {job.id} ({job.name}) appears completed. Removing from tracked list."
            )
            tracked.pop(job)
        # Update list of tracked jobs
        tracked.update(queued)

        breakdown = ", ".join(
            [
                f"{tracked.count(status)} {status}"
                for status in ["running", "queued", "unsubmitted"]
            ]
        )
        logger.info(
            f"{len(to_rerun)} jobs to rerun ({len(tracked)} total tracked, {breakdown})."
        )

        for job in to_rerun:
            try:
                manager.rerun_job(job)
                # Can untrack the job
                tracked.pop(job)
                if job in queued and not continue_on_rerun:
                    manager.delete_job(job)
            except JobSubmissionError as e:
                if isinstance(e, QueueFullError):
                    break
                elif isinstance(e, MissingJobScriptError):
                    # Missing jobscript, so job can never be requeued.
                    tracked.pop(job)

        # Update the tracked job list
        tracked_json = json.dumps([job for job in tracked])
        result = handler.execute(
            "echo '" + tracked_json + f"\n' > {RERUN_TRACKED_FILE}"
        )
        if result.returncode:
            logger.info(
                f"Saving tracked jobs failed with status {result.returncode} ({result.stderr.strip()})"
            )
            os.makedirs(CACHE_DIR, exist_ok=True)
            with open(RERUN_TRACKED_CACHE, "w") as f:
                f.write(tracked_json)
            logger.info(
                f"Tracked jobs cached locally to {RERUN_TRACKED_CACHE}. They will be synced during the next job execution."
            )
        else:
            # Can safely remove the cache
            if os.path.exists(RERUN_TRACKED_CACHE):
                os.remove(RERUN_TRACKED_CACHE)
    except Exception as e:
        logger.exception(e)
        raise e
