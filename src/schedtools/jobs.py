import json
import os
from logging import Logger
from typing import Union

from schedtools.exceptions import JobDeletionError, JobSubmissionError
from schedtools.log import loggers
from schedtools.managers import get_workload_manager, WorkloadManager
from schedtools.pbs_dataclasses import PBSJob, Queue
from schedtools.shell_handler import ShellHandler
from schedtools.utils import systemd_service

RERUN_TRACKED_FILE = "$HOME/.rerun-tracked.json"
if systemd_service():
    CACHE_DIR = "/var/tmp/rerun-service"
else:
    CACHE_DIR = os.path.join(os.path.expanduser("~"),".rerun")
RERUN_TRACKED_CACHE = os.path.join(CACHE_DIR,"rerun-tracked-cache.json")

def get_job_percentage(handler: ShellHandler):
    """Get percentage completion of current running / queued jobs.
    
    Queries the cluster using `qstat -p`, and so does not return full job information.
    Jobs are returned as a dict of job_id: percentage pairs

    Args:
        handler: `ShellHandler` instance to use to query cluster
    """
    result = handler.execute("qstat -p")
    if result.returncode:
        raise RuntimeError(f"qstat failed with returncode {result.returncode}")
    data = result.stdout
    # Trim any junk from the top of the file
    for start_line in range(len(data)):
        if tuple(data[0].strip().split())==('Job', 'id', 'Name', 'User', '%', 'done', 'S', 'Queue'):
            break
    data = data[start_line:]

    running_jobs = {}
    for i in range(2, len(data)):
        line = data[i].strip().split()
        if len(line):
            id_ = line[0][:-4]
            pc = line[-3]
            if pc != '(null)':
                running_jobs[id_] = float(pc.replace("%", ""))
    return running_jobs

def get_tracked_from_file(handler: ShellHandler):
    f"""Get tracked job list from file

    Expects tracked jobs to be stored in `{RERUN_TRACKED_FILE}` (json-formatted)
    
    Args:
        handler: `ShellHandler` instance to use to query cluster
    """
    result = handler.execute(f"cat {RERUN_TRACKED_FILE}")
    if result.returncode or not len(result.stdout):
        return Queue([])
    raw = json.loads("\n".join(result.stdout))
    return Queue([PBSJob(job) for job in raw])

def get_tracked_cache():
    if os.path.exists(RERUN_TRACKED_CACHE):
        with open(RERUN_TRACKED_CACHE, "r") as f:
            cached = Queue([PBSJob(job) for job in json.load(f)])
    else:
        cached = Queue([])
    return cached

def delete_queued_duplicates(
    handler: Union[ShellHandler, str], 
    manager: Union[WorkloadManager,None] = None,
    logger: Union[Logger, None] = None,
    count_running: bool = False):
    """Delete duplicates of queued jobs.
    
    Args:
        handler: Shell handler instance, or SSH host alias
        manager: Workload manager instance (defaults to `None`)
        logger: Logger instance (defaults to `None`)
        count_running: Include running jobs when identifying duplicates
    """
    if isinstance(handler, str):
        handler = ShellHandler(handler)
    if manager is None:
        manager = get_workload_manager(handler, logger or loggers.current)
    jobs = manager.get_jobs()
    queued_names = []
    duplicates = []
    for job in jobs:
        if not count_running and not job.is_queued:
            continue
        if job["Job_Name"] in queued_names:
            duplicates.append(job)
        else:
            queued_names.append(job["Job_Name"])
    for job in duplicates:
        try:
            manager.delete_job(job.id)
        except JobDeletionError:
            pass


def rerun_jobs(handler: Union[ShellHandler, str], threshold: Union[int, float]=95, logger: Union[Logger, None]=None, 
               continue_on_rerun: bool= False, **kwargs):
    """Rerun PBS jobs where elapsed time is greater than threshold (%).
    
    kwargs are provided to pass e.g. passwords to the created handler instance 
    without needing them stored anywhere.

    Args:

    """
    if logger is None:
        logger = loggers.current
    logger.info("Executing rerun.")
    if not isinstance(handler, ShellHandler):
        handler = ShellHandler(handler, **kwargs)

    manager = get_workload_manager(handler, logger)
    queued = manager.get_jobs()
    
    tracked = get_tracked_from_file(handler)
    
    tracked.update(get_tracked_cache())
    to_rerun = Queue([job for job in tracked if (job not in queued) and manager.was_killed(job)])
    to_rerun.extend([job for job in queued if job.percent_completion >= threshold])
    # Update list of tracked jobs
    tracked.update(queued)

    logger.info(f"{len(to_rerun)} jobs to rerun ({len(queued)} in queue).")

    for job in to_rerun:
        try: 
            manager.rerun_job(job)
            # Can untrack the job
            tracked.pop(job)
            if job in queued and not continue_on_rerun:
                manager.delete_job(job)
        except JobSubmissionError:
            pass
                
    # Update the tracked job list
    tracked_json = json.dumps([job for job in tracked])
    result = handler.execute("echo '" + tracked_json + f"\n' > {RERUN_TRACKED_FILE}")
    if result.returncode:
        logger.info(f"Saving tracked jobs failed with status {result.returncode} ({result.stderr[0].strip()})")
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(RERUN_TRACKED_CACHE,"w") as f:
            f.write(tracked_json)
        logger.info(f"Tracked jobs cached locally to {RERUN_TRACKED_CACHE}. They will be synced during the next job execution.")
    else:
        # Can safely remove the cache
        os.remove(RERUN_TRACKED_CACHE)