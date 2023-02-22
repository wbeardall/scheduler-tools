import json
from logging import Logger
from typing import Union

from schedtools.exceptions import JobDeletionError, JobSubmissionError
from schedtools.log import loggers
from schedtools.managers import get_workload_manager, WorkloadManager
from schedtools.pbs_dataclasses import PBSJob
from schedtools.shell_handler import ShellHandler

PRIORITY_RERUN_FILE = "$HOME/.priority-rerun"
SUCCESS_RERUN_FILE = "$HOME/.success-rerun"

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

def get_rerun_from_file(handler: ShellHandler):
    f"""Get priority rerun jobs from a cached rerun queue file on the cluster.

    Expects any priority jobs to be stored in `{PRIORITY_RERUN_FILE}` (json-formatted)
    
    Args:
        handler: `ShellHandler` instance to use to query cluster
    """
    result = handler.execute(f"cat {PRIORITY_RERUN_FILE}")
    if result.returncode:
        return []
    raw = json.loads("\n".join(result.stdout))
    return [PBSJob({"id":k, "jobscript_path":v}) for k,v in raw.items()]

def get_success_from_file(handler: ShellHandler):
    f"""Get successful rerun list from file

    Expects any successfully rerun jobs to be stored in `{SUCCESS_RERUN_FILE}` (json-formatted)
    
    Args:
        handler: `ShellHandler` instance to use to query cluster
    """
    result = handler.execute(f"cat {SUCCESS_RERUN_FILE}")
    if result.returncode:
        return []
    return json.loads("\n".join(result.stdout))

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


def rerun_jobs(handler: Union[ShellHandler, str], threshold: Union[int, float]=95, logger: Union[Logger, None]=None, **kwargs):
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
    jobs = manager.get_jobs()
    
    priority_rerun = get_rerun_from_file(handler)
    to_skip = get_success_from_file(handler)
    priority_ids = [el["id"] for el in priority_rerun]
    new_rerun = [job for job in jobs if job.percent_completion >= threshold and not job.id in priority_ids]
    to_rerun = [el for chunk in [priority_rerun, new_rerun] for el in chunk]
    succeeded = []
    for skip in to_skip:
        for i, job in enumerate(to_rerun):
            # Add to succeeded to ensure that the job isn't requeued next call.
            # If the job wasn't going to be rerun, we don't need to save the job for
            # skipping in future.
            if job.id == skip:
                succeeded.append(to_rerun.pop(i))
                break


    failed = []
    if len(to_rerun):
        for job in to_rerun:
            try: 
                manager.rerun_job(job)
                succeeded.append(job)
            except JobSubmissionError:
                failed.append(job)
                

    if len(succeeded):
        # Log any jobs that haven't been able to be requeued
        success_json = json.dumps([job.id for job in succeeded])
        result = handler.execute("echo '" + success_json + f"\n' > {SUCCESS_RERUN_FILE}")
        if result.returncode:
            logger.info(f"Saving successful jobs failed with status {result.returncode} ({result.stderr[0].strip()})")
    if len(failed):
        # Log any jobs that haven't been able to be requeued
        failed_json = json.dumps({job.id:job.jobscript_path for job in failed})
        result = handler.execute("echo '" + failed_json + f"\n' > {PRIORITY_RERUN_FILE}")
        if result.returncode:
            logger.info(f"Saving priority jobs failed with status {result.returncode} ({result.stderr[0].strip()})")