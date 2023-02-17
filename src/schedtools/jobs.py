import json
from logging import Logger
from typing import Union

from schedtools.exceptions import JobSubmissionError
from schedtools.log import loggers
from schedtools.managers import get_workload_manager
from schedtools.pbs_dataclasses import PBSJob
from schedtools.shell_handler import ShellHandler

PRIORITY_RERUN_FILE = "$HOME/.priority-rerun"

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


def rerun_jobs(handler: Union[ShellHandler, str], threshold: Union[int, float]=95, logger: Union[Logger, None]=None, **kwargs):
    """Rerun PBS jobs where elapsed time is greater than threshold (%).
    
    kwargs are provided to pass e.g. passwords to the created handler instance 
    without needing them stored anywhere.
    """
    if logger is None:
        logger = loggers.current
    logger.info("Executing rerun.")
    if not isinstance(handler, ShellHandler):
        handler = ShellHandler(handler, **kwargs)

    manager = get_workload_manager(handler, logger)
    jobs = manager.get_jobs()
    
    priority_rerun = get_rerun_from_file(handler)
    priority_ids = [el["id"] for el in priority_rerun]
    new_rerun = [job for job in jobs if job.percent_completion >= threshold and not job["id"] in priority_ids]
    to_rerun = [el for chunk in [priority_rerun, new_rerun] for el in chunk]
    for_future = []
    if len(to_rerun):
        for job in to_rerun:
            try: 
                manager.rerun_job(job)
            except JobSubmissionError:
                for_future.append(job)
                
    if len(for_future):
        # Log any jobs that haven't been able to be requeued
        future_json = json.dumps({job.id:job.jobscript_path for job in for_future})
        result = handler.execute("echo '" + future_json + f"\n' > {PRIORITY_RERUN_FILE}")
        if result.returncode:
            logger.info(f"Saving priority jobs failed with status {result.returncode} ({result.stderr[0].strip()})")