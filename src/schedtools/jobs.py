import json
import re

from schedtools.log import loggers
from schedtools.pbs_dataclasses import PBSJob
from schedtools.shell_handler import ShellHandler

PRIORITY_RERUN_FILE = "$HOME/.priority-rerun"

def get_jobs(handler):
    result = handler.execute("qstat -f")
    if result.returncode:
        raise RuntimeError(f"qstat failed with returncode {result.returncode}")
    data = result.stdout
    jobs = []
    current_job = PBSJob()
    current_key = ""
    current_indent = 0
    for line in data:
        if not len(line.strip()):
            continue
        if line.startswith("Job Id: "):
            if current_job:
                jobs.append(current_job)
            current_job = PBSJob({"id": re.findall("(?<=Job Id: )[0-9]+", line.strip())[0]})
            current_key = ""
            current_indent = 0
        elif " = " in line:
            indent = line.index(" ") - len(line.lstrip('\t'))
            if indent > current_indent:
                current_job[current_key] += line.strip()
                current_indent = indent
            else:
                key, val = line.strip().split(" = ")
                current_job[key] = val
                current_key = key
                current_indent = 0
        elif current_key:
            current_job[current_key] += line.strip()
    jobs.append(current_job)
    return jobs


def get_job_percentage(handler):
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

def get_rerun_from_file(handler):
    result = handler.execute(f"cat {PRIORITY_RERUN_FILE}")
    if result.returncode:
        return []
    raw = json.loads("\n".join(result.stdout))
    return [PBSJob({"id":k, "jobscript_path":v}) for k,v in raw.items()]


def rerun_jobs(handler, threshold=95, logger=None, **kwargs):
    """Rerun PBS jobs where elapsed time is greater than threshold (%).
    
    kwargs are provided to pass e.g. passwords to the created handler instance 
    without needing them stored anywhere.
    """
    if logger is None:
        logger = loggers.current
    logger.info("Executing rerun.")
    if not isinstance(handler, ShellHandler):
        handler = ShellHandler(handler, **kwargs)
    jobs = get_jobs(handler)
    
    priority_rerun = get_rerun_from_file(handler)
    priority_ids = [el["id"] for el in priority_rerun]
    new_rerun = [job for job in jobs if job.percent_completion >= threshold and not job["id"] in priority_ids]
    to_rerun = [el for chunk in [priority_rerun, new_rerun] for el in chunk]
    for_future = []
    qrerun_auth_fail = False
    if len(to_rerun):
        for job in to_rerun:
            if not qrerun_auth_fail:
                result = handler.execute(f"qrerun {job.id}")

                if result.returncode:
                    # Account not authorized for qrerun
                    if result.returncode==159:
                        logger.info("User not authorized to use `qrerun`. Attempting to requeue from jobscript.")
                        qrerun_auth_fail = True
                    else:
                        logger.info(f"Rerun failed with status {result.returncode} ({result.stderr[0].strip()}).")
                else:
                    logger.info(f"Rerunning job {job.id}")
            if qrerun_auth_fail:
                result = handler.execute(f"qsub {job.jobscript_path}")
                if result.returncode:
                    logger.info(f"Rerun job {job.id} failed with status {result.returncode} ({result.stderr[0].strip()})")
                    # Number of jobs exceeds user's limit
                    if result.returncode==38:
                        pass
                    for_future.append(job)
                else:
                    logger.info(f"Rerunning job {job.id}")
                
    if len(for_future):
        # Log any jobs that haven't been able to be requeued
        future_json = json.dumps({job.id:job.jobscript_path for job in for_future})
        result = handler.execute("echo '" + future_json + f"\n' > {PRIORITY_RERUN_FILE}")
        if result.returncode:
            logger.info(f"Saving priority jobs failed with status {result.returncode} ({result.stderr[0].strip()})")