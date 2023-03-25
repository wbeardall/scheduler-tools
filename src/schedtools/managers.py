from abc import ABC, abstractmethod, abstractstaticmethod
from functools import partialmethod
from logging import Logger
import re
from typing import Union

from schedtools.core import PBSJob, Queue
from schedtools.exceptions import JobDeletionError, JobSubmissionError
from schedtools.log import loggers
from schedtools.shell_handler import ShellHandler

class WorkloadManager(ABC):
    manager_check_cmd = None
    submit_cmd = None
    delete_cmd = None
    def __init__(self, handler: Union[ShellHandler, str], logger: Union[Logger, None] = None) -> None:
        if not isinstance(handler, ShellHandler):
            handler = ShellHandler(handler)
        if logger is None:
            logger = loggers.current
        self.handler = handler
        self.logger = logger
    
    @classmethod
    def is_valid(cls,handler: ShellHandler):
        result = handler.execute(cls.manager_check_cmd)
        if result.returncode == 0:
            return True
        elif result.returncode == 127:
            return False
        else:
            raise RuntimeError(f"Command `{cls.manager_check_cmd}` failed with status {result.returncode}")

    def get_jobs(self):
        """Get full job information on all running / queued jobs"""
        return self.get_jobs_from_handler(self.handler)

    @abstractstaticmethod
    def get_jobs_from_handler(handler: ShellHandler):
        ...

    def submit_job(self, jobscript_path: str):
        result = self.handler.execute(f"{self.submit_cmd} {jobscript_path}")
        if result.returncode:
            msg = f"Submission of jobscript at {jobscript_path} failed with status {result.returncode} ({result.stderr[0].strip()})"
            self.logger.info(msg)
            raise JobSubmissionError(msg)

    def delete_job(self, job: Union[str, PBSJob]):
        if isinstance(job,str):
            job_id = job
        else:
            job_id = job.id
        result = self.handler.execute(f"{self.delete_cmd} {job_id}")
        if result.returncode:
            msg = f"Deletion of job {job_id} failed with status {result.returncode} ({result.stderr[0].strip()})"
            self.logger.info(msg)
            raise JobDeletionError(msg)
        
    def was_killed(self, job: PBSJob):
        return self.was_killed_walltime(job) or self.was_killed_mem(job)

    @abstractmethod
    def was_killed_reason(self, job: PBSJob):
        ...

    @abstractmethod
    def was_killed_mem(self, job: PBSJob):
        ...

    @abstractmethod
    def was_killed_walltime(self, job: PBSJob):
        ...

    @abstractmethod
    def rerun_job(self, job: PBSJob):
        ...

class UCL(WorkloadManager):
    # UCL cluster uses a variant of PBS, with slightly different commands
    manager_check_cmd = "jobhist"
    submit_cmd = "qsub"
    delete_cmd = "qdel"
    # TODO: Implement rest of functionality

class PBS(WorkloadManager):
    manager_check_cmd = "qstat"
    submit_cmd = "qsub"
    delete_cmd = "qdel"
    qrerun_allowed = True

    @staticmethod
    def get_jobs_from_handler(handler: ShellHandler):
        """Get full job information on all running / queued jobs"""
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
        return Queue(jobs)

    def rerun_job(self, job: PBSJob):
        if self.qrerun_allowed:
            result = self.handler.execute(f"qrerun {job.id}")

            if result.returncode:
                # Account not authorized for qrerun
                if result.returncode==159:
                    self.logger.info("User not authorized to use `qrerun`. Attempting to requeue from jobscript.")
                    self.qrerun_allowed = False
            else:
                self.logger.info(f"Rerunning job {job.id}")
                return
        result = self.handler.execute(f"qsub {job.jobscript_path}")
        if result.returncode:
            msg = f"Rerun job {job.id} failed with status {result.returncode} ({result.stderr[0].strip()})"
            self.logger.info(msg)
            # Number of jobs exceeds user's limit
            if result.returncode==38:
                pass
            raise JobSubmissionError(msg)
        else:
            self.logger.info(f"Rerunning job {job.id} ({job.name})")

    def was_killed_reason(self, job: PBSJob, reason):
        result = self.handler.execute(f"tail {job.error_path}")
        if result.returncode:
            # TODO: Make more robust
            return False
        tail = "\n".join(result.stdout)
        if f"PBS: job killed: {reason}" in tail:
            return True
        return False
        
    was_killed_mem = partialmethod(was_killed_reason,reason="mem")
    was_killed_walltime = partialmethod(was_killed_reason,reason="walltime")

class SLURM(WorkloadManager):
    manager_check_cmd = "sinfo"
    submit_cmd = "sbatch --requeue"
    delete_cmd = "scancel"

    @staticmethod
    def get_jobs_from_handler(handler: ShellHandler):
        result = handler.execute("squeue --noheader -u $USER -o %i | xargs -I {} scontrol show job {}")
        raise NotImplementedError("SLURM job parsing not implemented currently.")

    def rerun_job(self, job: PBSJob):
        # TODO: implement
        # afternotok ensures job is only requeued if it failed (i.e. timed out)
        #sbatch --dependency=afternotok:<jobid> <script>
        raise NotImplementedError()

def get_workload_manager(handler: Union[ShellHandler, str], logger: Union[Logger, None]=None):
    if not isinstance(handler, ShellHandler):
        handler = ShellHandler(handler)
    if logger is None:
        logger = loggers.current
    for man_cls in [PBS, SLURM]:
        if man_cls.is_valid(handler):
            return man_cls(handler, logger)
    raise RuntimeError("No recognised workload manager found on cluster. Valid managers are PBS, SLURM.")