from abc import ABC, abstractmethod, abstractstaticmethod
from logging import Logger
import re

from schedtools.pbs_dataclasses import PBSJob
from schedtools.exceptions import JobSubmissionError
from schedtools.log import loggers
from schedtools.shell_handler import ShellHandler

class WorkloadManager(ABC):
    manager_check_cmd = None
    def __init__(self, handler: ShellHandler, logger: Logger = None) -> None:
        if not isinstance(handler, ShellHandler):
            handler = ShellHandler(handler)
        if logger is None:
            logger = loggers.current
        self.handler = handler
        self.logger = logger
    
    @classmethod
    def is_valid(cls,handler):
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
    def get_jobs_from_handler(handler):
        ...

    @abstractmethod
    def submit_job(self, jobscript):
        ...

    @abstractmethod
    def rerun_job(self, job):
        ...

class PBS(WorkloadManager):
    manager_check_cmd = "qstat"
    qrerun_allowed = True

    @staticmethod
    def get_jobs_from_handler(handler):
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
        return jobs

    def submit_job(self, jobscript_path):
        return self.handler.execute(f"qsub {jobscript_path}")

    def rerun_job(self, job):
        if self.qrerun_allowed:
            result = self.handler.execute(f"qrerun {job.id}")

            if result.returncode:
                # Account not authorized for qrerun
                if result.returncode==159:
                    self.logger.info("User not authorized to use `qrerun`. Attempting to requeue from jobscript.")
                    self.qrerun_allowed = False
                else:
                    msg = f"Rerun failed with status {result.returncode} ({result.stderr[0].strip()})."
                    self.logger.info(msg)
                    raise JobSubmissionError(msg)
            else:
                self.logger.info(f"Rerunning job {job.id}")
        if not self.qrerun_allowed:
            result = self.handler.execute(f"qsub {job.jobscript_path}")
            if result.returncode:
                msg = f"Rerun job {job.id} failed with status {result.returncode} ({result.stderr[0].strip()})"
                self.logger.info(msg)
                # Number of jobs exceeds user's limit
                if result.returncode==38:
                    pass
                raise JobSubmissionError(msg)
            else:
                self.logger.info(f"Rerunning job {job.id}")

class SLURM(WorkloadManager):
    manager_check_cmd = "sinfo"

    @staticmethod
    def get_jobs_from_handler(handler):
        result = handler.execute('squeue -o "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R %C"')
        raise NotImplementedError("SLURM job parsing not implemented currently.")

    def submit_job(self, jobscript_path):
        return self.handler.execute(f"sbatch --requeue {jobscript_path}")

    def rerun_job(self, job):
        # TODO: implement
        # afternotok ensures job is only requeued if it failed (i.e. timed out)
        #sbatch --dependency=afternotok:<jobid> <script>
        raise NotImplementedError()

def get_workload_manager(handler):
    if not isinstance(handler, ShellHandler):
        handler = ShellHandler(handler)
    for man_cls in [PBS, SLURM]:
        if man_cls.is_valid(handler):
            return man_cls(handler)
    raise RuntimeError("No recognised workload manager found on cluster. Valid managers are PBS, SLURM.")