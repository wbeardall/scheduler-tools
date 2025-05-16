import json
import re
from abc import ABC, abstractmethod, abstractstaticmethod
from functools import partialmethod
from logging import Logger
from typing import Any, Dict, Union

from schedtools.core import PBSJob, Queue
from schedtools.exceptions import (
    JobDeletionError,
    JobSubmissionError,
    MissingJobScriptError,
    QueueFullError,
)
from schedtools.log import loggers
from schedtools.shell_handler import CommandHandler, LocalHandler, ShellHandler
from schedtools.utils import retry_on


class WorkloadManager(ABC):
    manager_check_cmd = None
    submit_cmd = None
    delete_cmd = None
    get_jobs_json_cmd = None

    def __init__(
        self,
        handler: Union[CommandHandler, str, Dict[str, Any]],
        logger: Union[Logger, None] = None,
    ) -> None:
        if not isinstance(handler, CommandHandler):
            handler = ShellHandler(handler)
        if logger is None:
            logger = loggers.current
        self.handler = handler
        self.logger = logger

    @abstractmethod
    def get_storage_stats(self):
        ...

    @classmethod
    @retry_on(RecursionError, max_tries=2)
    def is_valid(cls, handler: CommandHandler):
        result = handler.execute(cls.manager_check_cmd)
        if result.returncode == 0:
            return True
        elif result.returncode == 127:
            return False
        else:
            raise RuntimeError(
                f"Command `{cls.manager_check_cmd}` failed with status {result.returncode}"
            )

    def get_jobs(self):
        """Get full job information on all running / queued jobs"""
        return self.get_jobs_from_handler(self.handler)

    @classmethod
    @abstractmethod
    def get_jobs_from_handler(cls, handler: CommandHandler) -> Queue:
        ...

    def submit_job(self, jobscript_path: str):
        result = self.handler.execute(f"{self.submit_cmd} {jobscript_path}")
        if result.returncode:
            msg = f"Submission of jobscript at {jobscript_path} failed with status {result.returncode} ({result.stderr.strip()})"
            self.logger.info(msg)
            raise JobSubmissionError(msg)

    def submit_or_track(self, jobscript_path: str):
        from schedtools.jobs import track_new_jobs

        try:
            self.submit_job(jobscript_path)
        except JobSubmissionError:
            self.logger.info(f"Tracking unsubmitted job ({jobscript_path}).")
            job = PBSJob.unsubmitted(jobscript_path)
            track_new_jobs(self.handler, job, logger=self.logger)

    def delete_job(self, job: Union[str, PBSJob]):
        if isinstance(job, str):
            job_id = job
        else:
            job_id = job.id
        result = self.handler.execute(f"{self.delete_cmd} {job_id}")
        if result.returncode:
            msg = f"Deletion of job {job_id} failed with status {result.returncode} ({result.stderr.strip()})"
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

    def get_storage_stats(self):
        storage_lines = self.handler.login_message[-4:]
        # NOTE: we don't currently report ephemeral stats
        stats = {}
        section_name = None
        section = {}
        for line in storage_lines:
            stat_name, tail = line.strip().split(":")
            if len(stat_name.split()) > 1:
                if section_name is not None:
                    stats[section_name] = section
                    section = {}
                section_name, stat_name = stat_name.split()
            section[stat_name] = dict(
                used=re.findall("[0-9]+\.{0,1}[0-9]*[kMGTP](?=B{0,1} of)", tail)[0],
                total=re.findall("(?<=of )[0-9]+\.{0,1}[0-9]*[kMGTP]", tail)[0],
                percent_used=float(
                    re.findall("(?<=\()[0-9]+\.{0,1}[0-9]*(?=\%\))", tail)[0]
                ),
            )
        return stats

    def submit_job(self, jobscript_path: str):
        result = self.handler.execute(f"{self.submit_cmd} {jobscript_path}")
        if result.returncode:
            msg = f"Submission of jobscript at {jobscript_path} failed with status {result.returncode} ({result.stderr.strip()})"
            self.logger.info(msg)
            # Number of jobs exceeds user's limit
            if result.returncode == 38:
                raise QueueFullError(msg)
            raise JobSubmissionError(msg)

    @classmethod
    def get_jobs_from_handler(cls, handler: CommandHandler) -> Queue:
        if cls.get_jobs_json_cmd:
            return cls.get_jobs_from_handler_json(handler)
        else:
            return cls.get_jobs_from_handler_native(handler)

    @classmethod
    def get_jobs_from_handler_json(cls, handler: CommandHandler) -> Queue:
        result = handler.execute(cls.get_jobs_json_cmd)
        if result.returncode:
            raise RuntimeError(f"{cls.get_jobs_json_cmd} failed with returncode {result.returncode}")
        data = json.loads(result.stdout)
        jobs = []
        for id, job_data in data.items():
            jobs.append(PBSJob({**job_data, "id": id}))
        return Queue(jobs)

    @classmethod
    def get_jobs_from_handler_native(cls, handler: CommandHandler) -> Queue:
        """Get full job information on all running / queued jobs"""
        result = handler.execute("qstat -f")
        if result.returncode:
            raise RuntimeError(f"qstat failed with returncode {result.returncode}")
        data = result.stdout.split("\n")
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
                current_job = PBSJob(
                    {"id": re.findall("(?<=Job Id: )[0-9]+", line.strip())[0]}
                )
                current_key = ""
                current_indent = 0
            elif " = " in line:
                indent = line.index(" ") - len(line.lstrip("\t"))
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
                if result.returncode == 159:
                    self.logger.info(
                        "User not authorized to use `qrerun`. Attempting to requeue from jobscript."
                    )
                    self.qrerun_allowed = False
                # Number of jobs exceeds user's limit
                elif result.returncode == 38:
                    msg = f"Rerun job {job.id} ({job.name}) failed with status {result.returncode} ({result.stderr.strip()})"
                    self.logger.info(msg)
                    raise QueueFullError(msg)
            else:
                self.logger.info(f"Rerunning job {job.id}")
                return
        result = self.handler.execute(f"qsub {job.jobscript_path}")
        if result.returncode:
            msg = f"Rerun job {job.id} ({job.name}) failed with status {result.returncode} ({result.stderr.strip()})"
            if "script file:: No such" in result.stderr.strip():
                self.logger.error(msg)
                raise MissingJobScriptError(msg)
            else:
                self.logger.info(msg)
            # Number of jobs exceeds user's limit
            if result.returncode == 38:
                raise QueueFullError(msg)
            raise JobSubmissionError(msg)
        else:
            self.logger.info(f"Rerunning job {job.id} ({job.name})")

    def was_killed_reason(self, job: PBSJob, reason):
        result = self.handler.execute(f"tail {job.error_path}")
        if result.returncode:
            # TODO: Make more robust
            return False
        if f"PBS: job killed: {reason}" in result.stdout:
            return True
        return False

    was_killed_mem = partialmethod(was_killed_reason, reason="mem")
    was_killed_walltime = partialmethod(was_killed_reason, reason="walltime")


class SLURM(WorkloadManager):
    manager_check_cmd = "sinfo"
    submit_cmd = "sbatch --requeue"
    delete_cmd = "scancel"

    @staticmethod
    def get_jobs_from_handler(handler: CommandHandler):
        result = handler.execute(
            "squeue --noheader -u $USER -o %i | xargs -I {} scontrol show job {}"
        )
        raise NotImplementedError("SLURM job parsing not implemented currently.")

    def rerun_job(self, job: PBSJob):
        # TODO: implement
        # afternotok ensures job is only requeued if it failed (i.e. timed out)
        # sbatch --dependency=afternotok:<jobid> <script>
        raise NotImplementedError()


def get_workload_manager(
    handler: Union[CommandHandler, str], logger: Union[Logger, None] = None
) -> WorkloadManager:
    if not isinstance(handler, CommandHandler):
        if handler == "local":
            handler = LocalHandler()
        else:
            handler = ShellHandler(handler)
    if logger is None:
        logger = loggers.current
    for man_cls in [PBS, SLURM]:
        if man_cls.is_valid(handler):
            return man_cls(handler, logger)
    raise RuntimeError(
        "No recognised workload manager found on cluster. Valid managers are PBS, SLURM."
    )
