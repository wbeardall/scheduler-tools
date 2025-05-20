import json
import re
from abc import ABC, abstractmethod
from functools import partialmethod
from logging import Logger
from typing import Any, Dict, List, Union

from schedtools.exceptions import (
    JobDeletionError,
    JobSubmissionError,
    MissingJobScriptError,
    QueueFullError,
)
from schedtools.log import loggers
from schedtools.schemas import Job, Queue
from schedtools.shell_handler import CommandHandler, LocalHandler, ShellHandler
from schedtools.utils import get_job_id, retry_on


class WorkloadManager(ABC):
    list_jobs_cmd = None
    submit_cmd = None
    delete_cmd = None
    list_jobs_cmd = None

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
    def get_storage_stats(self): ...

    def query_jobs(self, job_ids: Union[str, List[str]]) -> Queue:
        return self.get_jobs_from_handler(self.handler).filter_id(job_ids)

    @classmethod
    @retry_on(RecursionError, max_tries=2)
    def is_valid(cls, handler: CommandHandler):
        result = handler.execute(cls.list_jobs_cmd)
        if result.returncode == 0:
            return True
        elif result.returncode == 127:
            return False
        else:
            raise RuntimeError(
                f"Command `{cls.list_jobs_cmd}` failed with status {result.returncode}"
            )

    def get_jobs(self):
        """Get full job information on all running / queued jobs"""
        return self.get_jobs_from_handler(self.handler)

    @classmethod
    @abstractmethod
    def get_jobs_from_handler(cls, handler: CommandHandler) -> Queue: ...

    def submit_job(
        self,
        jobscript_path: str,
        *,
        express_queue: Union[str, None] = None,
        project: Union[str, None] = None,
    ):
        args = []
        if project is not None:
            # If project is set, default to `express` queue.
            # NOTE: This is correct for ICL CX3 Phase I. It should be changed
            # for other clusters.
            express_queue = express_queue or "express"
            args.extend(["-q", express_queue, "-P", project])

        result = self.handler.execute(
            f"{self.submit_cmd} {' '.join(args)} {jobscript_path}"
        )
        if result.returncode:
            msg = f"Submission of jobscript at {jobscript_path} failed with status {result.returncode} ({result.stderr.strip()})"
            self.logger.error(msg)
            raise JobSubmissionError(msg)

    @abstractmethod
    def elevate_job(
        self,
        job: Job,
        *,
        express_queue: Union[str, None] = None,
        project: Union[str, None] = None,
    ) -> None: ...

    def submit_or_track(
        self,
        jobscript_path: str,
        *,
        express_queue: Union[str, None] = None,
        project: Union[str, None] = None,
    ):
        from schedtools.jobs import track_new_jobs

        try:
            self.submit_job(
                jobscript_path, express_queue=express_queue, project=project
            )
        except JobSubmissionError:
            self.logger.warning(
                f"Failed to submit job, adding to tracked set ({jobscript_path})."
            )
            job = Job.unsubmitted(jobscript_path)
            track_new_jobs(self.handler, job, logger=self.logger)

    def delete_job(self, job: Union[str, Job]):
        job_id = get_job_id(job)
        result = self.handler.execute(f"{self.delete_cmd} {job_id}")
        if result.returncode:
            msg = f"Deletion of job {job_id} failed with status {result.returncode} ({result.stderr.strip()})"
            self.logger.error(msg)
            raise JobDeletionError(msg)

    def was_killed(self, job: Job):
        return self.was_killed_walltime(job) or self.was_killed_mem(job)

    @abstractmethod
    def was_killed_reason(self, job: Job): ...

    @abstractmethod
    def was_killed_mem(self, job: Job): ...

    @abstractmethod
    def was_killed_walltime(self, job: Job): ...

    @abstractmethod
    def rerun_job(self, job: Job): ...


class UCL(WorkloadManager):
    # UCL cluster uses a variant of PBS, with slightly different commands
    list_jobs_cmd = "jobhist"
    submit_cmd = "qsub"
    delete_cmd = "qdel"
    # TODO: Implement rest of functionality


class PBS(WorkloadManager):
    list_jobs_cmd = "qstat -fF json"
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

    def submit_job(
        self,
        jobscript_path: str,
        *,
        express_queue: Union[str, None] = None,
        project: Union[str, None] = None,
    ):
        args = []
        if project is not None:
            # If project is set, default to `express` queue.
            # NOTE: This is correct for ICL CX3 Phase I. It should be changed
            # for other clusters.
            express_queue = express_queue or "express"
            args.extend(["-q", express_queue, "-P", project])

        result = self.handler.execute(
            f"{self.submit_cmd} {' '.join(args)} {jobscript_path}"
        )
        if result.returncode:
            msg = f"Submission of jobscript at {jobscript_path} failed with status {result.returncode} ({result.stderr.strip()})"
            self.logger.error(msg)
            # Number of jobs exceeds user's limit
            if result.returncode == 38:
                raise QueueFullError(msg)
            raise JobSubmissionError(msg)

    def elevate_job(
        self,
        job: Job,
        *,
        express_queue: Union[str, None] = None,
        project: Union[str, None] = None,
    ) -> None:
        current_status = job.status
        if current_status != "queued":
            raise ValueError(f"Cannot elevate job in status {current_status}")
        try:
            self.submit_job(
                job.jobscript_path, express_queue=express_queue, project=project
            )
            self.logger.info(
                f"Elevated job {job.id} to queue '{express_queue}' with project '{project}'. Deleting original job."
            )
            self.delete_job(job)
        except Exception as e:
            self.logger.error(
                f"Failed to elevate job {job.id} to queue '{express_queue}' with project '{project}': {e}"
            )

    def query_jobs(self, job_ids: Union[str, List[str]]) -> Queue:
        if isinstance(job_ids, str):
            job_ids = [job_ids]
        result = self.handler.execute(f"{self.list_jobs_cmd} {' '.join(job_ids)}")
        if result.returncode:
            raise RuntimeError(
                f"{self.list_jobs_cmd} failed with returncode {result.returncode}"
            )
        d = json.loads(result.stdout)
        return Queue.parse(d["Jobs"])

    @classmethod
    def get_jobs_from_handler(cls, handler: CommandHandler) -> Queue:
        result = handler.execute(cls.list_jobs_cmd)
        if result.returncode:
            raise RuntimeError(
                f"{cls.list_jobs_cmd} failed with returncode {result.returncode}"
            )
        data = json.loads(result.stdout)
        return Queue.parse(data.get("Jobs", {}))

    def rerun_job(self, job: Job):
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

    def was_killed_reason(self, job: Job, reason):
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
    list_jobs_cmd = "sinfo"
    submit_cmd = "sbatch --requeue"
    delete_cmd = "scancel"

    @staticmethod
    def get_jobs_from_handler(handler: CommandHandler):
        result = handler.execute(
            "squeue --noheader -u $USER -o %i | xargs -I {} scontrol show job {}"
        )
        raise NotImplementedError("SLURM job parsing not implemented currently.")

    def rerun_job(self, job: Job):
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
