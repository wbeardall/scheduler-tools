from logging import getLogger

from schedtools.managers import get_workload_manager
from schedtools.schemas import JobState
from schedtools.shell_handler import LocalHandler
from schedtools.tracking import JobTrackingQueue

logger = getLogger(__name__)


def set_missing_alerts():
    queue = JobTrackingQueue.from_local()
    local_handler = LocalHandler()
    workload_manager = get_workload_manager(local_handler)
    scheduler_queue = workload_manager.get_jobs()

    for job in queue.filter_state(JobState.QUEUED):
        # Ensure the job is up to date
        job = queue.pull_updated(job)
        if job not in scheduler_queue:
            logger.info(
                f"Setting alert for job {job.id} because it is not in the scheduler queue."
            )
            local_handler.update_job_state(
                job_id=job.id,
                state=JobState.ALERT,
                comment=(
                    "Job not found in scheduler queue, even though it is "
                    "marked as queued. Was it deleted, or failed without updating its own state?"
                ),
            )


if __name__ == "__main__":
    set_missing_alerts()
