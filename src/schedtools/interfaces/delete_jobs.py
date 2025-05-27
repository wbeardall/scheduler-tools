import argparse
from typing import List

from schedtools.managers import get_workload_manager
from schedtools.shell_handler import LocalHandler
from schedtools.tracking import JobTrackingQueue


def delete_jobs_impl(job_ids: List[str]):
    queue = JobTrackingQueue.from_local()
    local_handler = LocalHandler()
    workload_manager = get_workload_manager(local_handler)
    scheduler_queue = workload_manager.get_jobs()
    for job_id in job_ids:
        job = queue.pull_updated(job_id)
        if job in scheduler_queue:
            workload_manager.delete_job(job)
        queue.pop(job.id)


def delete_jobs():
    parser = argparse.ArgumentParser(
        description="Delete jobs from the tracking database and scheduler queues"
    )

    parser.add_argument(
        "--job-ids",
        type=str,
        nargs="+",
        help="The IDs of the jobs to delete",
    )

    args = parser.parse_args()

    delete_jobs_impl(args.job_ids)


if __name__ == "__main__":
    delete_jobs()
