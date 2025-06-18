import multiprocessing as mp
import os

from schedtools.compression import zip_experiment
from schedtools.interfaces.tasks.task import queue_task
from schedtools.schemas import JobState
from schedtools.tracking import JobTrackingQueue


@queue_task(__file__)
def zip_tracked_completed() -> None:
    queue = JobTrackingQueue.from_local()
    dirs = []
    for job in queue.jobs:
        archive_path = job.experiment_path + ".zip"
        if job.state == JobState.COMPLETED and not os.path.exists(archive_path):
            dirs.append(job.experiment_path)

    with mp.Pool(mp.cpu_count()) as pool:
        pool.map(zip_experiment, dirs)


if __name__ == "__main__":
    zip_tracked_completed()
