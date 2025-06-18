import multiprocessing as mp
import os
from typing import List, Union

from schedtools.compression import zip_experiment
from schedtools.interfaces.tasks.task import queue_task
from schedtools.schemas import JobState
from schedtools.tracking import JobTrackingQueue


def get_all_completed_unzipped() -> List[str]:
    queue = JobTrackingQueue.from_local()
    dirs = []
    for job in queue.jobs:
        archive_path = job.experiment_path + ".zip"
        if job.state == JobState.COMPLETED and not os.path.exists(archive_path):
            dirs.append(job.experiment_path)
    return dirs


@queue_task(__file__)
def zip_experiments(paths: Union[str, List[str], None] = None) -> None:
    if paths is None:
        paths = get_all_completed_unzipped()
    elif isinstance(paths, str):
        paths = [paths]

    # Filter to completed experiments
    paths = [p for p in paths if os.path.exists(os.path.join(p, ".run_complete"))]

    with mp.Pool(mp.cpu_count()) as pool:
        pool.map(zip_experiment, paths)


if __name__ == "__main__":
    zip_experiments()
