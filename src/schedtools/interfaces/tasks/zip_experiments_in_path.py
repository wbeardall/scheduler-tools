import logging
import multiprocessing as mp
import os
import re

from schedtools.compression import zip_experiment
from schedtools.interfaces.tasks.task import queue_task

logger = logging.getLogger(__name__)


@queue_task(__file__)
def zip_experiments_in_path(path: str, pattern: str) -> None:
    dirs = []
    for d in os.listdir(path):
        abspath = os.path.join(path, d)
        conditions = [
            os.path.isdir(abspath),
            re.search(pattern, d),
            os.path.exists(os.path.join(abspath, ".run_complete")),
        ]
        if all(conditions):
            dirs.append(abspath)

    with mp.Pool(mp.cpu_count()) as pool:
        pool.map(zip_experiment, dirs)


if __name__ == "__main__":
    zip_experiments_in_path()
