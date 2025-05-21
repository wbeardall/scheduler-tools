import os
import tempfile
from pathlib import Path

import pytest

from schedtools.interfaces.clear_logs import clear_cluster_logs


@pytest.mark.parametrize("pattern", ["pbs", "slurm"])
@pytest.mark.parametrize("recursive", [False, True])
def test_clear_cluster_logs(pattern, recursive, to_destroy):
    dir_ = os.path.join(tempfile.gettempdir(), "test_jobscript_dir")
    # to_destroy.append(dir_)
    if recursive:
        check_dir = os.path.join(dir_, "subdir")
    else:
        check_dir = dir_
    os.makedirs(check_dir, exist_ok=True)

    if pattern == "pbs":
        to_clear = [
            "dummylog.pbs.o1234",
            "dummylog.pbs.e1234",
        ]
        no_clear = ["dummylog2.pbs.o12345", "dummylog2.pbs.e12345", "notalog.md"]
    else:
        to_clear = ["1234.dummylog.out", "1234.dummylog.err"]
        no_clear = ["12345.dummylog2.out", "12345.dummylog2.err", "notalog.md"]

    for block in [to_clear, no_clear]:
        for file in block:
            Path(os.path.join(check_dir, file)).touch()

    clear_cluster_logs(dir_, 5000, pattern=pattern, recursive=recursive, force=True)
    for block, should_exist in [[to_clear, False], [no_clear, True]]:
        for file in block:
            assert os.path.exists(os.path.join(check_dir, file)) == should_exist, (
                os.path.join(check_dir, file) + f" should_exist {should_exist}"
            )
