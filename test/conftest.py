import os
import shutil

import pytest

@pytest.fixture(scope="function")
def to_destroy():
    to_destroy = []
    yield to_destroy

    for v in to_destroy:
        if os.path.isfile(v):
            os.remove(v)
        elif os.path.isdir(v):
            shutil.rmtree(v)
    # Remove empty folders
    for v in to_destroy:
        dir_ = os.path.split(v)[0]
        if os.path.exists(dir_):
            if not len(os.listdir(dir_)):
                os.rmdir(dir_)