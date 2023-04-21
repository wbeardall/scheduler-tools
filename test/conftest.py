import os
import shutil

import pytest

from schedtools.utils import config_dir


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


@pytest.fixture(autouse=True)
def hide_smtp_creds(request):
    if "nohidecreds" in request.keywords:
        yield
    else:
        cred_path = os.path.join(config_dir(), "smtp.json")
        if os.path.exists(cred_path):
            os.rename(cred_path, cred_path + ".old")
            yield
            os.rename(cred_path + ".old", cred_path)
        else:
            yield
