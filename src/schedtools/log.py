import logging
import os
import subprocess

import systemd.journal as journald

from schedtools.utils import Singleton

def journald_active():
    return not subprocess.run("systemctl is-active --quiet systemd-journald".split()).returncode

def get_logger(name=None):
    """Gets a logger with a particular name. If None, infers from `SCHEDTOOLS_PROG` environment variable."""
    if name is None:
        name = os.environ["SCHEDTOOLS_PROG"]
    if journald_active():
        handler = journald.JournalHandler()
    else:
        handler = logging.FileHandler(os.path.expanduser("~/.{}.log".format(name)))
    log = logging.getLogger(name)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    return log

class Loggers(dict, metaclass=Singleton):
    def __getitem__(self,key):
        if key in self:
            return self[key]
        logger = get_logger(key)
        self[key] = logger
        return logger

    @property
    def current(self):
        return self[os.environ["SCHEDTOOLS_PROG"]]

loggers = Loggers()