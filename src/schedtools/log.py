import logging
import os
import subprocess

import systemd.journal as journald

from schedtools.utils import Singleton, journald_active, systemd_service


def get_logger(name=None):
    """Gets a logger with a particular name. If None, infers from `SCHEDTOOLS_PROG` environment variable."""
    if name is None:
        name = os.environ["SCHEDTOOLS_PROG"]
    # Preferentially use `SCHEDTOOLS_USER` in case we're running as a service
    user = os.environ.get("SCHEDTOOLS_USER",os.environ["LOGNAME"])
    handlers = []
    if journald_active() and systemd_service():
        handlers.append(journald.JournalHandler())
    else:
        handlers.append(logging.FileHandler("/home/{}/.{}.log".format(user, name)))
    
    log = logging.getLogger(name)
    for handler in handlers:
        log.addHandler(handler)
    log.setLevel(logging.INFO)
    return log

class Loggers(dict, metaclass=Singleton):
    def __getitem__(self,key):
        if key in self:
            return super().__getitem__(key)
        logger = get_logger(key)
        self[key] = logger
        return logger

    @property
    def current(self):
        return self[os.environ["SCHEDTOOLS_PROG"]]

loggers = Loggers()