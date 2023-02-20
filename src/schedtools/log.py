import logging
import os
from typing import Union

import systemd.journal as journald

from schedtools.utils import Singleton, journald_active, systemd_service

def get_logger(name: Union[str, None] = None):
    """Gets a logger with a particular name. If None, infers from `SCHEDTOOLS_PROG` environment variable.
    
    If `systemd-journald` is active, and the program is being run as a `systemd` service, 
    logs are written to `journald`. Otherwise, logs are written to `${HOME}/.${SCHEDTOOLS_PROG}.log`

    In the case where the program is running as a service, but `journald` is not active, logs will
    be written to the $HOME of the user who originally registered the service.
    """
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
    log.setLevel(logging.INFO)
    for handler in handlers:
        log.addHandler(handler)
    return log

class Loggers(dict, metaclass=Singleton):
    """Singleton dict-like logger registry. 
    
    Use to fetch loggers by name. If the requested logger does not exist,
    it is created, registered and returned.

    The named logger for the current `SCHEDTOOLS_PROG` can be accessed through the
    `current` attribute.
    """
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