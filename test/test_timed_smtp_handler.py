import logging
import os
import tempfile
import time
import uuid

import pytest

from schedtools.log import TimedSMTPHandler
from schedtools.smtp import load_credentials

@pytest.mark.skip(reason="This test requires email credentials, and sends an email, so is disabled for convenience.")
@pytest.mark.nohidecreds
def test_timed_smtp_handler(to_destroy):
    errs = 5
    logger = logging.getLogger("dummy")
    filename = os.path.join(tempfile.gettempdir(), str(uuid.uuid1()) + ".log")
    to_destroy.append(filename)
    creds = load_credentials()
    handler = TimedSMTPHandler(
        filename=filename,
        mailhost=creds.server,
        fromaddr=creds.sender_address,
        toaddrs=[creds.destination_address],
        subject="timed smtp log test", 
        credentials=(creds.sender_address, creds.password),
        secure=(),
        timeout=5.,
        when='s'
    )
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    for i in range(errs):
        # Want to make sure at least one of the errs comes after the 1s mark, so that rollover is triggered.
        logger.error(f"Test error ({i}/{errs})")
    time.sleep(1.)
    logger.error("Final error. This should send the email.")