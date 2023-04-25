import logging
import os
import tempfile
from datetime import datetime
from logging.handlers import SMTPHandler, TimedRotatingFileHandler
from typing import Union

try:
    import systemd.journal as journald  # type: ignore

    HAS_JOURNALD = True
except ImportError:
    HAS_JOURNALD = False

from schedtools.smtp import load_credentials
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

    handlers = []

    # Email notifications
    try:
        creds = load_credentials()
        mail_handler = SMTPHandler(
            mailhost=(creds.server, creds.port),
            fromaddr=creds.sender_address,
            toaddrs=[creds.destination_address],
            subject=f"{name} error",
            credentials=(creds.sender_address, creds.password),
            secure=(),
        )
        mail_handler.setLevel(logging.ERROR)
        mail_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        handlers.append(mail_handler)
        timed_handler = TimedSMTPHandler(
            os.path.join(tempfile.gettempdir(), "{}.log".format(name)),
            mailhost=(creds.server, creds.port),
            fromaddr=creds.sender_address,
            toaddrs=[creds.destination_address],
            subject=name + " log report",
            credentials=(creds.sender_address, creds.password),
            secure=(),
            when="d",
        )
        timed_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        handlers.append(timed_handler)
    except RuntimeError:
        pass

    if journald_active() and systemd_service() and HAS_JOURNALD:
        handlers.append(journald.JournalHandler())
    else:
        if systemd_service():
            # If systemd service, we have to assume a standard home directory location.
            user_home = "/home/{}".format(os.environ["SCHEDTOOLS_USER"])
        else:
            user_home = os.path.expanduser("~")
        try:
            handlers.append(
                logging.FileHandler(os.path.join(user_home, f".{name}.log"))
            )
        except FileNotFoundError:
            # Non-standard home directory location, skip file logging
            pass

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

    def __getitem__(self, key):
        if key in self:
            return super().__getitem__(key)
        logger = get_logger(key)
        self[key] = logger
        return logger

    @property
    def current(self):
        return self[os.environ.get("SCHEDTOOLS_PROG", "base")]


loggers = Loggers()


class TimedSMTPHandler(TimedRotatingFileHandler):
    """
    A handler class which periodically sends an SMTP email containing the logs since the last email.

    SMTP behaviour follows `logging.handlers.SMTPHandler`, whilst timing behaviour follows `logging.handlers.TimedRotatingFileHandler`,
    except in that the default period is 1 day instead of 1 hour.
    """

    def __init__(
        self,
        filename,
        mailhost,
        fromaddr,
        toaddrs,
        subject,
        credentials=None,
        secure=None,
        timeout=5.0,
        when="d",
        interval=1,
        encoding=None,
        delay=False,
        utc=False,
        atTime=None,
    ):
        super().__init__(
            filename,
            when=when,
            interval=interval,
            backupCount=1,  # Think of the emailed log as the one backup.
            encoding=encoding,
            delay=delay,
            utc=utc,
            atTime=atTime,
        )

        if isinstance(mailhost, (list, tuple)):
            self.mailhost, self.mailport = mailhost
        else:
            self.mailhost, self.mailport = mailhost, None
        if isinstance(credentials, (list, tuple)):
            self.username, self.password = credentials
        else:
            self.username = None
        self.fromaddr = fromaddr
        if isinstance(toaddrs, str):
            toaddrs = [toaddrs]
        self.toaddrs = toaddrs
        self.subject = subject
        self.secure = secure
        self.timeout = timeout
        try:
            with open(self.timeFilename, "r") as f:
                self.log_start = datetime.fromisoformat(f.read())
        except (FileNotFoundError, ValueError):
            self.log_start = datetime.now()

    @property
    def log_start(self):
        return self._log_start

    @log_start.setter
    def log_start(self, log_start):
        with open(self.timeFilename, "w") as f:
            f.write(log_start.isoformat())
        self._log_start = log_start

    @property
    def timeFilename(self):
        return self.baseFilename + ".log-start"

    def getSubject(self):
        now = datetime.now()
        subject = (
            self.subject
            + " ("
            + self.log_start.strftime("%Y-%m-%d %H:%M:%S")
            + " - "
            + now.strftime("%Y-%m-%d %H:%M:%S")
            + ")"
        )
        self.log_start = now
        return subject

    def emit(self, record):
        """
        Emit a record. Emits to file before checking rollover, unlike builting RotatingFileHandler classes.

        Output the record to the file, catering for rollover as described
        in doRollover().
        """
        try:
            logging.FileHandler.emit(self, record)
            if self.shouldRollover(record):
                self.doRollover()
        except Exception:
            self.handleError(record)

    def rotate(self, source, dest):
        with open(self.baseFilename, "r", encoding=self.encoding) as f:
            self.send_email("\n".join(f.readlines()))
        super().rotate(source, dest)

    def send_email(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """
        try:
            import email.utils
            import smtplib
            from email.message import EmailMessage

            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            smtp = smtplib.SMTP(self.mailhost, port, timeout=self.timeout)
            msg = EmailMessage()
            msg["From"] = self.fromaddr
            msg["To"] = ",".join(self.toaddrs)
            msg["Subject"] = self.getSubject()
            msg["Date"] = email.utils.localtime()
            msg.set_content(f"<pre>{record}</pre>", subtype="html")
            if self.username:
                if self.secure is not None:
                    smtp.ehlo()
                    smtp.starttls(*self.secure)
                    smtp.ehlo()
                smtp.login(self.username, self.password)
            smtp.send_message(msg)
            smtp.quit()
        except Exception:
            self.handleError(record)

    def getFilesToDelete(self):
        """
        Removes all backup files.
        """
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = baseName + "."
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                if self.extMatch.match(suffix):
                    result.append(os.path.join(dirName, fileName))
        return result
