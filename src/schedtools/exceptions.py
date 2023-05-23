class JobSubmissionError(RuntimeError):
    pass


class QueueFullError(JobSubmissionError):
    pass


class MissingJobScriptError(JobSubmissionError):
    pass


class JobDeletionError(RuntimeError):
    pass
