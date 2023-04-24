class JobSubmissionError(RuntimeError):
    pass


class QueueFullError(JobSubmissionError):
    pass


class JobDeletionError(RuntimeError):
    pass
