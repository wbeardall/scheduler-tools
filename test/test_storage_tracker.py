import pytest

from schedtools.storage_tracker import check_storage

if __package__ is None or __package__ == '':
    from dummy_handler import DummyHandler
else:
    from .dummy_handler import DummyHandler

class DummyLogger:
    def __init__(self) -> None:
        self.error_called = False

    def info(self,*args, **kwargs):
        ...

    def error(self,*args, **kwargs):
        self.error_called = True


@pytest.mark.parametrize("data_threshold", [False, True])
def test_check_storage(data_threshold):
    handler = DummyHandler(data_threshold=data_threshold)
    logger = DummyLogger()
    check_storage(handler, 80, logger = logger)
    assert logger.error_called == data_threshold