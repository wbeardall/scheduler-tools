import pytest

from schedtools.schemas import match_jobs, parse_memory


class AttrDict(dict):
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{item}'"
            )


@pytest.mark.parametrize(
    "memory, expected",
    [
        ("1000", 1000),
        ("1000kb", 1e6),
        ("1000mb", 1e9),
    ],
)
def test_parse_memory(memory, expected):
    assert parse_memory(memory) == expected


def test_parse_memory_raises_value_error():
    with pytest.raises(ValueError):
        parse_memory("not_a_memory")


@pytest.mark.parametrize(
    "a, b, expected",
    [
        # String matches
        ("job1", "job1", True),
        ("job1", "job2", False),
        # String vs JobSpec/Job with id
        ("job1", AttrDict({"id": "job1"}), True),
        ("job1", AttrDict({"id": "job2"}), False),
        # String vs JobSpec/Job with scheduler_id
        ("job1", AttrDict({"scheduler_id": "job1"}), True),
        ("job1", AttrDict({"scheduler_id": "job2"}), False),
        # JobSpec/Job with id matches
        (AttrDict({"id": "job1"}), AttrDict({"id": "job1"}), True),
        (AttrDict({"id": "job1"}), AttrDict({"id": "job2"}), False),
        # JobSpec/Job with scheduler_id matches
        (
            AttrDict({"scheduler_id": "job1"}),
            AttrDict({"scheduler_id": "job1"}),
            True,
        ),
        (
            AttrDict({"scheduler_id": "job1"}),
            AttrDict({"scheduler_id": "job2"}),
            False,
        ),
        # Mixed id and scheduler_id matches
        (AttrDict({"id": "job1"}), AttrDict({"scheduler_id": "job1"}), False),
        (AttrDict({"scheduler_id": "job1"}), AttrDict({"id": "job1"}), False),
        # No matches
        (AttrDict({"id": "job1"}), AttrDict({"scheduler_id": "job2"}), False),
        (AttrDict({"scheduler_id": "job1"}), AttrDict({"id": "job2"}), False),
    ],
)
def test_match_jobs(a, b, expected):
    assert match_jobs(a, b) == expected
