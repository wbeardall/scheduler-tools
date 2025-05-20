import pytest

from schedtools.schemas import parse_memory


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
