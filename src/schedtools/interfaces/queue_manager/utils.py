from enum import Enum
from typing import Union


def prettify(s: Union[str, Enum]) -> str:
    if isinstance(s, Enum):
        s = s.value
    return s.replace("_", " ").title()
