import re


def walltime_to(walltime, period="h"):
    assert period in ["s", "m", "h"]
    walltime = [int(el) for el in walltime.split(":")]
    assert len(walltime) == 3
    s = walltime[0] * 60 * 60 + walltime[1] * 60 + walltime[2]
    if period == "s":
        return s
    elif period == "m":
        return s / 60
    else:
        return s / 3600


def memory_to(memory, scale="MB"):
    scale_map = {"gb": 1000, "mb": 1, "": 1}
    pattern = r"(\d+)([A-Za-z]{0,2})"
    match = re.match(pattern, memory)
    if match:
        numeric_part = match.group(1)
        alpha_part = match.group(2)
        return (
            int(numeric_part) * scale_map[alpha_part.lower()] / scale_map[scale.lower()]
        )
    else:
        raise ValueError(f"Unrecognized memory format: {memory}")
