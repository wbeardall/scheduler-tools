import subprocess
from enum import Enum
from typing import Dict, List, Protocol


def parse_eq(lines: List[str]) -> Dict[str, str]:
    """Parse a list of lines into a dictionary of key-value pairs."""
    outputs = {}
    for line in lines:
        if "=" in line:
            key, value = line.split("=")
            outputs[key.strip()] = value.strip()
    return outputs


def is_cx3(stdout: str) -> bool:
    """Determine if the cluster is CX3.

    NOTE: This must be kept up-to-date!
    """
    try:
        lines = stdout.split("\n")
        parsed = parse_eq(lines)
        return parsed.get("pbs_version", "").startswith("19")
    except Exception:
        return False


def is_cx3_phase_2(stdout: str) -> bool:
    """Determine if the cluster is CX3 Phase 2.

    NOTE: This must be kept up-to-date!
    """
    try:
        lines = stdout.split("\n")
        parsed = parse_eq(lines)
        return parsed.get("pbs_version", "").startswith("2024")
    except Exception:
        return False


class HandlerProtocol(Protocol):
    def execute(self, cmd: str) -> subprocess.CompletedProcess[str]: ...


class Cluster(Enum):
    CX3 = "cx3"
    CX3_PHASE_2 = "cx3_phase_2"
    UNKNOWN = "unknown"

    @classmethod
    def from_handler(cls, handler: HandlerProtocol) -> "Cluster":
        try:
            result = handler.execute("qstat --version")
            return cls._from_cmd_output(result.stdout)
        except Exception:
            return cls.UNKNOWN

    @classmethod
    def from_local(cls) -> "Cluster":
        result = subprocess.run(["qstat", "--version"], capture_output=True, text=True)
        return cls._from_cmd_output(result.stdout)

    @classmethod
    def _from_cmd_output(cls, output: str) -> "Cluster":
        if is_cx3(output):
            return cls.CX3
        elif is_cx3_phase_2(output):
            return cls.CX3_PHASE_2
        else:
            return cls.UNKNOWN

    @classmethod
    def from_server(cls, server: str) -> "Cluster":
        if server == "pbs-7":
            return cls.CX3_PHASE_2
        elif server == "pbs1.rcs.ic.ac.uk":
            return cls.CX3
        else:
            return cls.UNKNOWN
