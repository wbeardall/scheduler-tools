import inspect
import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Dict, Union

import click

from schedtools.parsing import memory_to, walltime_to

default_nodes = 1
default_ncpus = 4
default_mem = "8gb"
default_walltime = "01:00:00"


@dataclass
class PBSJobSpec:
    nodes: int
    ncpus: int
    mem_mb: int
    walltime_hours: float

    @classmethod
    def parse(cls, nodes: int, ncpus: int, mem: str, walltime: str):
        mem_mb = memory_to(mem, "MB")
        walltime = walltime_to(walltime, "h")
        return cls(nodes=nodes, ncpus=ncpus, mem_mb=mem_mb, walltime_hours=walltime)

    @property
    def mem(self):
        return f"{int(self.mem_mb)}mb"

    @property
    def pbs_header(self):
        return "\n".join(
            [
                "#!/bin/sh",
                f"#PBS -l walltime={int(self.walltime_hours):02d}:00:00",
                f"#PBS -l select={self.nodes}:ncpus={self.ncpus}:mem={self.mem}",
            ]
        )


JOBSCRIPT_TEMPLATE = """
{header}

"{executable}" "{file}" execute '{payload}'
"""


@click.group("tasks")
def tasks():
    pass


def queue_task(
    file: str,
    param_dict: Union[Dict[str, click.Option], None] = None,
):
    """
    Decorator to create a click group for a function that can be queued to PBS.

    `param_dict` is useful for specifying click options with non-trivial options, like multiple choices,
    nargs, etc.

    Args:
        file: The file to run.
        param_dict: A dictionary of parameters to add to the click group. If None,
            the parameters will be inferred from the function signature.

    Returns:
        A click group that can be used to queue the function to PBS.

    """
    if param_dict is None:
        param_dict = {}

    def decorator(fn: Callable[[], None]) -> click.Group:
        sig = inspect.signature(fn)
        for name, param in sig.parameters.items():
            if name not in param_dict:
                if param.default is inspect.Parameter.empty:
                    param_dict[name] = click.option(f"--{name}", required=True)
                else:
                    param_dict[name] = click.option(f"--{name}", default=param.default)

        params = list(param_dict.values())

        @tasks.group(fn.__name__)
        def group():
            pass

        @click.option("--submit", is_flag=True)
        @click.option("--nodes", type=int, default=default_nodes)
        @click.option("--ncpus", type=int, default=default_ncpus)
        @click.option("--mem", type=str, default=default_mem)
        @click.option("--walltime", type=str, default=default_walltime)
        @click.pass_context
        def queue(ctx, *, nodes, ncpus, mem, walltime, submit, **kwargs):
            job_spec = PBSJobSpec.parse(nodes, ncpus, mem, walltime)
            json_payload = json.dumps(kwargs)
            job_script = JOBSCRIPT_TEMPLATE.format(
                header=job_spec.pbs_header,
                executable=sys.executable,
                file=file,
                payload=json_payload,
            )
            if submit:
                subprocess.run(["qsub"], input=job_script.encode(), check=True)
            else:
                print(job_script)

        for param in reversed(params):
            queue = param(queue)

        queue = group.command("queue")(queue)

        @group.command("execute")
        @click.argument("payload_json", type=str)
        @click.pass_context
        def execute(ctx, payload_json):
            payload: dict = json.loads(payload_json)
            kwargs = {k: v for k, v in payload.items() if k in sig.parameters}
            fn(**kwargs)

        return group

    return decorator
