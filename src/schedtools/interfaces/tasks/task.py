import inspect
import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass

import click

from schedtools.utils import memory_to, walltime_to

default_nodes = 1
default_ncpus = 4
default_mem = "4gb"
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


def queue_task(file: str):
    """
    Decorator to create a click group for a function that can be queued to PBS.

    """

    def decorator(fn: Callable[[], None]) -> click.Group:
        sig = inspect.signature(fn)
        params = []
        for name, param in sig.parameters.items():
            if param.default is inspect.Parameter.empty:
                params.append(click.option(f"--{name}", required=True))
            else:
                params.append(click.option(f"--{name}", default=param.default))

        @click.group(fn.__name__)
        def cli():
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
                subprocess.run(["qsub"], input=job_script, check=True)
            else:
                print(job_script)

        for param in reversed(params):
            queue = param(queue)

        queue = cli.command("queue")(queue)

        @cli.command("execute")
        @click.argument("payload_json", type=str)
        @click.pass_context
        def execute(ctx, payload_json):
            payload: dict = json.loads(payload_json)
            kwargs = {k: v for k, v in payload.items() if k in sig.parameters}
            fn(**kwargs)

        return cli

    return decorator
