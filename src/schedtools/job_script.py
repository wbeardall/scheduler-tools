from dataclasses import dataclass
from typing import Union

from schedtools.utils import memory_to, RevDict

_recommended_directives = {
    "SLURM": {"cpus-per-task": 42}
}

_replacement_dict = RevDict({
        "${PBS_JOBNAME}.o${PBS_JOBID%.pbs}":"${SLURM_JOBID}.${SLURM_JOB_NAME}.out",
        "${PBS_JOBNAME}.e${PBS_JOBID%.pbs}":"${SLURM_JOBID}.${SLURM_JOB_NAME}.err",
        "PBS_JOBNAME":"SLURM_JOB_NAME",
        "PBS_JOBID":"SLURM_JOBID",
        "PBS_O_WORKDIR":"SLURM_SUBMIT_DIR",
        "PBS_O_HOST":"SLURM_SUBMIT_HOST",
        "PBS_NODEFILE":"SLURM_JOB_NODELIST",
        "PBS_ARRAYID":"SLURM_ARRAY_TASK_ID"
    })

@dataclass
class JobScript:
    nodes: int
    ncpus: int
    mem_per_cpu: int # MB
    ngpus: int
    gpu_type: str
    walltime: str
    script_body: str
    account: Union[str, None] = None

    def __init__(self,nodes,ncpus, mem_per_cpu,ngpus,gpu_type,walltime,script_body,account=None) -> None:
        self.nodes = int(nodes)
        self.ncpus = int(ncpus)
        if isinstance(mem_per_cpu, str):
            mem_per_cpu = memory_to(mem_per_cpu, "MB")
        self.mem_per_cpu = int(mem_per_cpu)
        self.ngpus = int(ngpus)
        self.gpu_type = gpu_type
        self.walltime = walltime
        if isinstance(script_body, (list, tuple)):
            script_body = "\n".join(script_body)
        self.script_body=script_body
        self.account = account

    def update(self,other):
        assert isinstance(other, dict), "JobScripts can only be updated with dict objects"
        for k,v in other.items():
            setattr(self,k,v)

    @classmethod
    def parse(cls, script):
        with open(script, "r") as f:
            lines = f.readlines()
        if any([l.startswith("#PBS") for l in lines]):
            return cls.parse_from_pbs(script)
        elif any([l.startswith("#SBATCH") for l in lines]):
            return cls.parse_from_slurm(script)
        else:
            raise RuntimeError("Provided script file contains neither PBS or SLURM directives.")

    @classmethod
    def parse_from_pbs(cls, script):
        with open(script, "r") as f:
            lines = f.readlines()
        directives = {}
        # Ignore lines before directives
        after_directives = False
        script = []
        for l in lines:
            if l.startswith("#PBS"):
                after_directives = True
                l = l.strip().split("#PBS -l ")[-1]
                # Separate, because contains colons
                if "walltime" in l:
                    directives["walltime"] = l.split("=")[-1]
                else:
                    for l in l.split(":"):
                        directives.__setitem__(*l.split("="))
            elif after_directives:
                script.append(l)

        return cls(
            nodes=directives["select"],
            ncpus=directives["ncpus"],
            mem_per_cpu=memory_to(directives["mem"], "MB") / int(directives["ncpus"]),
            ngpus=directives["ngpus"],
            gpu_type=directives["gpu_type"],
            walltime=directives["walltime"],
            script_body=script
        )

    @classmethod
    def parse_from_slurm(cls, script):
        with open(script, "r") as f:
            lines = f.readlines()
        directives = {}
        # Ignore lines before directives
        after_directives = False
        script = []
        for l in lines:
            if l.startswith("#SBATCH"):
                after_directives = True
                directives.__setitem__(*l.strip().split("--")[-1].split("="))
            elif after_directives:
                script.append(l)

        gpu_alloc = directives["gres"].split(":")
        return cls(
            nodes=directives["nodes"],
            ncpus=directives["cpus-per-task"],
            mem_per_cpu=directives["mem-per-cpu"],
            ngpus=gpu_alloc[-1],
            gpu_type=gpu_alloc[1],
            walltime=directives["time"],
            script_body=script,
            account=directives.get("account",None)
            )
    
    @property
    def pbs_header(self):
        return "\n".join([
            "#!/bin/sh",
            f"#PBS -l walltime={self.walltime}",
            f"#PBS -l select={self.nodes}:ncpus={self.ncpus}:mem={self.mem_per_cpu*self.ncpus}mb:ngpus={self.ngpus}:gpu_type={self.gpu_type}"
        ])

    @property
    def slurm_header(self):
        assert self.account is not None, "Cannot render SLURM header, `account` is not set."
        return "\n".join([
            "#!/bin/sh",
            *[f"#SBATCH --{k}={v}" for k,v in {
                "nodes":self.nodes,
                "ntasks-per-node":1,
                "cpus-per-task":self.ncpus,
                "mem-per-cpu":self.mem_per_cpu,
                "gres":f"gpu:{self.gpu_type}:{self.ngpus}",
                "partition":"gpu",
                "time":self.walltime,
                "account":self.account
            }.items()]
        ])

    @property
    def slurm_body(self):
        body = self.script_body
        for old, new in _replacement_dict.items():
            body.replace(old,new)
        return body

    @property
    def pbs_body(self):
        body = self.script_body
        for old, new in _replacement_dict.rev.items():
            body.replace(old,new)
        return body

    def to_slurm(self,file=None):
        script = self.slurm_header + "\n" + self.slurm_body
        if file is None:
            return script
        else:
            with open(file, "w") as f:
                f.write(script)

    def to_pbs(self,file=None):
        script = self.pbs_header + "\n" + self.pbs_body
        if file is None:
            return script
        else:
            with open(file, "w") as f:
                f.write(script)

def convert_to_pbs(file, destination, updates = {}):
    """Convert a jobscript to PBS format."""
    jobscript = JobScript.parse(file)
    jobscript.update(updates)
    jobscript.to_pbs(destination)

def convert_to_slurm(file, destination, updates = {}):
    """Convert a jobscript to SLURM format."""
    jobscript = JobScript.parse(file)
    jobscript.update(updates)
    jobscript.to_slurm(destination)