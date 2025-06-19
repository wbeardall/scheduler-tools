from dataclasses import dataclass
from datetime import timedelta
from inspect import getmembers
from typing import Union


@dataclass
class JobClass:
    name: str
    nodes: int
    cpu_per_node: int
    memory_per_node: int
    walltime: timedelta
    gpus: Union[int, None] = None
    gpu_type: Union[str, None] = None
    description: Union[str, None] = None

    @property
    def ncpus(self):
        return self.nodes * self.cpu_per_node

    @property
    def mem(self):
        return self.memory_per_node * 1024

    @property
    def ngpus(self):
        return self.gpus


class JobClasses:
    def __init__(self):
        # Initialize with all job classes registered on the class object
        self.classes = {
            el[1].name: el[1] for el in getmembers(self) if isinstance(el[1], JobClass)
        }

    def register(self, job_class: JobClass):
        self.classes[job_class.name] = job_class

    def get(self, name: str) -> JobClass:
        try:
            return self.classes[name]
        except KeyError:
            raise KeyError(f"Job class '{name}' not found")

    def keys(self):
        return self.classes.keys()

    def values(self):
        return self.classes.values()

    def items(self):
        return self.classes.items()

    EXP_32_64_72 = JobClass(
        name="exp_32_62_72",
        description="32 CPUs, 62GB RAM, 72 hours walltime",
        nodes=1,
        cpu_per_node=32,
        memory_per_node=62,
        walltime=timedelta(hours=72),
    )

    EXP_48_128_72 = JobClass(
        name="exp_48_128_72",
        description="48 CPUs, 126GB RAM, 72 hours walltime",
        nodes=1,
        cpu_per_node=48,
        memory_per_node=126,
        walltime=timedelta(hours=72),
    )

    # Disabled, as we don't want to accidentally utilise this!
    # EXP_256_960_72 = JobClass(
    #     name="exp_256_960_72",
    #     description="256 CPUs, 960GB RAM, 72 hours walltime",
    #     nodes=30,
    #     cpu_per_node=256,
    #     memory_per_node=960,
    #     walltime=timedelta(hours=72),
    # )

    EXP_GPU_1 = JobClass(
        name="exp_gpu1",
        description="1 RTX6000 GPU, 4 CPUs, 24GB RAM, 72 hours walltime",
        nodes=1,
        cpu_per_node=4,
        memory_per_node=24,
        walltime=timedelta(hours=72),
        gpus=1,
        gpu_type="RTX6000",
    )

    EXP_GPU_2 = JobClass(
        name="exp_gpu2",
        description="2 RTX6000 GPUs, 8 CPUs, 48GB RAM, 72 hours walltime",
        nodes=1,
        cpu_per_node=8,
        memory_per_node=48,
        walltime=timedelta(hours=72),
        gpus=2,
        gpu_type="RTX6000",
    )

    EXP_GPU_4 = JobClass(
        name="exp_gpu4",
        description="4 RTX6000 GPUs, 16 CPUs, 96GB RAM, 72 hours walltime",
        nodes=1,
        cpu_per_node=16,
        memory_per_node=96,
        walltime=timedelta(hours=72),
        gpus=4,
        gpu_type="RTX6000",
    )

    EXP_GPU_8 = JobClass(
        name="exp_gpu8",
        description="8 RTX6000 GPUs, 32 CPUs, 192GB RAM, 72 hours walltime",
        nodes=1,
        cpu_per_node=32,
        memory_per_node=192,
        walltime=timedelta(hours=72),
        gpus=8,
        gpu_type="RTX6000",
    )


job_classes = JobClasses()
