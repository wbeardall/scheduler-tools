import os
import tempfile

import pytest

from schedtools.job_script import JobScript, convert_to_pbs, convert_to_slurm


@pytest.mark.parametrize("file", ["dummy.pbs", "dummy.sbatch"])
def test_resource_instantiation(file):
    file = os.path.join(os.path.dirname(__file__), file)
    expected = dict(
        nodes=1,
        ncpus=4,
        mem_per_cpu=3850,
        ngpus=1,
        gpu_type="RTX6000",
        walltime="72:00:00",
    )
    alloc = JobScript.parse(file)
    for k, v in expected.items():
        assert getattr(alloc, k) == v


@pytest.mark.parametrize(
    "format, convert_fn", [["pbs", convert_to_pbs], ["slurm", convert_to_slurm]]
)
def test_conversion(format, convert_fn, to_destroy):
    if format == "pbs":
        src_file = os.path.join(os.path.dirname(__file__), "dummy.sbatch")
        ref_file = os.path.join(os.path.dirname(__file__), "dummy.pbs")
        dst_file = os.path.join(tempfile.gettempdir(), "generated.pbs")
    else:
        src_file = os.path.join(os.path.dirname(__file__), "dummy.pbs")
        ref_file = os.path.join(os.path.dirname(__file__), "dummy.sbatch")
        dst_file = os.path.join(tempfile.gettempdir(), "generated.sbatch")
    to_destroy.append(dst_file)
    ref = JobScript.parse(ref_file)

    convert_fn(src_file, dst_file, updates=dict(account="xx111"))
    dst = JobScript.parse(dst_file)

    for attr in ["nodes", "ncpus", "mem_per_cpu", "ngpus", "gpu_type", "walltime"]:
        assert getattr(ref, attr) == getattr(dst, attr)

    # Check script consistency up to newlines
    assert ref.script_body.replace("\n", "") == dst.script_body.replace("\n", "")
