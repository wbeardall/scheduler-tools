import os
import shutil
import subprocess
import tempfile

import pytest

from schedtools.job_script import JobScript

@pytest.mark.parametrize("format", ["pbs", "slurm"])
@pytest.mark.parametrize("level",["file","dir"])
def test_conversion_interface(format, level, to_destroy):
    file = os.path.join(os.path.dirname(__file__),"dummy.pbs" if format == "slurm" else "dummy.sbatch")
    new = os.path.join(tempfile.gettempdir(),"tmp.pbs" if format == "slurm" else "tmp.sbatch")
    dst = os.path.join(tempfile.gettempdir(),"tmp.sbatch" if format == "slurm" else "tmp.pbs")
    to_destroy.append(new)
    to_destroy.append(dst)
    shutil.copy(file, new)
    if level=="file":
        search = new
    else:
        search = os.path.dirname(new)
    subprocess.run(["convert-jobscripts",format,search, "--updates", "account=xx111"])

    ref = JobScript.parse(file)
    dst = JobScript.parse(dst)

    for attr in ["nodes","ncpus","mem_per_cpu","ngpus","gpu_type","walltime"]:
        assert getattr(ref,attr) == getattr(dst,attr)

    # Check script consistency up to newlines
    assert ref.script_body.replace("\n","") == dst.script_body.replace("\n","")