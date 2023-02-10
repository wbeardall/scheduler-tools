# Scheduler Tools

Basic tools for automating some PBS work. In progress, and potentially unsafe. In particular, running provided 
programs with the `-s` flag will turn them into services. This requires `sudo` privileges, so don't run these unless
you trust that I'm not doing anything nasty to your box!

Only PBS is supported for now. Functionality might be extended to support SLURM in the future.

## Contents

### Rerun

Periodically check the runtimes of scheduled jobs on a cluster, and `qrerun` them if they are close to timing out.

## Usage

Recommended: Run the provided tools as services (with the `-s` flag), in a designated environment.

```
conda create -n schedtools python=3.8
conda activate schedtools
pip install -e .
```

This should allow the service to access the correct Python installation upon repeated reboots, and ensures that
the environment is stable.