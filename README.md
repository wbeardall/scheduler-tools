# Scheduler Tools

Basic tools for automating some PBS work. In progress, and potentially unsafe. In particular, running provided 
programs with the `-s` flag will turn them into services. This requires `sudo` privileges, so don't run these unless
you trust that I'm not doing anything nasty to your box!

Running provided programs with the `-s` flag will register them with `systemd`, allowing them to be automatically 
restarted upon reboot. This essentially fully automates the program on the server, so you don't have to worry about 
it again unless your box catches fire (or if you do one of the following things):

1. Remove or move the environment `schedtools` is installed in (see `Usage`)
2. Move or remove the `scheduler-tools` directory
3. Move or remove your `~/.ssh/config`
4. Change the password or other credentials on the cluster

Only PBS is supported for now. Functionality might be extended to support SLURM in the future.

## Contents

### Rerun

Periodically check the runtimes of scheduled jobs on a cluster, and `qrerun` them if they are close to timing out.
This program runs itself in a daemon context if not running as a service, and so is SIGHUP-safe.

## Usage

If you have manager-level privileges on the cluster, usage is straightforward, as the program can simply call the `qrerun`
command with the IDs of any jobs that are at risk of timing out. 

**Warning**
If you *do not have manager-level privileges* (which is likely to be the case), there are a couple considerations you need
to remember. `rerun` will call `qsub` internally, using the `jobscript.pbs` specified in the `Submit_arguments` section of the full
PBS job information (call `qstat -f` to see what this datastructure looks like). This means that you must:

1. `qsub` from a PBS jobscript file when launching jobs (i.e. don't pipe job information to `qsub`)
2. Not delete your jobscripts after submission, as `rerun` will need access to them to resubmit the jobs

    **Note** You can use your ephemeral storage for these except in the case of extremely long-running jobs

## Installation

This package relies on [systemd-python](https://pypi.org/project/systemd-python/) to interact with `journald`. As such,
there are a few non-Python dependencies. These can be installed by running

```
sudo apt-get install gcc pkg-config libsystemd-dev
```

## Usage

Recommended: Run the provided tools as services (with the `-s` flag), in a designated environment.

```
conda create -n schedtools python=3.8
conda activate schedtools
pip install -e .
```

This should allow the service to access the correct Python installation upon repeated reboots, and ensures that
the environment is stable.