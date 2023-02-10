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
This program runs itself in a daemon context, and so is SIGHUP-safe, even when not run as a service.

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