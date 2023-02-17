# Scheduler Tools

![Tests](https://github.com/wbeardall/scheduler-tools/actions/workflows/tox.yml/badge.svg) ![Platform](https://img.shields.io/badge/platform-linux--64-lightgray) ![Python](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue)


---

Basic tools for automating some PBS work. In progress, and potentially unsafe. In particular, running provided 
programs with the `-s` flag will register them as services with `systemd` (see [Running Programs as Services](#running-programs-as-services)). 
Registering a new service requires elevated privileges, so naturally you can't use programs in this way if you don't have
root access to the machine you're using. Additionally, never run any untrusted program with elevated privileges, so don't use
these programs in service mode unless you trust that I'm not doing anything nasty to your box!

Running provided programs with the `-s` flag will register them with `systemd`, allowing them to be automatically 
restarted upon reboot. This essentially fully automates the program on the server, so you don't have to worry about 
it again unless your box catches fire (or if you do one of the following things):

1. Remove or move the environment `schedtools` is installed in (see [Package Usage](#package-usage))
2. Move or remove the `scheduler-tools` directory
3. Move or remove your `~/.ssh/config`
4. Change the password or other credentials on the cluster

Only PBS is supported for now. Functionality might be extended to support SLURM in the future.

## Contents

### `rerun`

Periodically check the runtimes of scheduled jobs on a cluster, and `qrerun` them if they are close to timing out.
This program runs itself in a daemon context (see [Running Programs as Daemons](#running-programs-as-daemons)) 
if not running as a service, and so is `SIGHUP`-safe.

#### Usage

For detailed information on the CLI for the `rerun` utility, run the following command:

```
rerun -h
```

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

We recommend installing `schedtools` in a dedicated environment, especially if intended to be used in service mode. 
This minimises the probability that changes to the environment breaks the service in a way that might not be noticed 
for a while (in case you don't check your logs frequently!).

```
conda create -n schedtools python=3.8
conda activate schedtools
pip install -e .
```

### `convert-jobscripts`

Convert jobscripts from `.pbs` format into `.sbatch` format, or vice versa. 

**Note** this utility is not designed to be run as a service, as it is a simple conversion tool. Therefore, it lacks a 
`-s` flag. 

#### Usage

For detailed information on the CLI for the `convert-jobscripts` utility, run the following command:

```
convert-jobscripts -h
```

## Package Usage

Recommended: Run the provided tools as services (with the `-s` flag), in a dedicated environment.

This should allow the service to access the correct Python installation upon repeated reboots, and ensures that
the environment is stable.

### Before You Start

`schedtools` programs pull cluster SSH information from your user-level SSH configuration file (`$HOME/.ssh/config`). 
We recommend setting up key-based authentication with the cluster for security, if the cluster allows public key
authentication (see below). This will prevent the need to enter your password into any `schedtools` programs, and 
prevent `schedtools` from needing to store credentials itself. However, if `schedtools` does have to store the password,
it is stored in a config file to which only `root` has access, so it is as secure as storing SSH keys locally.

Ensure that you've added *at least* the following information to your SSH config for the cluster you're interfacing with:

```
Host my-cluster
  HostName my-cluster-address.com
  User cluster-username
```

Note that certain clusters, such as [SULIS](https://sulis-hpc.github.io/) use TOTP (Time-based One Time Password) authentication
to increase security. `schedtools` is not designed to handle the TOTP side of authentication, so you must manage this yourself.

### Setting Up Key-Based Authentication

If your cluster allows, it we recommend using key-based authentication to allow `schedtools` programs SSH access.

**Note**: The login servers at the Imperial College RCS don't allow key-based authentication. If you're using `schedtools`
with the Imperial College RCS, this section is not relevant to you. Check with your HPC admin if you're unsure whether 
your cluster supports key-based authentication.

1. Generate a key pair on your local machine with `ssh-keygen`

```
ssh-keygen -f $HOME/.ssh/cluster_rsa
```

2. Copy the public key to the cluster with `ssh-copy-id`

If you've already added the cluster to your `$HOME/.ssh/config`, you can simply run

```
ssh-copy-id -i $HOME/.ssh/cluster_rsa my-cluster
```

Otherwise, provide the full address:

```
ssh-copy-id -i $HOME/.ssh/cluster_rsa user@host
```

3. Add the key to your `$HOME/.ssh/config`. The updated config entry should look like this:

```
Host my-cluster
  HostName my-cluster-address.com
  User cluster-username
  IdentityFile ~/.ssh/cluster_rsa
```

### Running Programs as Daemons

If you've done the above correctly, you should be able to run (e.g. the `rerun` utility) in daemon mode
as follows:

```
rerun my-cluster
```

The program will daemonize itself, so it is safe to log out of your session, and the program will continue running
until you reboot the machine. You can check that the program is running properly by calling

```
ps aux | grep schedtools
```

### Running Programs as Services

Programs (e.g. the `rerun` utility) can be run in service mode as follows:

```
rerun my-cluster -s
```

This will register the program as a service with `systemd`. This ensures that the program is relaunched upon reboot.
You can check that the service is running properly by calling

```
systemctl status rerun.service
```

If your machine uses `journald` (likely), then logs can be accessed by calling

```
journalctl -u rerun.service
```