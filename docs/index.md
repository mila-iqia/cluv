# cluv

A powerful and lightweight CLI tool to sync UV-based Python projects and submit jobs across HPC
clusters.

This is a quick overview. New to cluv? Follow the [Getting started](guides/getting-started.md) guide.

## Who is this for?

Cluv is for you if:

- You have access to one or more HPC clusters running Slurm.
- You want to easily synchronize and dispatch jobs on all the clusters you have access to.
- You use [uv](https://docs.astral.sh/uv/) to manage your Python projects and dependencies.

## Installation

Cluv can be installed on a local machine or on a remote cluster.

Add the package to your project with `uv add`:

```console
uv add cluster-uv
```

Install as a command-line tool in an isolated environment:

```console
uv tool install cluster-uv
```

If you want the bleeding edge version from GitHub, use:

```console
uv add git+https://github.com/mila-iqia/cluv
```

### Update
Run this once in a while to get the latest features and bugfixes.

```console
uv tool update cluster-uv
```

## Core Assumptions

Cluv makes a few very important assumptions about your workflow. These are necessary and won't
change:

1. Your project uses [uv](https://docs.astral.sh/uv/) to manage your Python projects and dependencies.
2. You have SSH access to each cluster, with ControlMaster sessions configured for passwordless login.
3. Your project is in a Git repository hosted on GitHub.

!!! warning "Cluv is not supported on Windows"
    `cluv` is not supported on Windows (Windows SSH clients don't support the necessary features).
    Use WSL2 instead.

Cluv is not meant to be a _magic_ package that does everything for you. It is a lightweight wrapper around `uv` that makes it easier to work with multiple Slurm clusters, but it does not replace your SSH configuration or GitHub setup. If you need help with those, see the [Troubleshooting](troubleshooting.md) page.


## Usage

To view all available commands, use the `--help` flag:
```console
cluv --help
```

### Synchronize a project across clusters
- [`cluv init`](commands.md#cluv-init): Generate a cluv config for a project.
- [`cluv login`](commands.md#cluv-login): Connect to all the configured clusters.
- [`cluv sync`](commands.md#cluv-sync): Sync the project to every connected cluster, and fetch
  results back.

### Dispatch jobs to different Slurm clusters
- [`cluv submit`](commands.md#cluv-submit)` <cluster> job.sh`: Sync the project and submit a job on
  a cluster.
- `cluv submit first job.sh`: Submit the job on every configured cluster at once, keep whichever
  one starts running first, and cancel the rest.
- [`cluv run`](commands.md#cluv-run)` <cluster> -- ls logs`: Sync the project and run a command in the
  project folder on a cluster.
- [`cluv clean`](commands.md#cluv-clean): Remove results from the clusters once they've been
  deleted locally. See the ["Cleaning runs"](guides/cleaning-runs.md) page.

### Monitor jobs and cluster health
- [`cluv status`](commands.md#cluv-status): Show an overview of all clusters, including GPU
  availability, queue status, and job progress.
- [`cluv sh`](commands.md#cluv-sh)` -- squeue --me`: Run a raw shell command on every connected
  cluster and show the output.

### Launch a Hydra sweep on a remote cluster

```console
python main.py -m launcher=cluv lr=0.01,0.001 seed=1,2,3
```

See the [Hydra launcher](hydra-launcher.md) page for setup and usage.

### How the commands are used together

``` mermaid
    graph LR
        init(<b>cluv init</b> <br> Init project)
        sync(<b>cluv sync</b> <br> Sync project on clusters)
        login(<b>cluv login</b> <br> Connect to clusters)
        submit(<b>cluv submit</b> <br> Submit jobs to clusters)
        status(<b>cluv status</b> <br> See clusters and jobs status)
        disable(<b>cluv disable</b> <br> Disable access to clusters)
        enable(<b>cluv enable</b> <br> Enable access to clusters)
        clean(<b>cluv clean</b> <br> Clean old logs on clusters)
        run(<b>cluv run</b> <br> Run commands on clusters)
        sh(<b>cluv sh</b> <br> Run raw shell commands on clusters)

        init ===> login
        init ==> disable
        init ==> enable

        login ===> sync
        login ===> submit
        login ===> status
        login ===> clean
        login ===> run
        login ===> sh

        click init "commands/#cluv-init"
        click login "commands/#cluv-login"
        click sync "commands/#cluv-sync"
        click submit "commands/#cluv-submit"
        click status "commands/#cluv-status"
        click disable "commands/#cluv-disable"
        click enable "commands/#cluv-enable"
        click clean "commands/#cluv-clean"
        click run "commands/#cluv-run"
        click sh "commands/#cluv-sh"
```
