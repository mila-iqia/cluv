
# Introduction

`cluv` aims to be a simple tool to make it easier to work with `uv` and to dispatch jobs across
multiple Slurm clusters.

## Who is this for?

Cluv is for you if you:

- You use [uv](https://docs.astral.sh/uv/) (or would like to start using uv) to manage your Python projects and dependencies;
- You have access to one or more HPC clusters running Slurm, and;
- You want to easily synchronize and dispatch jobs on all the clusters you have access to.


## Goals

These are the main goals of Cluv:

### Make it simple to synchronize a project across clusters
- `cluv login`: Create a connection to all the cluster I care about (only once).
- `cluv sync`: Sync my project to the clusters where I run jobs, and optionally rsync results back.

### Easily dispatch jobs to different Slurm clusters
- `cluv submit rorqual job.sh`: Synchronize the project and submit a job on the rorqual cluster.
- `cluv submit auto job.sh`: Find the best cluster to run this job. I don't care where it runs.
- `cluv sync`: Fetch the results from the clusters where I ran jobs previously.

### Intuitive monitoring of jobs and cluster health across clusters

- Provide a simple, intuitive interface to monitor job status and cluster health across clusters
  - `cluv status`: Show me an overview of all my clusters, including GPU availability, queue status, and job progress.


## Non-goals

Cluv is not meant to be a _magic_ package that does everything for you.
It is to be understood as a lightweight wrapper around `uv`, with some additional assumptions about
your project, specific to working on Slurm clusters.

!!! warning "Cluv only operates at the project-level!"
    - Cluv does not modify your SSH configuration.
        - If you want to develop locally, use `mila init` from milatools to setup your local SSH
          config.
        - If you use `cluv` in an interactive workflow on the Mila cluster, you will also want to
            configure your SSH config such that you are able to connect to all the DRAC clusters
            directly from the Mila cluster. Cluv doesn't currently do that for you.
    - Cluv does not setup a GitHub repository for you.
        - You have to create a GitHub repository (public or private) for your project.
        - If your repository is private, you have to setup SSH keys and GitHub access for each
          cluster yourself. Cluv doesn't do that for you.


## Core Assumptions

Cluv makes a few very important assumptions about your workflow. These are necessary and won't
change:

1. Your project uses [uv](https://docs.astral.sh/uv/) to manage your Python projects and dependencies.
2. You have SSH access to each cluster, with ControlMaster sessions configured for passwordless login.
    - As a consequence of this, **cluv is not supported on Windows machines**. This is because
      Windows SSH clients don't support the necessary features. Use WSL2 if on Windows.
3. Your project is in a Git repository hosted on GitHub.


## Useful assumptions

These assumptions are useful to limit the initial scope and dev work on Cluv, but could be made more
flexible in the future:

### Project location
- Your project is under `$HOME`, not `$SCRATCH`.
- The project is located at the same relative path from `$HOME` to the project root on all clusters.
