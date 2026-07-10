# Commands

## cluv

**Usage**
```console
cluv <command> [options]
```

**Commands**

[`cluv init`](#cluv-init) 

Initialize a project.
{: .indent }

[`cluv login`](#cluv-login)

Establish SSH connections to clusters.
{: .indent }

[`cluv sync`](#cluv-sync)

Sync your project on clusters.
{: .indent }

[`cluv submit`](#cluv-submit)

Submit a job to clusters.
{: .indent }

[`cluv status`](#cluv-status)

Show the status of clusters and jobs.
{: .indent }

[`cluv run`](#cluv-run)

Run a command on a specific cluster.
{: .indent }

**Options**

Available for all commands.

`-h`, `--help`

TODO
{: .indent }

`-v`, `--verbose`

TODO
{: .indent }

`-q`, `--quiet`

Disable command output.
{: .indent }

## [`cluv init`](#cluv-init) 

Initialize a cluv project.

If the project already have a `pyproject.toml` file, it will add a `[tool.cluv]` section to the file.

If the project does not have a `pyproject.toml` file, it will create one with a `[tool.cluv]` section.

Default project structure after `cluv init`:
```
my_project/
├── README.md
├── logs -> $SCRATCH/logs/my_project   # symlink to $SCRATCH
├── pyproject.toml        # includes [tool.cluv] config
├── scripts/
│   ├── job.sh            # Slurm job script template
│   └── safe_job.sh       # Slurm job script template (copies .venv and prior results)
└── src/
    └── my_project/
        └── __init__.py
```

**Usage**
```console
cluv init [path]
```

**Arguments**

`path`

The path to use for the project. Defaults to the current working directory.
{: .indent }


## [`cluv login`](#cluv-login)

Create an SSH connection with the clusters. Reuse existing connections when possible.

Run this command before any command that requires a live connection (submit, sync, ...).

!!! tip
    If you don't have a SSH config to connect to the clusters, consider using [milatools](https://github.com/mila-iqia/milatools) to generate your config.

**Usage**
```console
cluv login [clusters]
```

**Arguments**

`clusters`

The clusters to connect to. If not specified, will connect to all clusters in the config. Unreachable clusters will be skipped.
{: .indent }


## [`cluv sync`](#cluv-sync)
TODO

## [`cluv submit`](#cluv-submit)
TODO

## [`cluv status`](#cluv-status)

TODO

**Usage**
```console
cluv status [table]
```

**Arguments**

`table`

Which table to display in the status output. Can be one of `jobs`, `clusters`, or `all`. Defaults to `all`.
{: .indent }

## [`cluv run`](#cluv-run)
TODO
