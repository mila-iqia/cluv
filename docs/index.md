# cluv

This is a quick overview. For more information, check out the [introduction](guides/introduction.md).


## Installation

1. (optional) Install UV: https://docs.astral.sh/uv/getting-started/installation/

2. Install this package:

```console
uv tool install git+https://github.com/mila-iqia/cluv
```

3. Navigate to an existing project, and add the cluv config to your `pyproject.toml`:

```console
cluv init
```

## Usage

To view all available commands, use the `--help` flag:
```console
cluv --help
```

Here are some common workflows:

### Setup cluv in an existing project

```console
cd ~/my-project  # Cluv requires your project to be located somewhere under your home directory (`$HOME`).
cluv init
```

### Establish SSH connections to all clusters

```console
cluv login
```

### Sync your project on all clusters
```console
cluv sync
```


### Sync your project on a specific cluster
```console
cluv sync rorqual
```

### Submit a job to a specific cluster
```console
cluv submit rorqual scripts/job.sh --time=00:10:00 -- python main.py
```
