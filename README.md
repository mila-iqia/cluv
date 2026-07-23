# cluv

cluv — sync UV-based Python projects across HPC clusters.

## Status

In early development. Commands are functional, but expect bugs or missing features.

## Requirements

- Python >= 3.13
- [UV](https://docs.astral.sh/uv/)
- SSH access configured for each cluster in `~/.ssh/config`
- A GitHub repository with your project

## Installation

To add `cluv` to your project, use `uv add` or `pip install`:
```bash
uv add cluster-uv
```

To also include the Cluv Hydra launcher:

```bash
uv add cluster-uv[hydra]
```

Install as a command-line tool in an isolated environment:

```bash
uv tool install cluster-uv
```

If you want the bleeding edge version from GitHub, use:

```bash
uv add git+https://github.com/mila-iqia/cluv
```

Then you can run `cluv` directly as a command:

```bash
cluv init
cluv login mila
cluv sync mila
cluv submit mila job.sh
```

## Documentation

* Cluv is documented at https://mila-iqia.github.io/cluv/.
* **Command line help** : Use `cluv --help` or `cluv <command> --help`.
* **Examples** : See the [examples](examples) folder for sample projects using cluv. Each example includes a README with instructions specific to that project.

## Quick Start

1. Initialize your project with:
   ```bash
   cluv init
   ```
2. Establish SSH connections to all configured clusters:
   ```bash
   cluv login
   ```
3. Sync your project to all clusters and run `uv sync` on each:
   ```bash
   cluv sync
   ```

## Configuration

Add a `[tool.cluv]` section to the `pyproject.toml` of your project to manage the behavior of the tool.
The command `cluv init` will add a default config if it doesn't already exists in the `.toml`.

See the config at the project root for an example, or refer to the [docs](https://mila-iqia.github.io/cluv/).
