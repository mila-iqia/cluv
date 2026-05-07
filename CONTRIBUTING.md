# Contributing to cluv

Thank you for your interest in contributing to cluv!

## Issues

Use [GitHub Issues](https://github.com/mila-iqia/cluv/issues) to report bugs or suggest features.

## Development setup

Clone and install project:
```bash
git clone https://github.com/mila-iqia/cluv
cd cluv
uv sync
```

Use your local version as the cluv tool:
```bash
uv tool install --editable <path_to_cluv_repo>
```

### Testing

```bash
uv run pytest
```

Tests marked `integration` require live SSH connections to real clusters. Skip them locally:

```bash
uv run pytest -m "not integration"
```

### Linting

```bash
uv run pre-commit run --all-files
```

